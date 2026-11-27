"""
Raw zone -> curated zone (HDFS) + InfluxDB (Spark batch job on YARN).

Cleaning (raw stays untouched, so these rules can change and be re-run):
  - drop rows without icao24, position or position time
  - drop stale positions (older than STALE_SECONDS at snapshot time)
  - drop physically impossible speeds/altitudes
  - normalise icao24/callsign, altitude = barometric, else geometric
  - one row per (icao24, time_position): OpenSky repeats the last known
    position in every snapshot until a new one arrives
  - flight_phase from on_ground and vertical rate

Outputs, both idempotent:
  - /data/curated/opensky_states/date=YYYY-MM-DD/ : the processed days are
    overwritten as a whole (dynamic partition overwrite)
  - InfluxDB: a point is identified by measurement + tags + timestamp, so
    writing the same point again overwrites it instead of adding a duplicate
      aircraft  tags icao24, time = time_position, one point per position
      traffic   no tags,     time = snapshot_time, KPIs per snapshot

Usage:
  curate.py                     today (and yesterday just after midnight);
                                InfluxDB gets the last --lookback minutes
  curate.py --date 2026-11-25   backfill one whole day, curated + InfluxDB
"""
import argparse
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW = os.getenv("RAW_PATH", "/data/raw/opensky_states")
CURATED = os.getenv("CURATED_PATH", "/data/curated/opensky_states")
STALE_SECONDS = 60
CLIMB_MS = 2.5       # ~500 ft/min
BATCH_LINES = 5000


def read_env_file(name):
    # shipped with spark-submit --files, so the token is not on the command line
    env = {}
    with open(name) as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                env[k] = v
    return env


def clean(raw):
    phase = (F.when(F.col("on_ground"), "ground")
             .when(F.col("vertical_rate") > CLIMB_MS, "climb")
             .when(F.col("vertical_rate") < -CLIMB_MS, "descent")
             .otherwise("level"))
    return (raw
        .filter(F.col("icao24").isNotNull()
                & F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
                & F.col("time_position").isNotNull())
        .filter(F.col("snapshot_time") - F.col("time_position") <= STALE_SECONDS)
        .withColumn("altitude_m", F.coalesce("baro_altitude", "geo_altitude"))
        .filter(F.col("velocity").isNull() | F.col("velocity").between(0, 350))
        .filter(F.col("altitude_m").isNull() | F.col("altitude_m").between(-500, 20000))
        .select(
            F.lower(F.trim("icao24")).alias("icao24"),
            F.expr("nullif(trim(callsign), '')").alias("callsign"),
            "origin_country",
            "snapshot_time",
            "time_position",
            F.col("time_position").cast("timestamp").alias("ts"),
            F.col("latitude").alias("lat"),
            F.col("longitude").alias("lon"),
            "altitude_m",
            "geo_altitude",
            F.col("velocity").alias("velocity_ms"),
            F.col("true_track").alias("heading_deg"),
            F.col("vertical_rate").alias("vertical_rate_ms"),
            "on_ground",
            "squawk",
            phase.alias("flight_phase"),
            "date"))


def dedupe(df):
    first = Window.partitionBy("icao24", "time_position").orderBy("snapshot_time")
    return (df.withColumn("_n", F.row_number().over(first))
              .filter("_n = 1").drop("_n"))


def traffic_kpis(cleaned):
    # per snapshot, before dedupe: an aircraft whose position did not change
    # between two snapshots is still in the air
    return (cleaned.groupBy("snapshot_time").agg(
        F.count("*").alias("aircraft"),
        F.sum(F.when(~F.col("on_ground"), 1).otherwise(0)).alias("airborne"),
        F.sum(F.when(F.col("flight_phase") == "climb", 1).otherwise(0)).alias("climbing"),
        F.sum(F.when(F.col("flight_phase") == "descent", 1).otherwise(0)).alias("descending"),
        F.avg(F.when(~F.col("on_ground"), F.col("altitude_m"))).alias("mean_altitude_m")))


# ---- InfluxDB line protocol -------------------------------------------------

def _field(key, value):
    if value is None:
        return None
    if isinstance(value, bool):
        return f"{key}={'true' if value else 'false'}"
    if isinstance(value, int):
        return f"{key}={value}i"
    if isinstance(value, float):
        return f"{key}={value!r}"
    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'{key}="{s}"'


def aircraft_line(r):
    fields = [_field(k, r[k]) for k in (
        "lat", "lon", "altitude_m", "velocity_ms", "heading_deg",
        "vertical_rate_ms", "on_ground", "flight_phase", "callsign",
        "origin_country", "squawk")]
    return f"aircraft,icao24={r['icao24']} {','.join(f for f in fields if f)} {r['time_position']}"


def traffic_line(r):
    fields = [_field(k, r[k]) for k in (
        "aircraft", "airborne", "climbing", "descending", "mean_altitude_m")]
    return f"traffic {','.join(f for f in fields if f)} {r['snapshot_time']}"


def make_writer(url, org, bucket, token, to_line):
    endpoint = f"{url}/api/v2/write?org={org}&bucket={bucket}&precision=s"

    def post(lines):
        body = "\n".join(lines).encode()
        for attempt in range(3):
            try:
                req = urllib.request.Request(endpoint, data=body, method="POST", headers={
                    "Authorization": f"Token {token}",
                    "Content-Type": "text/plain; charset=utf-8"})
                urllib.request.urlopen(req, timeout=30).read()
                return
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

    def write_partition(rows):
        lines = []
        for r in rows:
            lines.append(to_line(r.asDict()))
            if len(lines) >= BATCH_LINES:
                post(lines)
                lines = []
        if lines:
            post(lines)

    return write_partition


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="backfill one UTC day (YYYY-MM-DD)")
    ap.add_argument("--lookback", type=int, default=60,
                    help="minutes of data sent to InfluxDB in incremental mode")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    if args.date:
        dates = [args.date]
        since = 0
    else:
        start = now - timedelta(minutes=args.lookback)
        dates = sorted({start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")})
        since = int(start.timestamp())

    spark = (SparkSession.builder
             .appName("opensky-curate")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())

    raw = spark.read.parquet(RAW).filter(F.col("date").isin(dates))
    cleaned = clean(raw).cache()
    curated = dedupe(cleaned)

    # rewrite the processed days: running twice gives the same result
    (curated.repartition(1, "date").sortWithinPartitions("icao24", "time_position")
     .write.mode("overwrite").partitionBy("date").parquet(CURATED))

    env = read_env_file("influx.env")
    target = (env["INFLUX_URL"], env["INFLUX_ORG"], env["INFLUX_BUCKET"], env["INFLUX_TOKEN"])

    out = spark.read.parquet(CURATED).filter(F.col("date").isin(dates))
    points = out.filter(F.col("snapshot_time") >= since).coalesce(2)
    points.foreachPartition(make_writer(*target, aircraft_line))

    kpis = traffic_kpis(cleaned.filter(F.col("snapshot_time") >= since)).coalesce(1)
    kpis.foreachPartition(make_writer(*target, traffic_line))

    print(f"curate: dates={dates} raw={raw.count()} cleaned={cleaned.count()} "
          f"curated={out.count()} influx_points={points.count()} snapshots={kpis.count()}")
    spark.stop()


if __name__ == "__main__":
    main()
