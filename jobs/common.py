"""Shared code for the Spark jobs: schema, Kafka parsing, cleaning rules,
regions, TimescaleDB upserts and the pipeline_health reporter.

Shipped to YARN with  --py-files jobs/common.py  (see deploy/submit.sh).
"""
import json
import logging
import os
import threading
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger("opensky")

# ---------------------------------------------------------------- settings
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "node2:9092,node3:9092,node4:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "opensky.states.raw")
AGG_TOPIC = os.getenv("AGG_TOPIC", "opensky.traffic.5min")

LAKE = os.getenv("LAKE_ROOT", "hdfs:///lake")
CHECKPOINTS = os.getenv("CHECKPOINT_ROOT", "hdfs:///checkpoints")
RAW_PATH = f"{LAKE}/raw/opensky_states"
CLEAN_PATH = f"{LAKE}/cleaned/states"

TSDB_URL = os.getenv("TSDB_URL", "jdbc:postgresql://node6:5432/opensky")
TSDB_USER = os.getenv("TSDB_USER", "spark_writer")
TSDB_PASSWORD = os.getenv("TSDB_PASSWORD", "")

# Metropolitan France bounding box (same as the producer)
LAMIN, LOMIN, LAMAX, LOMAX = 41.3, -5.2, 51.1, 9.6

# ------------------------------------------------------------------ schema
# Kafka value = one whole OpenSky API response, as received:
#   {"time": <snapshot unix s>, "states": [[17 positional fields], ...]}
# parse_values() turns it into one row per aircraft with named columns
# (schema_version 0). Per-aircraft JSON events (schema_version 1, the
# Checkpoint 1 draft) are also accepted, so old data in either format parses.
STATE_ARRAY = T.ArrayType(T.ArrayType(T.StringType()))
VALUE_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("icao24", T.StringType()),
    T.StructField("callsign", T.StringType()),
    T.StructField("origin_country", T.StringType()),
    T.StructField("event_time", T.LongType()),
    T.StructField("snapshot_time", T.LongType()),
    T.StructField("ingest_time", T.StringType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("baro_altitude_m", T.DoubleType()),
    T.StructField("geo_altitude_m", T.DoubleType()),
    T.StructField("on_ground", T.BooleanType()),
    T.StructField("velocity_ms", T.DoubleType()),
    T.StructField("true_track_deg", T.DoubleType()),
    T.StructField("vertical_rate_ms", T.DoubleType()),
    T.StructField("squawk", T.StringType()),
    T.StructField("position_source", T.IntegerType()),
    # legacy snapshot format
    T.StructField("time", T.LongType()),
    T.StructField("states", STATE_ARRAY),
])

EVENT_COLS = ["schema_version", "icao24", "callsign", "origin_country",
              "event_time", "snapshot_time", "ingest_time", "lon", "lat",
              "baro_altitude_m", "geo_altitude_m", "on_ground", "velocity_ms",
              "true_track_deg", "vertical_rate_ms", "squawk", "position_source"]


def _state_to_event(s, snap):
    """Positional OpenSky state array (parsed as strings) -> named struct.
    0 icao24, 1 callsign, 2 origin_country, 3 time_position, 4 last_contact,
    5 lon, 6 lat, 7 baro_altitude, 8 on_ground, 9 velocity, 10 true_track,
    11 vertical_rate, 12 sensors, 13 geo_altitude, 14 squawk, 15 spi,
    16 position_source. event_time = time_position, else last_contact."""
    def d(i):
        return s[i].cast("double")
    return F.struct(
        F.lit(0).alias("schema_version"),
        s[0].alias("icao24"),
        F.trim(s[1]).alias("callsign"),
        s[2].alias("origin_country"),
        F.coalesce(s[3], s[4]).cast("long").alias("event_time"),
        snap.alias("snapshot_time"),
        F.lit(None).cast("string").alias("ingest_time"),
        d(5).alias("lon"), d(6).alias("lat"),
        d(7).alias("baro_altitude_m"), d(13).alias("geo_altitude_m"),
        s[8].cast("boolean").alias("on_ground"),
        d(9).alias("velocity_ms"), d(10).alias("true_track_deg"),
        d(11).alias("vertical_rate_ms"),
        s[14].alias("squawk"), s[16].cast("int").alias("position_source"))


def parse_values(df: DataFrame, value_col="value", keep=()) -> DataFrame:
    """JSON string column -> one row per aircraft with named columns.

    A snapshot message is exploded into its aircraft; a per-aircraft event
    gives one row. Works in batch and streaming (single explode, no union).
    Unparseable messages and aircraft without icao24 or any time are dropped.
    """
    v = F.from_json(F.col(value_col).cast("string"), VALUE_SCHEMA)
    df = df.withColumn("_v", v)
    single = F.struct(*[F.col("_v." + c).alias(c) for c in EVENT_COLS])
    snapshot = F.transform(F.col("_v.states"),
                           lambda s: _state_to_event(s, F.col("_v.time")))
    events = (F.when(F.col("_v.states").isNotNull(), snapshot)
               .when(F.col("_v.icao24").isNotNull(), F.array(single)))
    out = df.select(*keep, F.explode(events).alias("e"))
    return out.select(*keep, "e.*").where(F.col("icao24").isNotNull()
                                           & F.col("event_time").isNotNull())


def read_kafka(spark: SparkSession, topic=RAW_TOPIC, max_offsets=200000):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")   # only used on first start
            .option("failOnDataLoss", "false")        # 14-day retention
            .option("maxOffsetsPerTrigger", max_offsets)
            .load())


def kafka_events(spark, max_offsets=200000) -> DataFrame:
    """Kafka raw topic -> parsed events with Kafka metadata + timestamps."""
    raw = read_kafka(spark, RAW_TOPIC, max_offsets).select(
        "value",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"))
    return add_times(parse_values(
        raw, keep=["kafka_partition", "kafka_offset", "kafka_timestamp"]))


def add_times(df):
    return (df.withColumn("event_ts", F.timestamp_seconds("event_time"))
              .withColumn("snapshot_ts", F.timestamp_seconds(
                  F.coalesce("snapshot_time", "event_time"))))


# ---------------------------------------------------------------- cleaning
REGIONS = ["Paris", "North-East", "West", "South-West", "South-East"]


def region_col(lat="lat", lon="lon"):
    """Five zones of metropolitan France (Paris = Ile-de-France box)."""
    lat, lon = F.col(lat), F.col(lon)
    return (F.when(lat.isNull() | lon.isNull(), None)
             .when(lat.between(48.1, 49.25) & lon.between(1.4, 3.6), "Paris")
             .when((lon >= 2.5) & (lat >= 46.0), "North-East")
             .when((lon < 2.5) & (lat >= 46.0), "West")
             .when(lon < 2.5, "South-West")
             .otherwise("South-East"))


def flight_level_band(alt="baro_altitude_m"):
    a = F.col(alt)
    return (F.when(a.isNull(), None)
             .when(a < 3048, "below FL100")        # 10,000 ft
             .when(a < 9144, "FL100-FL300")        # 30,000 ft
             .otherwise("above FL300"))


def clean(df: DataFrame) -> DataFrame:
    """Cleaning rules, used by the streams and by clean_daily.

    * drop rows without a valid 6-hex icao24, an event time or a position
    * keep only positions inside the France box
    * impossible values become NULL (altitude < -500 m or > 20 km,
      speed > 400 m/s, |vertical rate| > 100 m/s, track outside 0-360)
    * normalise text: lower-case icao24, trimmed upper-case callsign
    * derived columns: airborne, region, fl_band
    """
    def keep_if(c, cond):
        return F.when(cond, F.col(c))

    out = (df
           .withColumn("icao24", F.lower(F.trim("icao24")))
           .where(F.col("icao24").rlike("^[0-9a-f]{6}$"))
           .where(F.col("event_time").isNotNull())
           .where(F.col("lat").between(LAMIN, LAMAX)
                  & F.col("lon").between(LOMIN, LOMAX))
           .withColumn("callsign", F.nullif(F.upper(F.trim("callsign")), F.lit("")))
           .withColumn("baro_altitude_m", keep_if("baro_altitude_m",
                       F.col("baro_altitude_m").between(-500, 20000)))
           .withColumn("geo_altitude_m", keep_if("geo_altitude_m",
                       F.col("geo_altitude_m").between(-500, 20000)))
           .withColumn("velocity_ms", keep_if("velocity_ms",
                       F.col("velocity_ms").between(0, 400)))
           .withColumn("vertical_rate_ms", keep_if("vertical_rate_ms",
                       F.abs("vertical_rate_ms") <= 100))
           .withColumn("true_track_deg", keep_if("true_track_deg",
                       F.col("true_track_deg").between(0, 360)))
           .withColumn("on_ground", F.coalesce("on_ground", F.lit(False)))
           .withColumn("airborne", ~F.col("on_ground"))
           .withColumn("region", region_col())
           .withColumn("fl_band", flight_level_band()))
    return out


# ------------------------------------------------------------ spark setup
def spark_session(app_name):
    spark = (SparkSession.builder.appName(app_name)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions",
                     os.getenv("SHUFFLE_PARTITIONS", "12"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return spark


# ------------------------------------------------------------ TimescaleDB
_jdbc_lock = threading.Lock()


def jdbc_execute(spark, *sqls):
    """Run SQL statements on TimescaleDB through the driver JVM (uses the
    PostgreSQL JDBC driver from --packages, so no extra Python library)."""
    jvm = spark.sparkContext._jvm
    with _jdbc_lock:
        jvm.org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry \
            .register("org.postgresql.Driver")
        props = jvm.java.util.Properties()
        props.setProperty("user", TSDB_USER)
        props.setProperty("password", TSDB_PASSWORD)
        conn = jvm.java.sql.DriverManager.getConnection(TSDB_URL, props)
        try:
            conn.setAutoCommit(False)
            st = conn.createStatement()
            for sql in sqls:
                st.execute(sql)
            conn.commit()
            st.close()
        finally:
            conn.close()


def upsert(spark, df: DataFrame, table: str, key_cols, update_cols):
    """Idempotent write: rows go to the UNLOGGED staging table stg_<table>,
    then INSERT ... ON CONFLICT DO UPDATE moves them into <table>.
    A replayed micro-batch overwrites rows instead of duplicating them.
    Each streaming query owns its own staging table."""
    stg = f"stg_{table}"
    cols = df.columns
    jdbc_execute(spark, f"TRUNCATE {stg}")
    (df.write.format("jdbc")
       .option("url", TSDB_URL).option("dbtable", stg)
       .option("user", TSDB_USER).option("password", TSDB_PASSWORD)
       .option("driver", "org.postgresql.Driver")
       .option("batchsize", 5000)
       .mode("append").save())
    collist = ", ".join(cols)
    if update_cols:
        action = "DO UPDATE SET " + ", ".join(
            f"{c} = EXCLUDED.{c}" for c in update_cols)
    else:
        action = "DO NOTHING"
    jdbc_execute(spark,
                 f"INSERT INTO {table} ({collist}) SELECT {collist} FROM {stg} "
                 f"ON CONFLICT ({', '.join(key_cols)}) {action}",
                 f"TRUNCATE {stg}")


# --------------------------------------------------------- pipeline health
def _lag(progress):
    lag = 0
    for src in progress.get("sources", []):
        try:
            end = json.loads(src["endOffset"]) if isinstance(src["endOffset"], str) else src["endOffset"]
            latest = src.get("latestOffset")
            latest = json.loads(latest) if isinstance(latest, str) else latest
            for topic, parts in (latest or {}).items():
                for p, off in parts.items():
                    lag += max(0, off - end.get(topic, {}).get(p, off))
        except Exception:
            return None
    return lag


def start_health_reporter(spark, interval=30):
    """Every `interval` s, write each active query's last progress to
    pipeline_health (input rows/s, processed rows/s, batch ms, Kafka lag)."""
    def loop():
        while True:
            time.sleep(interval)
            rows = []
            for q in spark.streams.active:
                p = q.lastProgress
                if not p:
                    continue
                metrics = {
                    "input_rows_per_s": p.get("inputRowsPerSecond"),
                    "processed_rows_per_s": p.get("processedRowsPerSecond"),
                    "batch_duration_ms": p.get("durationMs", {}).get("triggerExecution"),
                    "kafka_lag": _lag(p),
                }
                for m, v in metrics.items():
                    if v is not None and v == v:  # skip None/NaN
                        rows.append(f"(now(), '{q.name}', '{m}', {float(v)})")
            if rows:
                try:
                    jdbc_execute(spark,
                                 "INSERT INTO pipeline_health(time, source, metric, value) VALUES "
                                 + ", ".join(rows)
                                 + " ON CONFLICT (time, source, metric) DO UPDATE SET value = EXCLUDED.value")
                except Exception as e:
                    log.warning("pipeline_health write failed: %s", e)
    threading.Thread(target=loop, daemon=True, name="health").start()
