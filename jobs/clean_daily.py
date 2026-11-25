"""clean_daily: raw zone -> cleaned zone, one UTC day at a time (nightly batch).

  in : /lake/raw/opensky_states/date=D/
  out: /lake/cleaned/states/date=D/   (Parquet, overwritten -> safe to re-run)

Steps: cleaning rules from common.clean (valid icao24, position inside the
France box, impossible values set to NULL, normalised text), then
deduplication on (icao24, event_time) keeping the earliest Kafka copy, then
derived columns (event_ts, region, fl_band, airborne) and compaction to a few
large files sorted by aircraft and time.

Usage:  clean_daily.py [YYYY-MM-DD]        (default: yesterday, UTC)
        clean_daily.py 2026-11-20 2026-11-24   (inclusive range)
"""
import sys
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

import common

OUT_FILES = 4


def days(argv):
    if len(argv) == 0:
        return [(datetime.now(timezone.utc).date() - timedelta(days=1))]
    start = date.fromisoformat(argv[0])
    end = date.fromisoformat(argv[1]) if len(argv) > 1 else start
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def clean_day(spark, d):
    src = f"{common.RAW_PATH}/date={d.isoformat()}"
    try:
        raw = spark.read.parquet(src)
    except Exception as e:
        print(f"[clean_daily] {d}: no raw data ({e.__class__.__name__}), skipped")
        return
    n_raw = raw.count()
    cleaned = common.clean(common.add_times(raw))
    first = Window.partitionBy("icao24", "event_time").orderBy(
        F.col("kafka_timestamp").asc_nulls_last(), F.col("kafka_offset").asc_nulls_last())
    dedup = (cleaned.withColumn("_rn", F.row_number().over(first))
             .where("_rn = 1").drop("_rn"))
    out = (dedup.select(
              "icao24", "callsign", "origin_country", "event_time", "event_ts",
              "snapshot_time", "snapshot_ts", "lat", "lon", "baro_altitude_m",
              "geo_altitude_m", "on_ground", "airborne", "velocity_ms",
              "true_track_deg", "vertical_rate_ms", "squawk", "position_source",
              "region", "fl_band")
           .withColumn("date", F.lit(d.isoformat()).cast("date"))
           .repartition(OUT_FILES)
           .sortWithinPartitions("icao24", "event_time"))
    (out.write.mode("overwrite").partitionBy("date").parquet(common.CLEAN_PATH))
    n_out = spark.read.parquet(f"{common.CLEAN_PATH}/date={d.isoformat()}").count()
    print(f"[clean_daily] {d}: {n_raw} raw rows -> {n_out} clean rows "
          f"({n_raw - n_out} dropped or duplicates)")


def main():
    spark = common.spark_session("clean_daily")
    # overwrite only the partitions we write, never the whole cleaned zone
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    for d in days(sys.argv[1:]):
        clean_day(spark, d)
    spark.stop()


if __name__ == "__main__":
    main()
