"""backfill_jsonl: one-off import of the first collector's JSON-lines archive
(data/raw/date=YYYY-MM-DD/states.jsonl) into the HDFS raw zone.

Accepts both line formats written by the old archive consumer: a whole API
response {"time":..., "states":[[...], ...]} or a per-aircraft v1 event.
Rows get schema_version 0 when they came from a whole snapshot and NULL
Kafka columns. Output goes to the same /lake/raw/opensky_states/date=... folders
that raw_sink writes, as separate files, so clean_daily picks them up.

Run it ONCE (or delete the backfill files first): re-running appends again.
clean_daily's dedup on (icao24, event_time) would still remove the copies.

Usage:  backfill_jsonl.py hdfs:///staging/old_archive
        (the folder that contains the date=... sub-folders)
"""
import sys

from pyspark.sql import functions as F
from pyspark.sql import types as T

import common


def main():
    src = sys.argv[1]
    spark = common.spark_session("backfill_jsonl")
    lines = spark.read.text(f"{src}/date=*/*.jsonl")
    events = common.parse_values(lines)
    out = (events
           .withColumn("kafka_partition", F.lit(None).cast(T.IntegerType()))
           .withColumn("kafka_offset", F.lit(None).cast(T.LongType()))
           .withColumn("kafka_timestamp", F.lit(None).cast(T.TimestampType()))
           .withColumn("date", F.to_date(F.timestamp_seconds("event_time")))
           .select(*common.EVENT_COLS, "kafka_partition", "kafka_offset",
                   "kafka_timestamp", "date"))
    out.cache()
    print(f"[backfill] {out.count()} aircraft rows")
    out.groupBy("date").count().orderBy("date").show(100)
    (out.repartition("date").write.mode("append").partitionBy("date")
        .parquet(common.RAW_PATH))
    spark.stop()


if __name__ == "__main__":
    main()
