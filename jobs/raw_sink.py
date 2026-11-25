"""raw_sink: Kafka opensky.states.raw -> HDFS raw zone.

Each Kafka message is one whole OpenSky snapshot; it is split into one row
per aircraft with named columns (no other change, no cleaning) and written to
/lake/raw/opensky_states/date=YYYY-MM-DD/ as Parquet, partitioned by the UTC
date of event_time, every 5 minutes, plus the Kafka partition/offset/timestamp
for lineage.
The file sink + HDFS checkpoint give exactly-once output: after a crash the
job restarts from the checkpoint and never writes a batch twice.
"""
import os

from pyspark.sql import functions as F

import common

TRIGGER = os.getenv("RAW_TRIGGER", "5 minutes")
CHECKPOINT = f"{common.CHECKPOINTS}/raw_sink"


def main():
    spark = common.spark_session("raw_sink")
    events = common.kafka_events(spark, max_offsets=500000)
    raw = (events
           .withColumn("date", F.to_date("event_ts"))
           .select(*common.EVENT_COLS, "kafka_partition", "kafka_offset",
                   "kafka_timestamp", "date"))

    q = (raw.writeStream.queryName("raw_sink")
         .format("parquet")
         .option("path", common.RAW_PATH)
         .option("checkpointLocation", CHECKPOINT)
         .partitionBy("date")
         .trigger(processingTime=TRIGGER)
         .outputMode("append")
         .start())
    common.start_health_reporter(spark)
    q.awaitTermination()


if __name__ == "__main__":
    main()
