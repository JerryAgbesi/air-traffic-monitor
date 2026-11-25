"""traffic_agg: Kafka opensky.states.raw -> clean -> 5-minute traffic per region.

Sinks (inside foreachBatch):
  * TimescaleDB traffic_5min, upsert on (window_start, region)
  * Kafka opensky.traffic.5min, key = region (input of the scoring stream)

Event-time handling: 5-minute tumbling windows on the snapshot time, with a
10-minute watermark. Duplicates (same aircraft in the same snapshot, e.g. a
Kafka replay) are removed with dropDuplicatesWithinWatermark.

Output mode is "update": an open window is rewritten every trigger, so the
dashboard is live, and the upsert keeps one row per window and region.
Kafka receives every update too; the scoring job keeps the latest value per
(window_start, region).
"""
import os

from pyspark.sql import functions as F

import common

TRIGGER = os.getenv("AGG_TRIGGER", "1 minute")
CHECKPOINT = f"{common.CHECKPOINTS}/traffic_agg"


def build(events):
    ev = (common.clean(events)
          .withWatermark("snapshot_ts", "10 minutes")
          .dropDuplicatesWithinWatermark(["icao24", "snapshot_time"]))
    # Every event is copied to all six series so that each series sees every
    # snapshot (a region with no aircraft in one snapshot still counts that
    # snapshot as 0); it only adds to the counters of "France" and of its own
    # region.
    ev = (ev.withColumnRenamed("region", "aircraft_region")
            .withColumn("region", F.explode(F.array(
                *[F.lit(r) for r in ["France"] + common.REGIONS]))))
    mine = (F.col("region") == "France") | (F.col("region") == F.col("aircraft_region"))
    air = mine & F.col("airborne")
    agg = (ev.groupBy(F.window("snapshot_ts", "5 minutes"), "region")
             .agg(F.approx_count_distinct("snapshot_time").alias("snapshots"),
                  F.sum(air.cast("int")).alias("airborne_events"),
                  F.sum((air & (F.col("vertical_rate_ms") > 1.0)).cast("int")).alias("climb_events"),
                  F.sum((air & (F.col("vertical_rate_ms") < -1.0)).cast("int")).alias("desc_events"),
                  F.approx_count_distinct(F.when(air, F.col("icao24"))).alias("aircraft_distinct"),
                  F.sum(mine.cast("int")).alias("events")))
    n = F.greatest(F.col("snapshots"), F.lit(1))
    return agg.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "region",
        # average number airborne per snapshot (4 snapshots per window at 75 s)
        F.round(F.col("airborne_events") / n, 2).alias("airborne_avg"),
        F.round(F.col("climb_events") / n, 2).alias("climbing"),
        F.round(F.col("desc_events") / n, 2).alias("descending"),
        "aircraft_distinct", "snapshots", "events",
        F.current_timestamp().alias("updated_at"))


def write_batch(batch_df, batch_id):
    spark = batch_df.sparkSession
    batch_df = batch_df.persist()
    if batch_df.isEmpty():
        batch_df.unpersist()
        return
    common.upsert(spark, batch_df, "traffic_5min",
                  key_cols=["window_start", "region"],
                  update_cols=["window_end", "airborne_avg", "climbing",
                               "descending", "aircraft_distinct", "snapshots",
                               "events", "updated_at"])
    (batch_df.select(
        F.col("region").alias("key"),
        F.to_json(F.struct(
            F.date_format("window_start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
            F.date_format("window_end", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
            "region", "airborne_avg", "climbing", "descending",
            "aircraft_distinct", "snapshots", "events")).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", common.KAFKA_BOOTSTRAP)
     .option("topic", common.AGG_TOPIC)
     .save())
    batch_df.unpersist()


def main():
    spark = common.spark_session("traffic_agg")
    q = (build(common.kafka_events(spark))
         .writeStream.queryName("traffic_agg")
         .outputMode("update")
         .foreachBatch(write_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime=TRIGGER)
         .start())
    common.start_health_reporter(spark)
    q.awaitTermination()


if __name__ == "__main__":
    main()
