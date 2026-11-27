"""
Kafka -> HDFS raw zone (Spark Structured Streaming on YARN).

Reads full OpenSky snapshots from opensky.states.raw, explodes them into one
row per aircraft and appends Parquet files partitioned by UTC day:
    /data/raw/opensky_states/date=YYYY-MM-DD/   (on HDFS)
No cleaning here: every OpenSky field is kept, only typed. Kafka partition and
offset are kept too, so any row can be traced back to its message.

The checkpoint in HDFS stores the Kafka offsets, so a restart resumes where it
stopped and the file sink writes each batch exactly once.

Run: spark/submit_ingest_raw.sh
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "worker2:9092,worker3:9092,worker4:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "opensky.states.raw")
# Plain paths resolve against fs.defaultFS (HDFS); the streaming checkpoint
# rejects "hdfs:///..." URIs without a host.
OUTPUT = os.getenv("RAW_PATH", "/data/raw/opensky_states")
CHECKPOINT = os.getenv("RAW_CHECKPOINT", "/checkpoints/opensky/ingest_raw")
TRIGGER = os.getenv("RAW_TRIGGER", "5 minutes")

# OpenSky state vectors are arrays of mixed types; read every field as text
# and cast by position below.
SNAPSHOT = T.StructType([
    T.StructField("time", T.LongType()),
    T.StructField("states", T.ArrayType(T.ArrayType(T.StringType()))),
])

# (index in the state vector, column name, type)
FIELDS = [
    (0, "icao24", "string"),
    (1, "callsign", "string"),
    (2, "origin_country", "string"),
    (3, "time_position", "long"),
    (4, "last_contact", "long"),
    (5, "longitude", "double"),
    (6, "latitude", "double"),
    (7, "baro_altitude", "double"),
    (8, "on_ground", "boolean"),
    (9, "velocity", "double"),
    (10, "true_track", "double"),
    (11, "vertical_rate", "double"),
    (13, "geo_altitude", "double"),
    (14, "squawk", "string"),
    (15, "spi", "boolean"),
    (16, "position_source", "int"),
]


def parse(raw):
    snapshots = raw.select(
        F.from_json(F.col("value").cast("string"), SNAPSHOT).alias("s"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    states = snapshots.select(
        F.col("s.time").alias("snapshot_time"),
        F.explode("s.states").alias("st"),
        "kafka_partition", "kafka_offset",
    )
    return states.select(
        "snapshot_time",
        *[F.col("st")[i].cast(t).alias(name) for i, name, t in FIELDS],
        "kafka_partition", "kafka_offset",
        F.to_date(F.from_unixtime("snapshot_time")).alias("date"),
    )


def main():
    spark = (SparkSession.builder
             .appName("opensky-ingest-raw")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())

    # One file per batch instead of one per Kafka partition (~300 files/day
    # instead of ~1700); a batch is only a few MB.
    (parse(raw).coalesce(1).writeStream
     .format("parquet")
     .option("path", OUTPUT)
     .option("checkpointLocation", CHECKPOINT)
     .partitionBy("date")
     .trigger(processingTime=TRIGGER)
     .start()
     .awaitTermination())


if __name__ == "__main__":
    main()
