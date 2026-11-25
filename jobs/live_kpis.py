"""live_kpis: Kafka opensky.states.raw -> clean -> per-snapshot KPIs.

Two streaming queries in one Spark application:

1. live_kpis -> TimescaleDB snapshot_kpis, upsert on (snapshot_time, kpi, label)
   kpi             label                                 value
   airborne        France / Paris / North-East / ...     aircraft in the air
   on_ground       France                                aircraft on the ground
   flight_level    below FL100 / FL100-FL300 / above FL300
   paris_airports  CDG climbing / CDG descending / CDG level / ORY ...
                   (airborne, < 30 km from the airport, below 3,000 m)
   origin_country  France / Germany / ...                (Grafana takes top 10)

2. live_positions -> TimescaleDB live_positions (one row per aircraft and
   snapshot) for the Grafana live map. Kept 2 days by a retention policy.
"""
import os

from pyspark.sql import functions as F

import common

TRIGGER = os.getenv("KPI_TRIGGER", "30 seconds")
CHECKPOINT = f"{common.CHECKPOINTS}/live_kpis"
CHECKPOINT_POS = f"{common.CHECKPOINTS}/live_positions"

AIRPORTS = {"CDG": (49.0097, 2.5479), "ORY": (48.7262, 2.3652)}


def km_to(lat0, lon0):
    """Haversine distance in km from (lat0, lon0)."""
    dlat = F.radians(F.col("lat") - lat0)
    dlon = F.radians(F.col("lon") - lon0)
    a = (F.sin(dlat / 2) ** 2 + F.cos(F.radians(F.lit(lat0)))
         * F.cos(F.radians("lat")) * F.sin(dlon / 2) ** 2)
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def kpi_rows(ev):
    """Each aircraft contributes 1 to several (kpi, label) counters."""
    air = F.col("airborne")

    def item(cond, kpi, label):
        return F.when(cond, F.struct(F.lit(kpi).alias("kpi"), label.alias("label")))

    vr = F.col("vertical_rate_ms")
    phase = (F.when(vr > 1.0, "climbing").when(vr < -1.0, "descending")
              .otherwise("level"))
    items = [
        item(air, "airborne", F.lit("France")),
        item(air & F.col("region").isNotNull(), "airborne", F.col("region")),
        item(~air, "on_ground", F.lit("France")),
        item(air & F.col("fl_band").isNotNull(), "flight_level", F.col("fl_band")),
        item(air & F.col("origin_country").isNotNull(), "origin_country",
             F.col("origin_country")),
    ]
    for name, (la, lo) in AIRPORTS.items():
        near = air & (km_to(la, lo) < 30) & (F.col("baro_altitude_m") < 3000)
        items.append(item(near, "paris_airports",
                          F.concat(F.lit(name + " "), phase)))
    arr = F.filter(F.array(*items), lambda x: x.isNotNull())
    return ev.select("snapshot_ts", F.explode(arr).alias("k")).select(
        "snapshot_ts", "k.kpi", "k.label")


def build_kpis(events):
    ev = (common.clean(events)
          .withWatermark("snapshot_ts", "3 minutes")
          .dropDuplicatesWithinWatermark(["icao24", "snapshot_time"]))
    return (kpi_rows(ev)
            .groupBy("snapshot_ts", "kpi", "label")
            .agg(F.count(F.lit(1)).cast("double").alias("value"))
            .select(F.col("snapshot_ts").alias("snapshot_time"), "kpi",
                    "label", "value"))


def build_positions(events):
    return (common.clean(events)
            .where(F.col("airborne"))
            .select(F.col("snapshot_ts").alias("snapshot_time"), "icao24",
                    "callsign", "origin_country", "lat", "lon",
                    "baro_altitude_m", "velocity_ms", "true_track_deg",
                    "vertical_rate_ms", "region"))
    # duplicates (Kafka replays) are skipped by ON CONFLICT DO NOTHING


def write_kpis(df, batch_id):
    if not df.isEmpty():
        common.upsert(df.sparkSession, df, "snapshot_kpis",
                      key_cols=["snapshot_time", "kpi", "label"],
                      update_cols=["value"])


def write_positions(df, batch_id):
    if not df.isEmpty():
        common.upsert(df.sparkSession, df, "live_positions",
                      key_cols=["snapshot_time", "icao24"], update_cols=[])


def main():
    spark = common.spark_session("live_kpis")
    events = common.kafka_events(spark)
    q1 = (build_kpis(events).writeStream.queryName("live_kpis")
          .outputMode("update").foreachBatch(write_kpis)
          .option("checkpointLocation", CHECKPOINT)
          .trigger(processingTime=TRIGGER).start())
    q2 = (build_positions(events).writeStream.queryName("live_positions")
          .outputMode("append").foreachBatch(write_positions)
          .option("checkpointLocation", CHECKPOINT_POS)
          .trigger(processingTime=TRIGGER).start())
    common.start_health_reporter(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
