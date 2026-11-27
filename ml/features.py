"""
Feature engineering for air traffic forecasting.

Creates 5-minute window features from curated aircraft positions:
- Target: airborne count at t+30min (6 windows ahead)
- Features: airborne count at t, t-15min, t-30min, t-60min, hour, weekday
"""
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# Configuration
CURATED_PATH = os.getenv("CURATED_PATH", "/data/curated/opensky_states")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/airborne_forecaster")
FEATURE_WINDOW_MINUTES = 5
FORECAST_HORIZON_WINDOWS = 6  # 30 minutes = 6 * 5-min windows


def get_spark(app_name="feature_engineering"):
    """Create Spark session with UTC timezone."""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def load_curated_data(spark, start_date=None, end_date=None):
    """
    Load curated data from HDFS.

    Args:
        spark: SparkSession
        start_date: str in YYYY-MM-DD format (inclusive)
        end_date: str in YYYY-MM-DD format (inclusive)

    Returns:
        DataFrame with curated aircraft positions
    """
    df = spark.read.parquet(CURATED_PATH)

    if start_date:
        df = df.filter(F.col("date") >= start_date)
    if end_date:
        df = df.filter(F.col("date") <= end_date)

    return df


def compute_5min_windows(df):
    """
    Aggregate curated positions into 5-minute windows.

    Each window contains:
    - window_start, window_end (UTC)
    - airborne_count: distinct aircraft with airborne=true in that window
    - snapshot_count: number of snapshots in the window
    - hour (0-23 Paris time), weekday (0=Mon...6=Sun)
    """
    # Convert snapshot_time to timestamp and assign to 5-min windows
    windowed = (df
        .withColumn("snapshot_ts", F.col("snapshot_time").cast("timestamp"))
        .withColumn("window_start", F.window("snapshot_ts", "5 minutes").getField("start"))
        .withColumn("window_end", F.window("snapshot_ts", "5 minutes").getField("end"))
    )

    # Count distinct airborne aircraft per window
    # Use snapshot_time for deduplication within window (same aircraft can appear in multiple snapshots)
    airborne_per_window = (windowed
        .filter(F.col("airborne") == True)
        .groupBy("window_start", "window_end")
        .agg(
            F.approx_count_distinct("icao24").alias("airborne_count"),
            F.countDistinct("snapshot_time").alias("snapshot_count")
        )
    )

    # Add time features (Paris timezone)
    airborne_per_window = (airborne_per_window
        .withColumn("hour", F.hour(F.from_utc_timestamp("window_start", "Europe/Paris")))
        .withColumn("weekday", F.dayofweek(F.from_utc_timestamp("window_start", "Europe/Paris")) - 1)  # 0=Mon
        .withColumn("is_weekend", F.when(F.col("weekday").isin([5, 6]), 1).otherwise(0))
        .withColumn("date", F.to_date("window_start"))
    )

    return airborne_per_window.orderBy("window_start")


def create_lag_features(df, lags=[1, 3, 6, 12]):
    """
    Create lag features for time series forecasting.

    Args:
        df: DataFrame with window_start and airborne_count
        lags: list of lag periods in 5-min windows (1=5min, 3=15min, 6=30min, 12=60min)

    Returns:
        DataFrame with lag columns added
    """
    w = Window.orderBy("window_start")

    for lag in lags:
        df = df.withColumn(f"airborne_lag_{lag}", F.lag("airborne_count", lag).over(w))

    return df


def create_target(df, horizon=FORECAST_HORIZON_WINDOWS):
    """
    Create target variable: airborne count at t+horizon.

    Args:
        df: DataFrame with window_start and airborne_count
        horizon: number of 5-min windows ahead (default 6 = 30 min)

    Returns:
        DataFrame with target column
    """
    w = Window.orderBy("window_start")
    df = df.withColumn("target", F.lead("airborne_count", horizon).over(w))
    return df


def create_rolling_features(df, windows=[3, 6, 12]):
    """
    Create rolling statistics features.

    Args:
        df: DataFrame with airborne_count
        windows: list of window sizes in 5-min periods

    Returns:
        DataFrame with rolling mean/std columns
    """
    w = Window.orderBy("window_start")

    for window in windows:
        df = (df
            .withColumn(f"rolling_mean_{window}", F.mean("airborne_count").over(w.rowsBetween(-window + 1, 0)))
            .withColumn(f"rolling_std_{window}", F.stddev("airborne_count").over(w.rowsBetween(-window + 1, 0)))
        )

    return df


def prepare_training_data(spark, start_date=None, end_date=None):
    """
    Full feature engineering pipeline.

    Returns:
        DataFrame ready for ML training with columns:
        - window_start, window_end
        - airborne_count (current)
        - airborne_lag_1, _3, _6, _12 (5, 15, 30, 60 min ago)
        - rolling_mean_3, _6, _12
        - rolling_std_3, _6, _12
        - hour, weekday, is_weekend
        - target (airborne at t+30min)
    """
    # Load and aggregate
    df = load_curated_data(spark, start_date, end_date)
    df = compute_5min_windows(df)

    # Create features
    df = create_lag_features(df)
    df = create_rolling_features(df)
    df = create_target(df)

    # Drop rows with nulls (from lags/rolling at start, and target at end)
    feature_cols = [
        "airborne_count",
        "airborne_lag_1", "airborne_lag_3", "airborne_lag_6", "airborne_lag_12",
        "rolling_mean_3", "rolling_mean_6", "rolling_mean_12",
        "rolling_std_3", "rolling_std_6", "rolling_std_12",
        "hour", "weekday", "is_weekend"
    ]

    df = df.dropna(subset=feature_cols + ["target"])

    return df.select("window_start", "window_end", *feature_cols, "target")


def save_features(df, output_path):
    """Save features as Parquet for training."""
    df.write.mode("overwrite").parquet(output_path)
    print(f"Features saved to {output_path}: {df.count()} rows")


def load_features(spark, input_path):
    """Load features from Parquet."""
    return spark.read.parquet(input_path)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create ML features from curated data")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="/tmp/airborne_features", help="Output path")
    args = parser.parse_args()

    spark = get_spark()
    df = prepare_training_data(spark, args.start_date, args.end_date)
    save_features(df, args.output)
    spark.stop()