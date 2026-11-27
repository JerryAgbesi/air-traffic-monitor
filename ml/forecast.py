"""
Real-time forecasting for air traffic monitoring.

Loads trained model and generates 30-minute ahead forecasts
from the latest curated data. Outputs to Kafka topic for dashboard consumption.
"""
import os
import json
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml import PipelineModel

from .features import (
    get_spark, compute_5min_windows, create_lag_features,
    create_rolling_features, CURATED_PATH
)
from .train import load_model, FEATURE_COLS, MODEL_PATH


# Configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "worker2:9092,worker3:9092,worker4:9092")
FORECAST_TOPIC = os.getenv("FORECAST_TOPIC", "opensky.forecast.30min")
MODEL_NAME = os.getenv("FORECAST_MODEL", "random_forest")  # or linear_regression
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "4"))  # How much history to fetch for features


# Forecast output schema
FORECAST_SCHEMA = T.StructType([
    T.StructField("forecast_time", T.TimestampType()),     # When forecast was made (now)
    T.StructField("valid_from", T.TimestampType()),        # Start of forecast window (t+30min)
    T.StructField("valid_to", T.TimestampType()),          # End of forecast window (t+35min)
    T.StructField("predicted_airborne", T.DoubleType()),   # Predicted count
    T.StructField("current_airborne", T.DoubleType()),     # Current count at forecast time
    T.StructField("model", T.StringType()),                # Model name used
    T.StructField("model_version", T.StringType()),        # Model version/timestamp
])


def load_latest_curated(spark, hours=LOOKBACK_HOURS):
    """
    Load recent curated data to compute current features.
    Need enough history for lag features (up to 60 min = 12 windows).
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    df = spark.read.parquet(CURATED_PATH)
    df = df.filter(F.col("ts") >= since_str)
    return df


def prepare_latest_features(df):
    """
    Compute features for the latest window from curated data.
    Returns the feature row for the most recent complete 5-min window.
    """
    # Aggregate to 5-min windows
    windowed = compute_5min_windows(df)

    # Create lag and rolling features
    windowed = create_lag_features(windowed)
    windowed = create_rolling_features(windowed)

    # Get the most recent window (has all lag features populated)
    latest = windowed.orderBy(F.desc("window_start")).limit(1)

    # Select feature columns
    feature_cols = ["window_start", "window_end"] + FEATURE_COLS
    latest = latest.select(*feature_cols)

    return latest


def generate_forecast(spark, model_name=MODEL_NAME):
    """
    Generate a single 30-minute ahead forecast.

    Returns:
        DataFrame with forecast output schema
    """
    # Load model
    model = load_model(model_name)

    # Get model metadata
    model_version = "unknown"
    try:
        metrics_path = f"{MODEL_PATH}/{model_name}/metrics.json"
        # Try to read from local filesystem first
        import os
        if os.path.exists(metrics_path):
            with open(metrics_path) as f:
                meta = json.load(f)
                model_version = meta.get("trained_at", "unknown")
    except:
        pass

    # Load recent curated data
    curated = load_latest_curated(spark)

    if curated.count() == 0:
        raise ValueError("No recent curated data available for forecasting")

    # Prepare features
    features_df = prepare_latest_features(curated)

    if features_df.count() == 0:
        raise ValueError("Could not compute features (not enough history for lags)")

    # Make prediction
    forecast_time = datetime.now(timezone.utc)
    prediction = model.transform(features_df)

    # Extract the prediction
    pred_row = prediction.select("window_start", "window_end", "airborne_count", "prediction").collect()[0]

    # Forecast is valid for t+30min (6 windows ahead)
    valid_from = pred_row["window_start"] + timedelta(minutes=30)
    valid_to = valid_from + timedelta(minutes=5)

    # Build forecast row
    forecast_row = spark.createDataFrame([{
        "forecast_time": forecast_time,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "predicted_airborne": float(pred_row["prediction"]),
        "current_airborne": float(pred_row["airborne_count"]),
        "model": model_name,
        "model_version": model_version
    }], FORECAST_SCHEMA)

    return forecast_row


def write_to_kafka(df, topic=FORECAST_TOPIC, bootstrap=KAFKA_BOOTSTRAP):
    """Write forecast to Kafka topic."""
    (df.select(F.to_json(F.struct("*")).alias("value"))
     .write.format("kafka")
     .option("kafka.bootstrap.servers", bootstrap)
     .option("topic", topic)
     .save())


def write_to_console(df):
    """Print forecast to console for debugging."""
    row = df.collect()[0]
    print(f"\n=== FORECAST ===")
    print(f"Forecast time:  {row['forecast_time']}")
    print(f"Valid from:     {row['valid_from']} to {row['valid_to']}")
    print(f"Predicted:      {row['predicted_airborne']:.1f} airborne aircraft")
    print(f"Current:        {row['current_airborne']:.1f} airborne aircraft")
    print(f"Model:          {row['model']} (v{row['model_version']})")
    print(f"================\n")


def continuous_forecast(spark, interval_minutes=5, output="kafka"):
    """
    Run continuous forecasting loop.

    Args:
        interval_minutes: How often to generate forecasts (should align with 5-min windows)
        output: "kafka" or "console"
    """
    import time

    print(f"Starting continuous forecasting (every {interval_minutes} min, output={output})")
    print(f"Model: {MODEL_NAME}")

    while True:
        try:
            forecast = generate_forecast(spark)

            if output == "kafka":
                write_to_kafka(forecast)
                print(f"[{datetime.now(timezone.utc).isoformat()}] Forecast sent to Kafka")
            else:
                write_to_console(forecast)

        except Exception as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] Forecast error: {e}")

        # Sleep until next interval
        time.sleep(interval_minutes * 60)


def main():
    parser = argparse.ArgumentParser(description="Generate 30-min ahead airborne forecasts")
    parser.add_argument("--once", action="store_true", help="Generate single forecast and exit")
    parser.add_argument("--interval", type=int, default=5, help="Forecast interval in minutes (continuous mode)")
    parser.add_argument("--output", choices=["kafka", "console"], default="console", help="Output destination")
    parser.add_argument("--model", default=MODEL_NAME, help="Model to use (linear_regression or random_forest)")
    args = parser.parse_args()

    global MODEL_NAME
    MODEL_NAME = args.model

    spark = get_spark("forecast_airborne")
    try:
        if args.once:
            forecast = generate_forecast(spark)
            if args.output == "kafka":
                write_to_kafka(forecast)
            else:
                write_to_console(forecast)
        else:
            continuous_forecast(spark, args.interval, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()