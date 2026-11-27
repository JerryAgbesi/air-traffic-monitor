"""
Model evaluation and comparison for air traffic forecasting.

Loads trained models, evaluates on test data, generates comparison plots,
and produces model cards for documentation.
"""
import os
import json
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

from .features import get_spark, prepare_training_data, FEATURE_COLS
from .train import evaluate_model, persistence_baseline, load_model, MODEL_PATH


def load_test_data(spark, start_date=None, end_date=None, test_fraction=0.2):
    """Load the same test split used in training."""
    df = prepare_training_data(spark, start_date, end_date)
    total = df.count()
    train_count = int(total * (1 - test_fraction))
    test_df = df.orderBy(F.desc("window_start")).limit(total - train_count).orderBy("window_start")
    return test_df


def evaluate_saved_model(model_name, test_df):
    """Evaluate a saved pipeline model on test data."""
    model = load_model(model_name)
    predictions = model.transform(test_df)
    return evaluate_model(predictions), predictions


def evaluate_all_models(spark, test_df):
    """Evaluate all saved models plus persistence baseline."""
    results = {}
    predictions = {}

    # Persistence baseline
    print("Evaluating persistence baseline...")
    pers_pred = persistence_baseline(test_df)
    results["persistence"] = evaluate_model(pers_pred, pred_col="persistence_pred")
    predictions["persistence"] = pers_pred

    # Load and evaluate saved models
    model_names = ["linear_regression", "random_forest"]
    for name in model_names:
        try:
            print(f"Evaluating {name}...")
            metrics, preds = evaluate_saved_model(name, test_df)
            results[name] = metrics
            predictions[name] = preds
        except FileNotFoundError:
            print(f"  Model {name} not found, skipping")

    return results, predictions


def print_comparison(results):
    """Print formatted comparison table."""
    print("\n" + "=" * 60)
    print("MODEL COMPARISON (MAE in aircraft count)")
    print("=" * 60)
    print(f"{'Model':<25} {'MAE':>8} {'RMSE':>8} {'R²':>8}")
    print("-" * 60)

    baseline_mae = results.get("persistence", {}).get("mae", 0)

    for name, metrics in results.items():
        mae = metrics.get("mae", float('inf'))
        rmse = metrics.get("rmse", float('inf'))
        r2 = metrics.get("r2", 0)
        marker = ""
        if name != "persistence" and baseline_mae > 0:
            if mae < baseline_mae:
                marker = " ✓ BEATS BASELINE"
            else:
                marker = " ✗ Worse than baseline"
        print(f"{name:<25} {mae:>8.2f} {rmse:>8.2f} {r2:>8.4f}{marker}")

    print("-" * 60)

    # Best model
    best = min(
        ((name, m["mae"]) for name, m in results.items() if name != "persistence"),
        key=lambda x: x[1],
        default=(None, float('inf'))
    )
    if best[0]:
        print(f"\nBest model: {best[0]} (MAE: {best[1]:.2f})")
        improvement = ((baseline_mae - best[1]) / baseline_mae * 100) if baseline_mae > 0 else 0
        print(f"Improvement over persistence: {improvement:.1f}%")


def detailed_error_analysis(predictions, model_name):
    """Analyze prediction errors by hour, weekday, etc."""
    df = predictions

    # Add error columns
    if model_name == "persistence":
        pred_col = "persistence_pred"
    else:
        pred_col = "prediction"

    df = df.withColumn("error", F.col(pred_col) - F.col("target"))
    df = df.withColumn("abs_error", F.abs("error"))
    df = df.withColumn("pct_error", F.when(F.col("target") != 0, F.abs("error") / F.col("target") * 100).otherwise(0))

    print(f"\n--- {model_name} Error Analysis ---")

    # Overall stats
    stats = df.agg(
        F.mean("error").alias("mean_error"),
        F.mean("abs_error").alias("mae"),
        F.stddev("abs_error").alias("std_abs_error"),
        F.max("abs_error").alias("max_abs_error"),
        F.mean("pct_error").alias("mean_pct_error")
    ).collect()[0]
    print(f"Mean Error (bias):     {stats['mean_error']:.2f}")
    print(f"MAE:                   {stats['mae']:.2f}")
    print(f"Std Abs Error:         {stats['std_abs_error']:.2f}")
    print(f"Max Abs Error:         {stats['max_abs_error']:.2f}")
    print(f"Mean % Error:          {stats['mean_pct_error']:.1f}%")

    # By hour
    print("\nError by Hour (Paris time):")
    hourly = df.groupBy("hour").agg(
        F.mean("abs_error").alias("mae"),
        F.count("*").alias("n")
    ).orderBy("hour").collect()
    for row in hourly:
        print(f"  Hour {row['hour']:2d}: MAE={row['mae']:.2f} (n={row['n']})")

    # By weekday
    print("\nError by Weekday (0=Mon):")
    daily = df.groupBy("weekday").agg(
        F.mean("abs_error").alias("mae"),
        F.count("*").alias("n")
    ).orderBy("weekday").collect()
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for row in daily:
        print(f"  {days[row['weekday']]}: MAE={row['mae']:.2f} (n={row['n']})")

    # By traffic level (tertiles)
    print("\nError by Traffic Level:")
    quantiles = df.approxQuantile("airborne_count", [0.33, 0.67], 0.01)
    df = df.withColumn(
        "traffic_level",
        F.when(F.col("airborne_count") <= quantiles[0], "low")
        .when(F.col("airborne_count") <= quantiles[1], "medium")
        .otherwise("high")
    )
    traffic_error = df.groupBy("traffic_level").agg(
        F.mean("abs_error").alias("mae"),
        F.mean("pct_error").alias("mean_pct"),
        F.count("*").alias("n")
    ).collect()
    for row in traffic_error:
        print(f"  {row['traffic_level']:6s}: MAE={row['mae']:.2f}, %Err={row['mean_pct']:.1f}% (n={row['n']})")


def save_evaluation_report(results, predictions, output_path):
    """Save detailed evaluation report as JSON."""
    report = {
        "evaluated_at": datetime.utcnow().isoformat() + "Z",
        "models": {}
    }

    for name, metrics in results.items():
        report["models"][name] = metrics

    # Add best model
    best = min(
        ((name, m["mae"]) for name, m in results.items() if name != "persistence"),
        key=lambda x: x[1],
        default=(None, float('inf'))
    )
    if best[0]:
        report["best_model"] = best[0]
        report["best_mae"] = best[1]

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nEvaluation report saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Evaluate trained forecasting models")
    parser.add_argument("--start-date", help="Test data start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Test data end date (YYYY-MM-DD)")
    parser.add_argument("--output", default=f"{MODEL_PATH}/evaluation_report.json", help="Output report path")
    parser.add_argument("--detail", action="store_true", help="Show detailed error analysis")
    args = parser.parse_args()

    spark = get_spark("evaluate_forecaster")
    try:
        print("Loading test data...")
        test_df = load_test_data(spark, args.start_date, args.end_date)
        print(f"Test rows: {test_df.count()}")

        print("\nEvaluating models...")
        results, predictions = evaluate_all_models(spark, test_df)

        print_comparison(results)

        if args.detail:
            for name in predictions:
                if name != "persistence":  # Skip persistence for detailed analysis
                    detailed_error_analysis(predictions[name], name)

        save_evaluation_report(results, predictions, args.output)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()