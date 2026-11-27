"""
Model training for air traffic forecasting.

Trains and evaluates:
- Linear Regression (baseline ML model)
- Random Forest Regressor (non-linear model)
- Persistence baseline (forecast = current count)

All models use Spark MLlib for distributed training.
"""
import os
import json
import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from .features import (
    get_spark, prepare_training_data, load_features, save_features,
    FEATURE_WINDOW_MINUTES, FORECAST_HORIZON_WINDOWS
)


# Configuration
MODEL_PATH = os.getenv("MODEL_PATH", "/models/airborne_forecaster")
FEATURES_PATH = os.getenv("FEATURES_PATH", "/tmp/airborne_features")


# Feature columns (must match features.py)
FEATURE_COLS = [
    "airborne_count",
    "airborne_lag_1", "airborne_lag_3", "airborne_lag_6", "airborne_lag_12",
    "rolling_mean_3", "rolling_mean_6", "rolling_mean_12",
    "rolling_std_3", "rolling_std_6", "rolling_std_12",
    "hour", "weekday", "is_weekend"
]


def persistence_baseline(df):
    """
    Persistence model: forecast = current airborne count.
    Returns DataFrame with prediction column.
    """
    return df.withColumn("persistence_pred", F.col("airborne_count"))


def evaluate_model(predictions, label_col="target", pred_col="prediction"):
    """
    Evaluate regression model with MAE, RMSE, R2.
    """
    evaluator_mae = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col, metricName="r2")

    mae = evaluator_mae.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    return {"mae": mae, "rmse": rmse, "r2": r2}


def train_linear_regression(train_df, feature_col="features", label_col="target"):
    """Train Linear Regression model."""
    lr = LinearRegression(
        featuresCol=feature_col,
        labelCol=label_col,
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0,  # Ridge
        standardization=True
    )
    return lr.fit(train_df)


def train_random_forest(train_df, feature_col="features", label_col="target"):
    """Train Random Forest Regressor."""
    rf = RandomForestRegressor(
        featuresCol=feature_col,
        labelCol=label_col,
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=5,
        featureSubsetStrategy="auto",
        seed=42
    )
    return rf.fit(train_df)


def build_pipeline(model, feature_cols=FEATURE_COLS):
    """Build ML pipeline with feature assembly and scaling."""
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    return Pipeline(stages=[assembler, scaler, model])


def cross_validate_model(pipeline, train_df, param_grid, num_folds=3):
    """Cross-validate model with parameter grid."""
    evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="mae")
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=num_folds,
        parallelism=4,
        seed=42
    )
    return cv.fit(train_df)


def save_model(model, model_name, metrics=None):
    """Save model to HDFS/local and write metrics."""
    model_dir = f"{MODEL_PATH}/{model_name}"
    model.write().overwrite().save(model_dir)
    print(f"Model saved to {model_dir}")

    if metrics:
        metrics_path = f"{model_dir}/metrics.json"
        with open(metrics_path, "w") as f:
            json.dump({
                "model": model_name,
                "trained_at": datetime.utcnow().isoformat() + "Z",
                **metrics
            }, f, indent=2)
        print(f"Metrics saved to {metrics_path}")


def load_model(model_name):
    """Load model from HDFS/local."""
    model_dir = f"{MODEL_PATH}/{model_name}"
    # Try PipelineModel first, then fall back to individual models
    try:
        from pyspark.ml import PipelineModel
        return PipelineModel.load(model_dir)
    except:
        pass
    # Try individual model types
    from pyspark.ml.regression import LinearRegressionModel, RandomForestRegressionModel
    for cls in [LinearRegressionModel, RandomForestRegressionModel]:
        try:
            return cls.load(model_dir)
        except:
            pass
    raise FileNotFoundError(f"Could not load model from {model_dir}")


def train_and_evaluate(spark, start_date=None, end_date=None, cv=False):
    """
    Full training pipeline:
    1. Prepare features
    2. Split train/test (chronological: last 20% for test)
    3. Train models
    4. Evaluate on test set
    5. Compare with persistence baseline
    6. Save best model
    """
    print("Preparing training data...")
    df = prepare_training_data(spark, start_date, end_date)

    total_rows = df.count()
    print(f"Total feature rows: {total_rows}")

    if total_rows < 100:
        raise ValueError(f"Not enough data for training: {total_rows} rows (need >100)")

    # Chronological split: first 80% train, last 20% test
    train_count = int(total_rows * 0.8)
    train_df = df.limit(train_count)
    test_df = df.orderBy(F.desc("window_start")).limit(total_rows - train_count).orderBy("window_start")

    print(f"Train: {train_df.count()}, Test: {test_df.count()}")

    # Persistence baseline on test set
    print("\n=== Persistence Baseline ===")
    persistence_test = persistence_baseline(test_df)
    persistence_metrics = evaluate_model(persistence_test, pred_col="persistence_pred")
    print(f"MAE: {persistence_metrics['mae']:.2f}, RMSE: {persistence_metrics['rmse']:.2f}, R2: {persistence_metrics['r2']:.4f}")

    # Build feature pipeline
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

    # Prepare train/test with features
    train_features = scaler.fit(assembler.transform(train_df)).transform(assembler.transform(train_df))
    test_features = scaler.transform(assembler.transform(test_df))

    results = {"persistence": persistence_metrics}

    # Linear Regression
    print("\n=== Linear Regression ===")
    lr = LinearRegression(featuresCol="features", labelCol="target", maxIter=100, regParam=0.1)

    if cv:
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1, 1.0]) \
            .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
            .build()
        cv_model = cross_validate_model(Pipeline(stages=[lr]), train_features, param_grid)
        best_lr = cv_model.bestModel.stages[-1]
        print(f"Best params: regParam={best_lr.getRegParam()}, elasticNetParam={best_lr.getElasticNetParam()}")
    else:
        best_lr = lr.fit(train_features)

    lr_pred = best_lr.transform(test_features)
    lr_metrics = evaluate_model(lr_pred)
    print(f"MAE: {lr_metrics['mae']:.2f}, RMSE: {lr_metrics['rmse']:.2f}, R2: {lr_metrics['r2']:.4f}")
    results["linear_regression"] = lr_metrics

    # Save LR model with preprocessing
    lr_pipeline = Pipeline(stages=[assembler, scaler, best_lr])
    lr_pipeline_model = lr_pipeline.fit(train_df)
    save_model(lr_pipeline_model, "linear_regression", lr_metrics)

    # Random Forest
    print("\n=== Random Forest ===")
    rf = RandomForestRegressor(
        featuresCol="features", labelCol="target",
        numTrees=100, maxDepth=10, minInstancesPerNode=5, seed=42
    )

    if cv:
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [50, 100, 200]) \
            .addGrid(rf.maxDepth, [5, 10, 15]) \
            .build()
        cv_model = cross_validate_model(Pipeline(stages=[rf]), train_features, param_grid)
        best_rf = cv_model.bestModel.stages[-1]
        print(f"Best params: numTrees={best_rf.getNumTrees()}, maxDepth={best_rf.getMaxDepth()}")
    else:
        best_rf = rf.fit(train_features)

    rf_pred = best_rf.transform(test_features)
    rf_metrics = evaluate_model(rf_pred)
    print(f"MAE: {rf_metrics['mae']:.2f}, RMSE: {rf_metrics['rmse']:.2f}, R2: {rf_metrics['r2']:.4f}")
    results["random_forest"] = rf_metrics

    # Save RF model with preprocessing
    rf_pipeline = Pipeline(stages=[assembler, scaler, best_rf])
    rf_pipeline_model = rf_pipeline.fit(train_df)
    save_model(rf_pipeline_model, "random_forest", rf_metrics)

    # Feature importance for RF
    if hasattr(best_rf, "featureImportances"):
        importances = best_rf.featureImportances.toArray()
        feature_importance = list(zip(FEATURE_COLS, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        print("\nFeature Importance (Random Forest):")
        for name, imp in feature_importance[:10]:
            print(f"  {name}: {imp:.4f}")
        results["feature_importance"] = dict(feature_importance)

    # Summary
    print("\n=== Summary ===")
    print(f"Persistence MAE:     {persistence_metrics['mae']:.2f}")
    print(f"Linear Reg MAE:      {lr_metrics['mae']:.2f}  {'✓ BEATS PERSISTENCE' if lr_metrics['mae'] < persistence_metrics['mae'] else '✗'}")
    print(f"Random Forest MAE:   {rf_metrics['mae']:.2f}  {'✓ BEATS PERSISTENCE' if rf_metrics['mae'] < persistence_metrics['mae'] else '✗'}")

    best_model = "linear_regression" if lr_metrics['mae'] < rf_metrics['mae'] else "random_forest"
    print(f"\nBest model: {best_model} (MAE: {results[best_model]['mae']:.2f})")

    # Save comparison
    comparison_path = f"{MODEL_PATH}/model_comparison.json"
    with open(comparison_path, "w") as f:
        json.dump({
            "trained_at": datetime.utcnow().isoformat() + "Z",
            "train_rows": train_df.count(),
            "test_rows": test_df.count(),
            "results": results,
            "best_model": best_model
        }, f, indent=2)
    print(f"Comparison saved to {comparison_path}")

    return results, best_model


def main():
    parser = argparse.ArgumentParser(description="Train air traffic forecasting models")
    parser.add_argument("--start-date", help="Training data start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Training data end date (YYYY-MM-DD)")
    parser.add_argument("--cv", action="store_true", help="Run cross-validation for hyperparameter tuning")
    args = parser.parse_args()

    spark = get_spark("train_forecaster")
    try:
        train_and_evaluate(spark, args.start_date, args.end_date, args.cv)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()