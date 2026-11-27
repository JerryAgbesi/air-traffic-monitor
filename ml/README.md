# ML Forecasting Module

Predicts airborne aircraft count over French airspace 30 minutes ahead (6 × 5-min windows).

## Features

- **Target**: Airborne aircraft count at t+30min
- **Features**: 
  - Lag features: counts at t, t-15min, t-30min, t-60min (1, 3, 6, 12 windows)
  - Rolling statistics: mean/std over 3, 6, 12 windows (15, 30, 60 min)
  - Time features: hour (0-23 Paris), weekday (0=Mon), is_weekend
- **Models**: Linear Regression, Random Forest (Spark MLlib)
- **Baseline**: Persistence (forecast = current count)
- **Metric**: MAE (Mean Absolute Error) in aircraft count

## Quick Start (Local Development)

```bash
# Install dependencies
pip install -r requirements.txt

# Generate features from curated data (requires Spark + HDFS access)
python -m ml.features --start-date 2026-11-25 --end-date 2026-11-27 --output /tmp/features

# Train models
python -m ml.train --start-date 2026-11-25 --end-date 2026-11-27

# Evaluate models
python -m ml.evaluate --detail

# Generate single forecast
python -m ml.forecast --once --output console
```

## Cluster Deployment (YARN)

### Training (Batch Job)
```bash
./submit_train.sh [--cv] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
```

Options:
- `--cv`: Run cross-validation for hyperparameter tuning (slower)
- `--start-date` / `--end-date`: Training data date range (default: all available)

Outputs:
- Models saved to `/models/airborne_forecaster/{linear_regression,random_forest}/`
- Metrics in `metrics.json` per model
- Comparison in `model_comparison.json`

### Forecasting (Streaming Job)
```bash
./submit_forecast.sh [--model linear_regression|random_forest] [--interval 5]
```

Runs continuously, generating forecast every 5 minutes (aligned with 5-min windows).
Outputs to Kafka topic `opensky.forecast.30min`.

## Data Flow

```
Curated HDFS (/data/curated/opensky_states)
    │
    ▼
features.py: prepare_training_data()
    │  ├─ Load curated Parquet
    │  ├─ Aggregate to 5-min windows (distinct airborne count)
    │  ├─ Add lag features (t-5,15,30,60min)
    │  ├─ Add rolling mean/std (15,30,60min)
    │  ├─ Add time features (hour, weekday, weekend)
    │  └─ Create target (t+30min)
    ▼
Feature DataFrame (Parquet)
    │
    ▼
train.py: train_and_evaluate()
    │  ├─ Chronological split (80/20)
    │  ├─ Persistence baseline
    │  ├─ Linear Regression + CV
    │  ├─ Random Forest + CV
    │  ├─ Feature importance (RF)
    │  └─ Save best pipeline model
    ▼
Saved Models (HDFS /models/airborne_forecaster/)
    │
    ▼
forecast.py: generate_forecast()
    │  ├─ Load latest curated data (last 4 hours)
    │  ├─ Compute features for latest window
    │  ├─ Load trained pipeline model
    │  ├─ Predict t+30min airborne count
    │  └─ Publish to Kafka
    ▼
Kafka: opensky.forecast.30min
    │
    ▼
Dashboard consumes for "Forecast vs Actual" chart
```

## Model Details

### Persistence Baseline
- Forecast = current airborne count
- Simple but strong baseline for short horizons
- MAE typically ~5-15 aircraft depending on traffic volatility

### Linear Regression
- Ridge regularization (L2)
- Interpretable coefficients
- Fast training and inference
- Good baseline ML model

### Random Forest
- 100 trees, max depth 10
- Captures non-linear patterns
- Feature importance available
- Typically beats Linear Regression on this task

## Feature Engineering Notes

1. **5-minute windows**: Aligned with Kafka producer polling (75s) and ingest trigger (5min)
2. **Distinct airborne count**: Uses `approx_count_distinct(icao24)` per window
3. **Lag features**: 
   - lag_1 = 5 min ago
   - lag_3 = 15 min ago  
   - lag_6 = 30 min ago
   - lag_12 = 60 min ago
4. **Rolling windows**: Mean/std over last 3, 6, 12 windows (15, 30, 60 min)
5. **Time features**: Paris timezone (Europe/Paris) for hour/weekday
6. **Target**: Lead of 6 windows (30 min ahead)

## Requirements

- Spark 4.0.3 (Scala 2.13)
- HDFS access to curated zone
- YARN cluster for distributed training
- Kafka for forecast output
- Python 3.11+ with pyspark

## Monitoring

- Check YARN UI for job status
- Model metrics in `/models/airborne_forecaster/*/metrics.json`
- Forecast topic lag: `kafka-consumer-groups.sh --group forecast-consumer --describe`
- Dashboard: "Forecast vs Actual" chart shows real-time performance

## Next Steps

1. **Hyperparameter tuning**: Expand CV grids for production
2. **Model versioning**: Add MLflow tracking
3. **Online learning**: Periodic retraining with new data
4. **Ensemble**: Combine LR + RF predictions
5. **Exogenous features**: Weather, airport schedules, holidays