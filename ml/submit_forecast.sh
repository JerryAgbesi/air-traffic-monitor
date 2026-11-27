#!/bin/bash
# Submit forecasting job to YARN cluster (runs continuously)
# Usage: ./submit_forecast.sh [--model linear_regression|random_forest] [--interval 5]

set -e

SPARK_HOME=${SPARK_HOME:-/opt/spark}
JARS_DIR=${JARS_DIR:-/opt/spark/jars}
ML_DIR=$(cd "$(dirname "$0")" && pwd)

MODEL="random_forest"
INTERVAL=5

while [[ $# -gt 0 ]]; do
    case $1 in
        --model)
            MODEL="$2"
            shift 2
            ;;
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

JARS=$(find "$JARS_DIR" -name "*.jar" | tr '\n' ',' | sed 's/,$//')

"$SPARK_HOME/bin/spark-submit" \
    --master yarn \
    --deploy-mode cluster \
    --name "airborne-forecaster-inference" \
    --queue default \
    --driver-memory 1g \
    --executor-memory 2g \
    --executor-cores 1 \
    --num-executors 1 \
    --jars "$JARS" \
    --py-files "$ML_DIR/features.py,$ML_DIR/forecast.py,$ML_DIR/train.py" \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.yarn.maxAppAttempts=4 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    "$ML_DIR/forecast.py" --model "$MODEL" --interval "$INTERVAL" --output kafka