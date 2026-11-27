#!/bin/bash
# Submit ML training job to YARN cluster
# Usage: ./submit_train.sh [--cv] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

set -e

# Configuration
SPARK_HOME=${SPARK_HOME:-/opt/spark}
JARS_DIR=${JARS_DIR:-/opt/spark/jars}
ML_DIR=$(cd "$(dirname "$0")" && pwd)

# Default args
CV_FLAG=""
START_DATE=""
END_DATE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --cv)
            CV_FLAG="--cv"
            shift
            ;;
        --start-date)
            START_DATE="--start-date $2"
            shift 2
            ;;
        --end-date)
            END_DATE="--end-date $2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Required JARs for Kafka, HDFS, etc.
JARS=$(find "$JARS_DIR" -name "*.jar" | tr '\n' ',' | sed 's/,$//')

# Submit to YARN
"$SPARK_HOME/bin/spark-submit" \
    --master yarn \
    --deploy-mode cluster \
    --name "airborne-forecaster-train" \
    --queue default \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 2 \
    --num-executors 4 \
    --jars "$JARS" \
    --py-files "$ML_DIR/features.py,$ML_DIR/train.py,$ML_DIR/evaluate.py" \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.yarn.maxAppAttempts=2 \
    "$ML_DIR/train.py" $CV_FLAG $START_DATE $END_DATE