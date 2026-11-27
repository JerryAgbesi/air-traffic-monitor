#!/bin/bash
# Submits the Kafka -> HDFS raw ingestion stream to YARN (run on master).
# Cluster mode: the driver runs in a YARN container, so the job keeps running
# after you log out and does not use the master's memory.
# Containers must fit in yarn.scheduler.maximum-allocation-mb (2048):
# 1g + 384m overhead each.
set -euo pipefail
cd "$(dirname "$0")"

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/usr/local/hadoop/etc/hadoop}
SPARK_VERSION=$(/opt/spark/bin/spark-submit --version 2>&1 | grep -oE "version [0-9.]+" | head -1 | cut -d" " -f2)

if /usr/local/hadoop/bin/yarn application -list 2>/dev/null | grep -q opensky-ingest-raw; then
  echo "opensky-ingest-raw is already running on YARN" >&2
  exit 1
fi

/opt/spark/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --name opensky-ingest-raw \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}" \
  --driver-memory 1g \
  --num-executors 1 --executor-memory 1g --executor-cores 1 \
  --conf spark.yarn.submit.waitAppCompletion=false \
  ingest_raw.py
