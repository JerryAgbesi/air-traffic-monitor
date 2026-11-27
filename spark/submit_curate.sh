#!/bin/bash
# Runs the curated/InfluxDB batch job on YARN (run on master) and waits for it.
# Arguments are passed to curate.py, e.g.  submit_curate.sh --date 2026-11-25
# The InfluxDB token is shipped as a file (--files), so it never appears in
# the process list or in the Spark UI.
set -euo pipefail
cd "$(dirname "$0")"

export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/usr/local/hadoop/etc/hadoop}
INFLUX_ENV=${INFLUX_ENV:-$HOME/.config/opensky/influx.env}

exec /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --name opensky-curate \
  --driver-memory 1g \
  --num-executors 2 --executor-memory 1g --executor-cores 1 \
  --conf spark.sql.shuffle.partitions=8 \
  --files "${INFLUX_ENV}#influx.env" \
  curate.py "$@"
