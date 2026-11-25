#!/usr/bin/env bash
# Run on: node1 (master), from the pipeline folder.
# Submits one Spark job to YARN in cluster mode.
#
#   deploy/submit.sh raw_sink
#   deploy/submit.sh traffic_agg
#   deploy/submit.sh live_kpis
#   deploy/submit.sh clean_daily [YYYY-MM-DD [YYYY-MM-DD]]
#   deploy/submit.sh backfill_jsonl hdfs:///staging/old_archive
set -euo pipefail
cd "$(dirname "$0")/.."
JOB=$1; shift
[ -f .env ] && set -a && . ./.env && set +a

SPARK_HOME=${SPARK_HOME:-/opt/spark}
# Spark version taken from the installed jars, so the Kafka package always matches
SPARK_VERSION=$(ls $SPARK_HOME/jars/spark-core_2.12-*.jar | sed -E 's/.*spark-core_2.12-(.*)\.jar/\1/')
PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:$SPARK_VERSION,org.postgresql:postgresql:42.7.4

case $JOB in
  raw_sink)       RES="--num-executors 2 --executor-cores 1 --executor-memory 1g" ;;
  traffic_agg)    RES="--num-executors 2 --executor-cores 1 --executor-memory 1g" ;;
  live_kpis)      RES="--num-executors 2 --executor-cores 1 --executor-memory 1g" ;;
  *)              RES="--num-executors 3 --executor-cores 2 --executor-memory 2g" ;;
esac

ENVCONF=""
for v in KAFKA_BOOTSTRAP TSDB_URL TSDB_USER TSDB_PASSWORD LAKE_ROOT CHECKPOINT_ROOT; do
  if [ -n "${!v:-}" ]; then
    ENVCONF="$ENVCONF --conf spark.yarn.appMasterEnv.$v=${!v} --conf spark.executorEnv.$v=${!v}"
  fi
done

exec $SPARK_HOME/bin/spark-submit \
  --master yarn --deploy-mode cluster \
  --name "$JOB" \
  --packages "$PACKAGES" \
  --py-files jobs/common.py \
  $RES \
  --driver-memory 1g \
  --conf spark.yarn.maxAppAttempts=4 \
  --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.sql.streaming.metricsEnabled=true \
  $ENVCONF \
  jobs/$JOB.py "$@"
