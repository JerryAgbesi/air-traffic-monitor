#!/usr/bin/env bash
# Run on: node2 (any one Kafka node), once.
# Creates the two topics from the Checkpoint 2 report.
set -euo pipefail
KAFKA=${KAFKA_HOME:-/opt/kafka}
BS=node2:9092,node3:9092,node4:9092

$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic opensky.states.raw --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=1209600000 \
  --config max.message.bytes=2097152   # 14 days; one message = one whole snapshot

$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists \
  --topic opensky.traffic.5min --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=604800000    # 7 days

$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --describe \
  --topic opensky.states.raw
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --describe \
  --topic opensky.traffic.5min
