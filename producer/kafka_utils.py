# Creates the Kafka topic and sends messages
import json
import logging

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import TOPIC_PARTITIONS, TOPIC_RETENTION_MS

log = logging.getLogger(__name__)


def ensure_topic(bootstrap, topic):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    fs = admin.create_topics([NewTopic(
        topic, num_partitions=TOPIC_PARTITIONS, replication_factor=1,
        config={"retention.ms": str(TOPIC_RETENTION_MS)})])
    for t, f in fs.items():
        try:
            f.result()
            log.info("Created topic %s", t)
        except Exception as e:
            log.info("Topic %s: %s", t, e)


def make_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
    })


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key %s: %s", msg.key(), err)


def send_messages(producer, topic, msgs):
    for key, value in msgs:
        producer.produce(
            topic, key=key.encode(),
            value=json.dumps(value).encode(),
            timestamp=value["time"] * 1000,
            on_delivery=delivery_report)
        producer.poll(0)
    producer.flush(10)
