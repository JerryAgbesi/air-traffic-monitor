# Kafka helpers: topic creation and an idempotent producer.
import json
import logging

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

log = logging.getLogger(__name__)


def ensure_topic(bootstrap, topic, partitions, replication, retention_ms):
    """Create the topic if missing (normally create_topics.sh already did it)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    fs = admin.create_topics([NewTopic(
        topic, num_partitions=partitions, replication_factor=replication,
        config={"retention.ms": str(retention_ms),
                "min.insync.replicas": "2" if replication >= 3 else "1"})])
    for t, f in fs.items():
        try:
            f.result()
            log.info("Created topic %s", t)
        except Exception as e:  # already exists, etc.
            log.info("Topic %s: %s", t, e)


class EventProducer:
    """acks=all + idempotence: a retry never writes a duplicate into Kafka."""

    def __init__(self, bootstrap):
        self.failed = 0
        self.p = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
            "client.id": "opensky-producer",
            # a busy snapshot is ~2,000 aircraft, about 300 KB of JSON
            "message.max.bytes": 2000000,
        })

    def _report(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("Delivery failed for key %s: %s", msg.key(), err)

    def send_snapshot(self, topic, payload):
        """One API response = one message. Key and timestamp = snapshot time.
        Returns True once Kafka acknowledged it (acks=all)."""
        self.failed = 0
        snap = int(payload["time"])
        self.p.produce(topic, key=str(snap).encode(),
                       value=json.dumps(payload, separators=(",", ":")).encode(),
                       timestamp=snap * 1000, on_delivery=self._report)
        left = self.p.flush(30)
        return left == 0 and self.failed == 0

    def close(self):
        self.p.flush(30)
