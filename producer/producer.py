"""OpenSky -> Kafka producer (runs on node6 as systemd service opensky-producer).

Every POLL_SECONDS it calls /states/all for the France bounding box and sends
the whole API response as ONE MESSAGE to opensky.states.raw, exactly as
received ({"time": ..., "states": [[...17 fields...], ...]}), keyed by the
snapshot time, with the Kafka timestamp = snapshot time (same design as PR #2).
Splitting into one row per aircraft and all cleaning happen in Spark.
"""
import logging
import signal
import time

import requests

import config
from kafka_utils import EventProducer, ensure_topic
from opensky_client import OpenSkyClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

running = True


def stop(*_):
    global running
    running = False
    log.info("Stopping after current cycle...")


def sleep_while_running(seconds):
    end = time.time() + seconds
    while running and time.time() < end:
        time.sleep(1)


class Health:
    """Optional: producer metrics into TimescaleDB pipeline_health."""

    def __init__(self, dsn):
        self.dsn = dsn
        self.conn = None

    def write(self, metrics):
        if not self.dsn:
            return
        try:
            import psycopg2
            if self.conn is None or self.conn.closed:
                self.conn = psycopg2.connect(self.dsn, connect_timeout=5)
                self.conn.autocommit = True
            with self.conn.cursor() as cur:
                for metric, value in metrics.items():
                    if value is None:
                        continue
                    cur.execute(
                        "INSERT INTO pipeline_health(time, source, metric, value) "
                        "VALUES (now(), 'producer', %s, %s) "
                        "ON CONFLICT (time, source, metric) DO UPDATE SET value = EXCLUDED.value",
                        (metric, float(value)))
        except Exception as e:  # never stop producing because of metrics
            log.warning("pipeline_health write failed: %s", e)
            self.conn = None


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    if not config.CLIENT_ID:
        log.warning("No OPENSKY_CLIENT_ID set: running anonymously (low quota)")
    ensure_topic(config.BOOTSTRAP, config.TOPIC, config.TOPIC_PARTITIONS,
                 config.TOPIC_REPLICATION, config.TOPIC_RETENTION_MS)
    producer = EventProducer(config.BOOTSTRAP)
    client = OpenSkyClient(config.CLIENT_ID, config.CLIENT_SECRET)
    health = Health(config.TSDB_DSN)
    last_snapshot = None
    log.info("Producing to %s on %s every %s s",
             config.TOPIC, config.BOOTSTRAP, config.POLL_SECONDS)

    while running:
        started = time.time()
        wait_extra = 0
        try:
            payload, wait_extra = client.fetch_states()
            fetch_s = time.time() - started
            if payload:
                snap = payload.get("time")
                if snap == last_snapshot:
                    # OpenSky served the same snapshot again: sending it would
                    # only create duplicates downstream.
                    log.info("Snapshot %s already sent, skipping", snap)
                else:
                    n = len(payload.get("states") or [])
                    ok = producer.send_snapshot(config.TOPIC, payload)
                    last_snapshot = snap
                    log.info("Snapshot %s: sent %d aircraft%s, credits left %s",
                             snap, n, "" if ok else " (DELIVERY FAILED)",
                             client.credits_left)
                    health.write({"aircraft_sent": n if ok else 0,
                                  "delivery_failed": 0 if ok else 1,
                                  "api_credits_left": client.credits_left,
                                  "fetch_seconds": fetch_s,
                                  "snapshot_age_s": time.time() - snap})
        except requests.RequestException as e:
            log.error("HTTP error: %s", e)
            wait_extra = 30
        except Exception:
            log.exception("Unexpected error")
            wait_extra = 30

        elapsed = time.time() - started
        sleep_while_running(max(0, config.POLL_SECONDS - elapsed) + wait_extra)

    producer.close()
    log.info("Producer stopped")


if __name__ == "__main__":
    main()
