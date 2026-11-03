"""
Kafka -> local archive consumer.

Reads raw events from Kafka and appends them to one JSON-lines file per UTC day:
    data/raw/date=YYYY-MM-DD/states.jsonl
This is the history the ML part will train on, and a backup if Kafka is reset.

Run:  python consumer/archive_consumer.py
"""
import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "opensky.states.raw")
OUT_DIR = Path(os.getenv("ARCHIVE_DIR", "data/raw"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("archiver")

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "raw-archiver",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    files = {}
    count = 0
    log.info("Archiving %s to %s", TOPIC, OUT_DIR)
    try:
        while running:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            if not msgs:
                continue
            for msg in msgs:
                if msg.error():
                    log.error("Kafka error: %s", msg.error())
                    continue
                value = json.loads(msg.value())
                day = datetime.fromtimestamp(
                    value.get("event_time", value.get("time")),
                    tz=timezone.utc).strftime("%Y-%m-%d")
                if day not in files:
                    path = OUT_DIR / f"date={day}"
                    path.mkdir(parents=True, exist_ok=True)
                    files[day] = open(path / "states.jsonl", "a", encoding="utf-8")
                files[day].write(json.dumps(value) + "\n")
                count += 1
            for f in files.values():
                f.flush()
            consumer.commit(asynchronous=False)  # commit only after writing
            if count and count % 5000 < len(msgs):
                log.info("Archived %d events so far", count)
    finally:
        for f in files.values():
            f.close()
        consumer.close()
        log.info("Archiver stopped (%d events)", count)


if __name__ == "__main__":
    main()
