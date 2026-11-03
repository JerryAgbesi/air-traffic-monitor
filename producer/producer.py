# Main loop: fetch aircraft from OpenSky and send them to Kafka
import logging
import signal
import time

import requests

import config
from kafka_utils import ensure_topic, make_producer, send_messages
from opensky_client import OpenSkyClient
from transform import to_messages

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


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    if not config.CLIENT_ID:
        log.warning("No OPENSKY_CLIENT_ID set: running anonymously (low quota)")
    ensure_topic(config.BOOTSTRAP, config.TOPIC)
    producer = make_producer(config.BOOTSTRAP)
    client = OpenSkyClient(config.CLIENT_ID, config.CLIENT_SECRET)
    log.info("Producing to %s on %s every %s s",
             config.TOPIC, config.BOOTSTRAP, config.POLL_SECONDS)

    while running:
        started = time.time()
        wait_extra = 0
        try:
            payload, wait_extra = client.fetch_states()
            if payload:
                msgs = to_messages(payload)
                send_messages(producer, config.TOPIC, msgs)
                log.info("Snapshot %s: sent %d aircraft",
                         payload.get("time"), len(msgs))
        except requests.RequestException as e:
            log.error("HTTP error: %s", e)
            wait_extra = 30
        except Exception:
            log.exception("Unexpected error")
            wait_extra = 30

        elapsed = time.time() - started
        sleep_while_running(max(0, config.POLL_SECONDS - elapsed) + wait_extra)

    producer.flush(10)
    log.info("Producer stopped")


if __name__ == "__main__":
    main()
