# Producer settings, read from .env (see .env.example).
import os

from dotenv import load_dotenv

load_dotenv(os.getenv("OPENSKY_ENV_FILE", ".env"))

TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network"
             "/protocol/openid-connect/token")
STATES_URL = "https://opensky-network.org/api/states/all"
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

# Native Kafka 3.9.1 KRaft quorum on node2-node4
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "node2:9092,node3:9092,node4:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "opensky.states.raw")
TOPIC_PARTITIONS = 6
TOPIC_REPLICATION = int(os.getenv("KAFKA_REPLICATION", "3"))
TOPIC_RETENTION_MS = 14 * 24 * 3600 * 1000

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "75"))

BBOX = {
    "lamin": float(os.getenv("LAMIN", "41.3")),
    "lomin": float(os.getenv("LOMIN", "-5.2")),
    "lamax": float(os.getenv("LAMAX", "51.1")),
    "lomax": float(os.getenv("LOMAX", "9.6")),
}

# Optional: write producer metrics to TimescaleDB pipeline_health.
# Example: host=localhost dbname=opensky user=spark_writer password=...
TSDB_DSN = os.getenv("TSDB_DSN", "")
