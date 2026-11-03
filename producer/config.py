# Loads settings from .env
import os

from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = ("https://auth.opensky-network.org/auth/realms/opensky-network"
             "/protocol/openid-connect/token")
STATES_URL = "https://opensky-network.org/api/states/all"
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "opensky.states.raw")
TOPIC_PARTITIONS = 6
TOPIC_RETENTION_MS = 14 * 24 * 3600 * 1000

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "75"))

BBOX = {
    "lamin": float(os.getenv("LAMIN", "41.3")),
    "lomin": float(os.getenv("LOMIN", "-5.2")),
    "lamax": float(os.getenv("LAMAX", "51.1")),
    "lomax": float(os.getenv("LOMAX", "9.6")),
}

SCHEMA_VERSION = 1
