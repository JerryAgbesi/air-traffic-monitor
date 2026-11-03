# Converts OpenSky data into Kafka messages
from datetime import datetime, timezone

from config import SCHEMA_VERSION

STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "lon", "lat", "baro_altitude_m", "on_ground", "velocity_ms",
    "true_track_deg", "vertical_rate_ms", "sensors", "geo_altitude_m",
    "squawk", "spi", "position_source",
]


def to_messages(payload):
    snapshot_time = payload.get("time")
    ingest_time = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    out = []
    for state in payload.get("states") or []:
        rec = dict(zip(STATE_FIELDS, state))
        event_time = rec["time_position"] or rec["last_contact"]
        if not rec["icao24"] or event_time is None:
            continue
        value = {
            "schema_version": SCHEMA_VERSION,
            "icao24": rec["icao24"],
            "callsign": (rec["callsign"] or "").strip() or None,
            "origin_country": rec["origin_country"],
            "event_time": int(event_time),
            "snapshot_time": snapshot_time,
            "ingest_time": ingest_time,
            "lon": rec["lon"],
            "lat": rec["lat"],
            "baro_altitude_m": rec["baro_altitude_m"],
            "geo_altitude_m": rec["geo_altitude_m"],
            "on_ground": rec["on_ground"],
            "velocity_ms": rec["velocity_ms"],
            "true_track_deg": rec["true_track_deg"],
            "vertical_rate_ms": rec["vertical_rate_ms"],
            "squawk": rec["squawk"],
            "position_source": rec["position_source"],
        }
        out.append((rec["icao24"], value))
    return out
