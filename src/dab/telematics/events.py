"""Fake car telematics events, used by the Kafka simulator."""

# json: turn a Python dict into JSON text.
import json

# random: type hint for the random number generator passed into generate_event().
import random

# UTC/datetime: build timezone-aware timestamps.
from datetime import UTC, datetime

# Rough bounding box around Houston, TX.
# Latitude range (south, north) that generated positions fall within.
LAT_RANGE = (29.5, 30.1)
# Longitude range (west, east); negative because Houston is west of Greenwich.
LON_RANGE = (-95.8, -95.0)
# Upper bound for the random speed, in km/h.
MAX_SPEED_KMH = 180.0

# Ten fake vehicle IDs: "CHS000001" ... "CHS000010" (zero-padded to 6 digits).
DEFAULT_CHASSIS_NUMBERS = [f"CHS{n:06d}" for n in range(1, 11)]


def generate_event(rng: random.Random, chassis_numbers: list[str], now: datetime) -> dict:
    """Return one telematics reading for a random car."""
    # `rng` and `now` are passed in rather than created here so tests can make the output deterministic.
    return {
        # Pick one car at random; this also becomes the Kafka message key in simulator.py.
        "chassis_number": rng.choice(chassis_numbers),
        # Random speed between 0 and MAX_SPEED_KMH, rounded to 1 decimal place.
        "speed": round(rng.uniform(0, MAX_SPEED_KMH), 1),
        # Random latitude inside the bounding box, 6 decimals (~0.1 m precision).
        "latitude": round(rng.uniform(*LAT_RANGE), 6),
        # Random longitude inside the bounding box, 6 decimals.
        "longitude": round(rng.uniform(*LON_RANGE), 6),
        # When the reading happened, converted to UTC and written as ISO-8601,
        # e.g. "2026-10-21T17:05:12.345678+00:00".
        "event_timestamp": now.astimezone(UTC).isoformat(),
    }


def to_json(event: dict) -> bytes:
    """Serialize an event as the Kafka message value."""
    # dict -> JSON string -> UTF-8 bytes. Kafka stores keys and values as raw bytes.
    return json.dumps(event).encode("utf-8")
