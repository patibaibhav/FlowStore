"""Event schema, seeded generator and the validating processor logic.

Default domain: IoT environmental sensors (temperature, humidity, pressure, vibration).
"""

from __future__ import annotations

import json
import math
import random
import re
import time
import uuid
import zlib
from dataclasses import dataclass
from typing import Any

SENSOR_TYPES: dict[str, tuple[float, float, float, float]] = {
    # name: (mean, stddev, min_valid, max_valid)
    "temperature": (22.0, 4.0, -60.0, 150.0),
    "humidity": (55.0, 12.0, 0.0, 100.0),
    "pressure": (1013.0, 8.0, 300.0, 1100.0),
    "vibration": (0.4, 0.15, 0.0, 50.0),
}
SENSOR_NAMES = list(SENSOR_TYPES)
QUALITY_FLAGS = ("GOOD", "SUSPECT", "BAD")
DEVICE_RE = re.compile(r"^dev-\d{4,6}$")
TS_MAX_FUTURE_S = 300.0
TS_MAX_PAST_S = 86_400.0

INVALID_KINDS = ("bad_timestamp", "missing_timestamp", "bad_value", "nan_value", "bad_device", "bad_quality", "missing_sensor")


def partition_for(device_id: str, num_partitions: int) -> int:
    """Stable (process-independent) partition routing by device id."""
    return zlib.crc32(device_id.encode()) % num_partitions


class EventGenerator:
    """Deterministic (seeded) generator producing dict events, some deliberately invalid."""

    def __init__(self, seed: int, num_devices: int, num_partitions: int, invalid_ratio: float) -> None:
        self._rng = random.Random(seed)
        self._num_devices = num_devices
        self._num_partitions = num_partitions
        self._invalid_ratio = invalid_ratio
        self._offsets = [self._rng.uniform(-2, 2) for _ in range(num_devices)]
        self._seq = 0
        self._ns = uuid.UUID(int=random.Random(seed).getrandbits(128))

    def next_event(self, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        rng = self._rng
        self._seq += 1
        dev_idx = rng.randrange(self._num_devices)
        device_id = f"dev-{dev_idx + 1:04d}"
        stype = SENSOR_NAMES[rng.randrange(len(SENSOR_NAMES))]
        mean, sd, _, _ = SENSOR_TYPES[stype]
        value = rng.gauss(mean + self._offsets[dev_idx] * (sd / 4), sd)
        if stype in ("humidity",):
            value = min(100.0, max(0.0, value))
        elif stype == "vibration":
            value = abs(value)
        r = rng.random()
        quality = "GOOD" if r < 0.94 else ("SUSPECT" if r < 0.99 else "BAD")
        ev: dict[str, Any] = {
            "event_id": str(uuid.uuid5(self._ns, str(self._seq))),
            "timestamp": now,
            "device_id": device_id,
            "sensor_type": stype,
            "sensor_value": value,
            "partition_key": partition_for(device_id, self._num_partitions),
            "quality_flag": quality,
            "metadata": json.dumps({"fw": f"1.{dev_idx % 5}", "seq": self._seq}, separators=(",", ":")),
        }
        if self._invalid_ratio > 0 and rng.random() < self._invalid_ratio:
            self._corrupt(ev, INVALID_KINDS[rng.randrange(len(INVALID_KINDS))], now)
        return ev

    @staticmethod
    def _corrupt(ev: dict[str, Any], kind: str, now: float) -> None:
        if kind == "bad_timestamp":
            ev["timestamp"] = now + 10 * 365 * 86_400
        elif kind == "missing_timestamp":
            ev["timestamp"] = None
        elif kind == "bad_value":
            ev["sensor_value"] = "ERR"
        elif kind == "nan_value":
            ev["sensor_value"] = float("nan")
        elif kind == "bad_device":
            ev["device_id"] = "???"
        elif kind == "bad_quality":
            ev["quality_flag"] = "CORRUPT"
        elif kind == "missing_sensor":
            ev.pop("sensor_type", None)


@dataclass(slots=True)
class Rejection:
    event_id: str | None
    reason: str
    field: str


def validate(ev: dict[str, Any], num_partitions: int, now: float | None = None) -> tuple[dict[str, Any] | None, Rejection | None]:
    """Validate + lightly transform one event. Returns (row, None) or (None, rejection)."""
    now = time.time() if now is None else now
    eid = ev.get("event_id")
    if not isinstance(eid, str) or not eid:
        return None, Rejection(None, "missing or invalid event_id", "event_id")
    device = ev.get("device_id")
    if not isinstance(device, str) or not DEVICE_RE.match(device):
        return None, Rejection(eid, "invalid device identifier", "device_id")
    ts = ev.get("timestamp")
    if ts is None:
        return None, Rejection(eid, "missing timestamp", "timestamp")
    if isinstance(ts, bool) or not isinstance(ts, (int, float)) or not math.isfinite(ts):
        return None, Rejection(eid, "timestamp is not numeric", "timestamp")
    if ts > now + TS_MAX_FUTURE_S or ts < now - TS_MAX_PAST_S:
        return None, Rejection(eid, "timestamp out of acceptable range", "timestamp")
    stype = ev.get("sensor_type")
    if stype not in SENSOR_TYPES:
        return None, Rejection(eid, "unknown or missing sensor_type", "sensor_type")
    val = ev.get("sensor_value")
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return None, Rejection(eid, "sensor_value is not numeric", "sensor_value")
    if not math.isfinite(val):
        return None, Rejection(eid, "sensor_value is NaN/Infinity", "sensor_value")
    _, _, lo, hi = SENSOR_TYPES[stype]
    if not (lo <= val <= hi):
        return None, Rejection(eid, f"sensor_value outside {lo}..{hi}", "sensor_value")
    q = ev.get("quality_flag")
    if q not in QUALITY_FLAGS:
        return None, Rejection(eid, "invalid quality_flag", "quality_flag")
    meta = ev.get("metadata")
    # partition-key extraction + lightweight transformation (rounding)
    row = {
        "event_id": eid,
        "timestamp": float(ts),
        "device_id": device,
        "sensor_type": stype,
        "sensor_value": round(float(val), 4),
        "partition_key": partition_for(device, num_partitions),
        "quality_flag": q,
        "metadata": meta if isinstance(meta, str) else "{}",
        "ingested_at": now,
    }
    return row, None


def approx_row_bytes(row: dict[str, Any]) -> int:
    return 8 + 8 + len(row["event_id"]) + len(row["device_id"]) + len(row["sensor_type"]) + len(row["quality_flag"]) + len(row["metadata"]) + 8 + 4
