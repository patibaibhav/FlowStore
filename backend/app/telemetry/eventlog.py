"""Structured event log: bounded in-memory ring + bounded SQLite persistence."""

from __future__ import annotations

import itertools
import threading
import time
from collections import deque
from typing import Any

from app.models.db import Database

COMPONENTS = ("Generator", "Queue", "Processor", "Validation", "Buffer", "Chunk Builder",
              "Parquet Writer", "Amazon S3", "Run Controller", "System")
SEVERITIES = ("INFO", "WARNING", "ERROR", "SUCCESS")


class EventLog:
    def __init__(self, db: Database | None, max_rows_per_run: int = 5000, ring: int = 500) -> None:
        self._db = db
        self._max_rows = max_rows_per_run
        self._ring: deque[dict[str, Any]] = deque(maxlen=ring)
        self._seq = itertools.count(1)
        self._lock = threading.Lock()

    def log(self, run_id: str | None, component: str, severity: str, message: str,
            details: dict[str, Any] | None = None) -> dict[str, Any]:
        entry = {"seq": 0, "ts": time.time(), "run_id": run_id, "component": component,
                 "severity": severity, "message": message, "details": details}
        with self._lock:
            entry["seq"] = next(self._seq)
            self._ring.append(entry)
        if self._db is not None:
            try:
                self._db.add_event(run_id, entry["ts"], component, severity, message, details, self._max_rows)
            except Exception:  # logging must never break the pipeline
                pass
        return entry

    def since(self, seq: int, run_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            items = [e for e in self._ring if e["seq"] > seq and (run_id is None or e["run_id"] == run_id)]
        return items[-limit:]

    def recent(self, n: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._ring)[-n:]

    @property
    def last_seq(self) -> int:
        with self._lock:
            return self._ring[-1]["seq"] if self._ring else 0
