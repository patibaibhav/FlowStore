"""AWS Lambda handler for the optional "lambda" processor mode.

This file is the single source for the function: `lambda/build.py` packages it, together with
`events.py`, into the deployment zip (handler setting: `lambda_handler.lambda_handler`).
It runs the same `validate()` the in-process processor uses, so both modes apply identical rules.
Pure computation only: no AWS calls, no side effects, safe to retry.
"""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path
from typing import Any

try:  # normal backend import
    from app.pipeline import events as _events
    from app.pipeline.events import validate
except ImportError:  # inside the Lambda zip: events.py sits next to this file
    import events as _events  # type: ignore[no-redef]
    from events import validate  # type: ignore[no-redef]


def _code_hash() -> str:
    """Fingerprint of the validation rules (events.py), line-ending independent."""
    data = Path(_events.__file__).read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()[:16]


CODE_HASH = _code_hash()
_cold = True


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    global _cold
    t0 = time.perf_counter()
    cold, _cold = _cold, False
    out: dict[str, Any] = {"rows": [], "rejected": [], "cold_start": cold, "code_hash": CODE_HASH,
                           "python": sys.version.split()[0]}
    batch = event.get("events") or []
    num_partitions = int(event.get("num_partitions", 4))
    now = event.get("now")
    for ev in batch:
        row, rej = validate(ev, num_partitions, now)
        if rej is None:
            out["rows"].append(row)
        else:
            out["rejected"].append({"event_id": rej.event_id, "reason": rej.reason, "field": rej.field})
    out["count"] = len(batch)
    out["handler_ms"] = (time.perf_counter() - t0) * 1000.0
    return out
