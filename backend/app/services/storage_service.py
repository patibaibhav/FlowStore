"""Storage Explorer logic: reconciliation between S3 and SQLite, usage summary."""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

from app.core.config import Settings
from app.models.db import Database
from app.storage.s3 import S3Storage

KEY_RE = re.compile(r"^(?P<prefix>.+)/runs/(?P<run>[^/]+)/partitions/(?P<part>\d+)/chunks/(?P<chunk>[^/]+)\.parquet$")


def reconcile(db: Database, storage: S3Storage, run_id: str = "") -> dict[str, Any]:
    """Compare S3 listing with SQLite and repair recoverable inconsistencies.

    * Objects present in S3 but unknown to SQLite (e.g. upload OK, metadata write failed)
      are recorded (record count read from the Parquet footer via ranged GET).
    * Metadata rows whose object vanished from S3 are flagged MISSING.
    """
    listing = {o["key"]: o for o in storage.list_keys(run_id)}
    known = {o["object_key"]: o for o in db.query(
        "SELECT * FROM s3_objects" + (" WHERE run_id=?" if run_id else ""), [run_id] if run_id else [])}
    repaired, orphans, missing = [], [], []
    for key, meta in listing.items():
        if key in known:
            if known[key]["status"] != "PERSISTED":
                db.execute("UPDATE s3_objects SET status='PERSISTED' WHERE object_key=?", (key,))
            continue
        m = KEY_RE.match(key)
        if not m or not db.get_run(m["run"]):
            orphans.append(key)
            continue
        try:
            rows, codec = storage.parquet_num_rows(key)
        except Exception:
            orphans.append(key)
            continue
        now = time.time()
        chunk_id = m["chunk"]
        db.execute(
            "INSERT OR IGNORE INTO chunks(chunk_id,run_id,partition_id,record_count,status,seal_reason,created_at,"
            "parquet_bytes,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (chunk_id, m["run"], int(m["part"]), rows, "PERSISTED", "reconciled", meta["last_modified"], meta["size"], now))
        db.record_persisted(
            {"object_id": uuid.uuid5(uuid.NAMESPACE_URL, key).hex, "run_id": m["run"], "chunk_id": chunk_id,
             "partition_id": int(m["part"]), "bucket": storage.bucket, "object_key": key, "size_bytes": meta["size"],
             "record_count": rows, "compression": str(codec or "unknown").lower(), "uploaded_at": meta["last_modified"],
             "upload_ms": 0.0, "attempts": 1},
            {"parquet_bytes": meta["size"], "updated_at": now})
        repaired.append(key)
    for key, row in known.items():
        if key not in listing and row["status"] == "PERSISTED":
            db.execute("UPDATE s3_objects SET status='MISSING' WHERE object_key=?", (key,))
            missing.append(key)
    return {"s3_objects": len(listing), "metadata_objects": len(known), "repaired": repaired,
            "orphans": orphans, "missing_in_s3": missing}


def usage(db: Database, settings: Settings) -> dict[str, Any]:
    u = db.usage_summary()
    gb = u["bytes"] / (1024 ** 3)
    return {
        "runs": u["runs"], "generated_records": u["generated"], "s3_objects": u["objects"],
        "parquet_bytes": u["bytes"], "persisted_records": u["records"],
        "estimated_put_requests": u["objects"],
        "estimated_storage_gb": gb,
        "estimated_monthly_storage_cost_usd": gb * settings.price_s3_gb_month,
        "estimated_put_cost_usd": u["objects"] / 1000 * settings.price_s3_put_per_1000,
        "max_storage_mb": settings.s3_max_storage_mb,
        "cap_used_fraction": u["bytes"] / (settings.s3_max_storage_mb * 1024 * 1024) if settings.s3_max_storage_mb else 0,
        "disclaimer": "ESTIMATES ONLY, based on configurable pricing assumptions "
                      "(PRICE_S3_PUT_PER_1000, PRICE_S3_GB_MONTH). This is not your AWS billing balance.",
    }
