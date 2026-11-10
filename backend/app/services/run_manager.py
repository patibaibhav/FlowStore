"""Single-active-run controller: validation, health gate, lifecycle, startup recovery."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from app.core.config import Settings
from app.models.db import Database
from app.pipeline.engine import ACTIVE_STATES, InvalidTransition, RunEngine
from app.pipeline.parquet_writer import cleanup_staging
from app.pipeline.processor_backend import build_processor
from app.schemas.run_config import RunConfig, estimate
from app.storage.s3 import S3Storage
from app.telemetry.cloudwatch import CloudWatchPublisher
from app.telemetry.eventlog import EventLog


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, extra: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.extra = status, code, message, extra or {}


class RunManager:
    def __init__(self, settings: Settings, db: Database, storage: S3Storage, elog: EventLog,
                 lambda_client: Any | None = None, cloudwatch_client: Any | None = None) -> None:
        self.settings, self.db, self.storage, self.elog = settings, db, storage, elog
        self._lock = threading.Lock()
        self.engine: RunEngine | None = None
        self._latest: dict[str, Any] | None = None
        self._snap_lock = threading.Lock()
        self.last_health: dict[str, Any] | None = None
        self.last_processor_health: dict[str, Any] | None = None
        self._lambda_client = lambda_client
        self.cloudwatch = CloudWatchPublisher(settings, cloudwatch_client) if settings.cloudwatch_active else None

    def capabilities(self) -> dict[str, Any]:
        """What this server offers (drives the UI toggle). Makes no AWS calls."""
        s = self.settings
        reason = None
        if not s.lambda_available:
            reason = ("Lambda is not available with the S3 emulator (no AWS)." if s.using_s3_emulator
                      else "LAMBDA_PROCESSOR_NAME is not set on the server.")
        return {
            "processor": {"modes": ["local", "lambda"] if s.lambda_available else ["local"],
                          "lambda_available": s.lambda_available, "lambda_emulated": s.lambda_emulate_local,
                          "lambda_function": s.lambda_processor_name or None, "unavailable_reason": reason,
                          "last_health": self.last_processor_health},
            "cloudwatch": self.cloudwatch.status() if self.cloudwatch else
                          {"enabled": False, "namespace": s.cloudwatch_namespace},
        }

    # --- lifecycle -----------------------------------------------------
    def recover_on_startup(self) -> list[str]:
        ids = self.db.mark_interrupted(time.time())
        for rid in ids:
            self.elog.log(rid, "System", "WARNING", "Run was interrupted by a backend restart; marked INTERRUPTED")
        removed = cleanup_staging(self.settings.staging_dir, max_age_s=0)
        # remove now-empty run staging dirs
        if self.settings.staging_dir.exists():
            for d in self.settings.staging_dir.iterdir():
                if d.is_dir():
                    for f in d.glob("*"):
                        f.unlink(missing_ok=True)
                    try:
                        d.rmdir()
                    except OSError:
                        pass
        if removed:
            self.elog.log(None, "System", "INFO", f"Cleaned {removed} abandoned staging files")
        self.db.prune_old_runs(self.settings.max_stored_runs)
        return ids

    def _on_snapshot(self, snap: dict[str, Any]) -> None:
        with self._snap_lock:
            self._latest = snap

    def latest(self) -> dict[str, Any] | None:
        with self._snap_lock:
            return self._latest

    def active(self) -> RunEngine | None:
        e = self.engine
        return e if e and e.state in ACTIVE_STATES else None

    def validate(self, cfg: RunConfig) -> dict[str, Any]:
        est = estimate(cfg, self.settings.max_records_per_run, self.settings.max_rate_per_second)
        cap_errors = []
        if cfg.planned_records > self.settings.max_records_per_run:
            cap_errors.append(f"planned records {cfg.planned_records:,} exceed cap {self.settings.max_records_per_run:,}")
        if cfg.rate_per_second > self.settings.max_rate_per_second:
            cap_errors.append(f"rate exceeds cap {self.settings.max_rate_per_second:,}/s")
        if cfg.duration_seconds > self.settings.max_run_seconds:
            cap_errors.append(f"duration exceeds cap {self.settings.max_run_seconds}s")
        if cfg.max_pending_uploads > self.settings.max_pending_uploads_cap:
            cap_errors.append(f"max_pending_uploads exceeds cap {self.settings.max_pending_uploads_cap}")
        if cfg.upload_concurrency > self.settings.s3_upload_concurrency * 4:
            cap_errors.append(f"upload_concurrency exceeds {self.settings.s3_upload_concurrency * 4}")
        if cfg.chunk_size_records / max(1, cfg.num_partitions) < 1:
            cap_errors.append("chunk size too small for partition count")
        return {"valid": not cap_errors, "errors": cap_errors, **est}

    def start(self, cfg: RunConfig) -> RunEngine:
        v = self.validate(cfg)
        if not v["valid"]:
            raise ApiError(422, "CONFIG_EXCEEDS_CAPS", "; ".join(v["errors"]))
        with self._lock:
            if self.active():
                raise ApiError(409, "RUN_ACTIVE", "A run is already active. Stop it before starting another.")
            health = self.storage.health(write_probe=True)
            self.last_health = health
            if not (health["ready"] and health["write_verified"]):
                failing = [c for c in health["checks"] if not c["ok"]]
                raise ApiError(424, "S3_NOT_READY",
                               failing[0]["detail"] if failing else "S3 is not ready",
                               {"checks": health["checks"]})
            processor, p_health = None, None
            if cfg.processor_mode == "lambda":
                if not self.settings.lambda_available:
                    caps = self.capabilities()["processor"]
                    raise ApiError(424, "PROCESSOR_NOT_READY", caps["unavailable_reason"] or "Lambda is not available",
                                   {"checks": [{"name": "configuration", "ok": False,
                                                "detail": caps["unavailable_reason"], "hint": None}]})
                processor = build_processor(cfg, self.settings, client=self._lambda_client)
                p_health = processor.health()   # real pre-warm invoke; no silent fallback to local
                self.last_processor_health = p_health
                if not p_health["ready"]:
                    failing = [c for c in p_health["checks"] if not c["ok"]]
                    raise ApiError(424, "PROCESSOR_NOT_READY",
                                   failing[0]["detail"] if failing else "Lambda processor is not ready",
                                   {"checks": p_health["checks"]})
            run_id = f"run-{int(time.time() * 1000)}-{uuid.uuid4().hex[:4]}"
            engine = RunEngine(run_id, cfg, self.settings, self.db, self.storage, self.elog, self._on_snapshot,
                               processor=processor, cloudwatch=self.cloudwatch)
            self.engine = engine
            self._latest = None
            if p_health:
                pw = p_health.get("prewarm", {})
                self.elog.log(run_id, "Processor", "INFO",
                              f"Lambda processor ready ({'in-process emulation, not real Lambda' if p_health['emulated'] else p_health['function']}); "
                              f"pre-warm {pw.get('rtt_ms', 0):.0f} ms" + (" (cold start)" if pw.get("cold_start") else ""),
                              {"prewarm": pw})
                for w in p_health["warnings"]:
                    self.elog.log(run_id, "Processor", "WARNING", w)
            engine.start()
            return engine

    def _get(self, run_id: str) -> RunEngine:
        e = self.engine
        if not e or e.run_id != run_id:
            raise ApiError(404, "RUN_NOT_ACTIVE", "No such active run")
        return e

    def pause(self, run_id: str) -> dict[str, Any]:
        return self._call(run_id, lambda e: e.pause())

    def resume(self, run_id: str) -> dict[str, Any]:
        return self._call(run_id, lambda e: e.resume())

    def stop(self, run_id: str, force: bool = False) -> dict[str, Any]:
        return self._call(run_id, lambda e: e.stop(force))

    def _call(self, run_id: str, fn) -> dict[str, Any]:
        e = self._get(run_id)
        try:
            fn(e)
        except InvalidTransition as ex:
            raise ApiError(409, "INVALID_TRANSITION", str(ex))
        return {"run_id": run_id, "state": e.state}

    def status(self) -> dict[str, Any]:
        e = self.engine
        if e is None:
            return {"state": "IDLE", "run_id": None}
        snap = self.latest() or e.snapshot()
        return snap
