"""REST API."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.schemas.run_config import PRESETS, RunConfig
from app.services import compare as compare_svc
from app.services import storage_service
from app.services.run_manager import ApiError, RunManager
from app.storage.s3 import StorageError, classify

router = APIRouter(prefix="/api")


def mgr(request: Request) -> RunManager:
    return request.app.state.manager


# ----------------------------------------------------------------- system
@router.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "ts": time.time()}


@router.get("/system/status")
def system_status(request: Request) -> dict[str, Any]:
    m = mgr(request)
    s = m.settings
    active = m.active()
    return {
        "state": active.state if active else "IDLE", "active_run_id": active.run_id if active else None,
        "s3": {"bucket": s.s3_bucket_name, "region": s.aws_region, "prefix": s.s3_prefix,
               "configured": bool(s.s3_bucket_name), "last_health": m.last_health},
        "limits": {"max_run_seconds": s.max_run_seconds, "max_rate_per_second": s.max_rate_per_second,
                   "max_records_per_run": s.max_records_per_run, "max_pending_uploads": s.max_pending_uploads_cap,
                   "s3_max_storage_mb": s.s3_max_storage_mb},
        "telemetry_hz": s.telemetry_hz,
    }


@router.get("/storage/health")
def storage_health(request: Request) -> dict[str, Any]:
    m = mgr(request)
    h = m.storage.health(write_probe=True)
    m.last_health = h
    return h


@router.get("/usage")
def usage(request: Request) -> dict[str, Any]:
    m = mgr(request)
    return storage_service.usage(m.db, m.settings)


# ---------------------------------------------------------------- presets
@router.get("/presets")
def presets() -> list[dict[str, Any]]:
    defaults = RunConfig().model_dump(mode="json")
    return [{**p, "config": {**defaults, **p["config"]}} for p in PRESETS]


@router.get("/config/defaults")
def config_defaults() -> dict[str, Any]:
    return RunConfig().model_dump(mode="json")


@router.post("/runs/estimate")
def estimate_run(cfg: RunConfig, request: Request) -> dict[str, Any]:
    return mgr(request).validate(cfg)


# ------------------------------------------------------------------- runs
@router.post("/runs", status_code=202)
def start_run(cfg: RunConfig, request: Request) -> dict[str, Any]:
    e = mgr(request).start(cfg)
    return {"run_id": e.run_id, "state": e.state}


@router.get("/runs")
def list_runs(request: Request, search: str = "", state: str = "", sort: str = "started_at",
              desc: bool = True, limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    return mgr(request).db.list_runs(search, state, sort, desc, limit)


@router.get("/runs/current")
def current_run(request: Request) -> dict[str, Any]:
    return mgr(request).status()


class StopBody(BaseModel):
    force: bool = False


class CompareBody(BaseModel):
    run_ids: list[str] = Field(min_length=2, max_length=4)


@router.post("/runs/compare")
def compare_runs(body: CompareBody, request: Request) -> dict[str, Any]:
    try:
        return compare_svc.compare(mgr(request).db, body.run_ids)
    except KeyError as e:
        raise HTTPException(404, f"run not found: {e.args[0]}")


@router.get("/runs/{run_id}")
def get_run(run_id: str, request: Request) -> dict[str, Any]:
    m = mgr(request)
    r = m.db.get_run(run_id)
    if not r:
        raise HTTPException(404, "run not found")
    e = m.engine
    if e and e.run_id == run_id and e.is_active():
        r["live"] = e.snapshot()
    r["chunks"] = m.db.chunks_for_run(run_id)
    return r


@router.post("/runs/{run_id}/pause")
def pause(run_id: str, request: Request) -> dict[str, Any]:
    return mgr(request).pause(run_id)


@router.post("/runs/{run_id}/resume")
def resume(run_id: str, request: Request) -> dict[str, Any]:
    return mgr(request).resume(run_id)


@router.post("/runs/{run_id}/stop")
def stop(run_id: str, request: Request, body: StopBody | None = None) -> dict[str, Any]:
    return mgr(request).stop(run_id, force=bool(body and body.force))


@router.get("/runs/{run_id}/metrics")
def run_metrics(run_id: str, request: Request) -> dict[str, Any]:
    m = mgr(request)
    if not m.db.get_run(run_id):
        raise HTTPException(404, "run not found")
    return {"run_id": run_id, "samples": m.db.metric_samples(run_id)}


@router.get("/runs/{run_id}/events")
def run_events(run_id: str, request: Request, component: str = "", severity: str = "", search: str = "",
               limit: int = Query(200, ge=1, le=1000)) -> list[dict[str, Any]]:
    return mgr(request).db.list_events(run_id, component, severity, search, limit)


@router.get("/events")
def all_events(request: Request, component: str = "", severity: str = "", search: str = "",
               limit: int = Query(200, ge=1, le=1000)) -> list[dict[str, Any]]:
    return mgr(request).db.list_events(None, component, severity, search, limit)


@router.get("/runs/{run_id}/rejected")
def run_rejected(run_id: str, request: Request, limit: int = Query(200, ge=1, le=500)) -> list[dict[str, Any]]:
    return mgr(request).db.list_rejected(run_id, limit)


# ---------------------------------------------------------------- storage
def _s3_error(e: Exception) -> HTTPException:
    code, retryable = classify(e)
    status = 503 if retryable else (404 if code in ("NoSuchKey", "404", "NoSuchBucket") else 502)
    return HTTPException(status, {"code": code, "message": "Amazon S3 request failed"})


@router.get("/storage/objects")
def list_objects(request: Request, run_id: str = "", partition: int | None = None, search: str = "",
                 limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)) -> dict[str, Any]:
    rows, total = mgr(request).db.list_objects(run_id, partition, search, limit, offset)
    return {"items": rows, "total": total, "limit": limit, "offset": offset,
            "source": "SQLite metadata written after confirmed S3 upload (use /storage/reconcile to verify against S3)"}


@router.post("/storage/reconcile")
def reconcile(request: Request, run_id: str = "") -> dict[str, Any]:
    m = mgr(request)
    if m.active():
        raise HTTPException(409, "Reconcile is disabled while a run is active")
    try:
        return storage_service.reconcile(m.db, m.storage, run_id)
    except Exception as e:
        raise _s3_error(e)


def _obj(m: RunManager, object_id: str) -> dict[str, Any]:
    o = m.db.get_object(object_id)
    if not o:
        raise HTTPException(404, "object not found")
    return o


@router.get("/storage/objects/{object_id}")
def object_detail(object_id: str, request: Request) -> dict[str, Any]:
    m = mgr(request)
    o = _obj(m, object_id)
    try:
        info = m.storage.inspect(o["object_key"], sample_rows=10)
    except Exception as e:
        raise _s3_error(e)
    return {**o, "inspection": info}


@router.get("/storage/objects/{object_id}/preview")
def object_preview(object_id: str, request: Request, rows: int = Query(25, ge=1, le=100)) -> dict[str, Any]:
    m = mgr(request)
    o = _obj(m, object_id)
    try:
        info = m.storage.inspect(o["object_key"], sample_rows=rows)
    except Exception as e:
        raise _s3_error(e)
    return {"object_id": object_id, "rows": info["sample"], "schema": info["schema"], "bytes_read": info["bytes_read"]}


@router.get("/storage/objects/{object_id}/download")
def object_download(object_id: str, request: Request) -> StreamingResponse:
    m = mgr(request)
    o = _obj(m, object_id)
    try:
        stream = m.storage.download_stream(o["object_key"])
    except Exception as e:
        raise _s3_error(e)
    name = o["object_key"].rsplit("/", 1)[-1]
    return StreamingResponse(stream, media_type="application/vnd.apache.parquet",
                             headers={"Content-Disposition": f'attachment; filename="{name}"',
                                      "Content-Length": str(o["size_bytes"])})


@router.delete("/storage/objects/{object_id}")
def object_delete(object_id: str, request: Request, confirm: bool = False,
                  x_admin_token: str | None = Header(default=None)) -> dict[str, Any]:
    m = mgr(request)
    if not confirm:
        raise HTTPException(400, "Deletion requires confirm=true")
    if m.settings.admin_token and x_admin_token != m.settings.admin_token:
        raise HTTPException(403, "Admin token required for destructive operations")
    o = _obj(m, object_id)
    active = m.active()
    if active and active.run_id == o["run_id"]:
        raise HTTPException(409, "Cannot delete objects of the active run")
    try:
        m.storage.delete(o["object_key"])
    except StorageError as e:
        raise HTTPException(403, str(e))
    except Exception as e:
        raise _s3_error(e)
    m.db.delete_object_row(object_id)
    m.elog.log(o["run_id"], "Amazon S3", "WARNING", f"Deleted object {o['object_key']}")
    return {"deleted": o["object_key"]}


@router.delete("/storage/runs/{run_id}")
def delete_run_objects(run_id: str, request: Request, confirm: bool = False,
                       x_admin_token: str | None = Header(default=None)) -> dict[str, Any]:
    m = mgr(request)
    if not confirm:
        raise HTTPException(400, "Deletion requires confirm=true")
    if m.settings.admin_token and x_admin_token != m.settings.admin_token:
        raise HTTPException(403, "Admin token required for destructive operations")
    active = m.active()
    if active and active.run_id == run_id:
        raise HTTPException(409, "Cannot delete objects of the active run")
    rows, _ = m.db.list_objects(run_id, limit=10_000)
    n = 0
    try:
        for o in rows:
            m.storage.delete(o["object_key"])
            m.db.delete_object_row(o["object_id"])
            n += 1
    except Exception as e:
        raise _s3_error(e)
    m.elog.log(run_id, "Amazon S3", "WARNING", f"Deleted {n} objects of run {run_id}")
    return {"deleted": n}


def api_error_handler(_: Request, exc: ApiError):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=exc.status, content={"code": exc.code, "message": exc.message, **exc.extra})
