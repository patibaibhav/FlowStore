"""FlowStore 2.0 ASGI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import api_error_handler, router
from app.core.config import Settings, get_settings
from app.models.db import Database
from app.services.run_manager import ApiError, RunManager
from app.storage.s3 import S3Storage
from app.telemetry.eventlog import EventLog
from app.telemetry.hub import TelemetryHub

log = logging.getLogger("flowstore")


def create_app(settings: Settings | None = None, storage: S3Storage | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        db = Database(settings.db_path)
        elog = EventLog(db, settings.max_event_log_rows_per_run)
        store = storage or S3Storage(settings)
        manager = RunManager(settings, db, store, elog)
        interrupted = manager.recover_on_startup()
        if interrupted:
            log.warning("marked interrupted runs: %s", interrupted)
        hub = TelemetryHub(manager, elog, settings.telemetry_hz)
        app.state.manager, app.state.hub = manager, hub
        hub.start()
        elog.log(None, "System", "INFO", "FlowStore backend started")
        try:
            yield
        finally:
            active = manager.active()
            if active:
                active.stop(force=True)
                active.join(10)
            await hub.stop()
            db.close()

    app = FastAPI(
        title="FlowStore 2.0 API",
        version="2.0.0",
        description="Time-series ingestion pipeline lab: Generator → Queue → Processor → Buffers → "
                    "Chunk Builder → Parquet Writer → Amazon S3.",
        lifespan=lifespan,
    )
    app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origin_list, allow_methods=["*"], allow_headers=["*"])
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router)

    @app.exception_handler(Exception)
    async def unhandled(_: Request, exc: Exception):  # never leak internals
        log.exception("unhandled error")
        return JSONResponse(status_code=500, content={"code": "INTERNAL_ERROR", "message": "Internal server error"})

    @app.websocket("/ws/telemetry")
    async def ws_telemetry(ws: WebSocket) -> None:
        hub: TelemetryHub = app.state.hub
        await hub.connect(ws)
        try:
            while True:
                await ws.receive_text()   # client pings keep the connection observable
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            hub.disconnect(ws)

    return app


app = create_app()
