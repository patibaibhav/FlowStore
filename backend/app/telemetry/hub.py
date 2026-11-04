"""WebSocket telemetry hub: bounded-rate broadcasts, hello snapshot, heartbeats, safe disconnects."""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

from fastapi import WebSocket

from app.services.run_manager import RunManager
from app.telemetry.eventlog import EventLog


class TelemetryHub:
    def __init__(self, manager: RunManager, elog: EventLog, hz: float) -> None:
        self.manager, self.elog, self.period = manager, elog, 1.0 / hz
        self._clients: dict[WebSocket, int] = {}   # ws -> last event seq delivered
        self._seq = 0
        self._task: asyncio.Task | None = None
        self._last_sent_ts = 0.0

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        for ws in list(self._clients):
            with contextlib.suppress(Exception):
                await ws.close()
        self._clients.clear()

    def _snapshot(self) -> dict[str, Any]:
        return self.manager.status()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._seq += 1
        last = self.elog.last_seq
        self._clients[ws] = last
        await ws.send_json({"type": "hello", "seq": self._seq, "ts": time.time(), "hz": 1 / self.period,
                            "snapshot": self._snapshot(), "events": self.elog.recent(60)})

    def disconnect(self, ws: WebSocket) -> None:
        self._clients.pop(ws, None)

    async def _send(self, ws: WebSocket, msg: dict[str, Any]) -> bool:
        try:
            await asyncio.wait_for(ws.send_json(msg), timeout=2.0)
            return True
        except Exception:
            self._clients.pop(ws, None)   # slow or gone: drop; the client reconnects
            return False

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.period)
            if not self._clients:
                continue
            snap = self._snapshot()
            now = time.time()
            self._seq += 1
            active = snap.get("state") not in ("IDLE", None) and snap.get("ts", 0) > now - 3600
            idle_quiet = (not self.manager.active()) and (now - self._last_sent_ts < 5.0) and \
                all(self.elog.last_seq <= s for s in self._clients.values())
            if idle_quiet:
                continue
            self._last_sent_ts = now
            for ws, last in list(self._clients.items()):
                events = self.elog.since(last)
                msg = {"type": "telemetry" if active else "heartbeat", "seq": self._seq, "ts": now,
                       "snapshot": snap, "events": events}
                if await self._send(ws, msg) and events:
                    self._clients[ws] = events[-1]["seq"]
