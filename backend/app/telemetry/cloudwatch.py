"""Optional best-effort publisher of application metrics to Amazon CloudWatch.

* Off unless CLOUDWATCH_ENABLED=true, and never used with the S3 emulator.
* At most 10 custom metrics, NO dimensions: every extra dimension combination is a separate
  billable custom metric (per-run or per-preset dimensions would multiply the cost).
* One batched PutMetricData call every CLOUDWATCH_INTERVAL_SECONDS while a run is active,
  plus a final flush when the run ends. Runs in a short-lived thread; errors are swallowed.
* Values come from the engine's measured snapshot; nothing is estimated here.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import boto3
from botocore.config import Config

from app.core.config import Settings

log = logging.getLogger("flowstore.cloudwatch")

ACTIVE = {"STARTING", "RUNNING", "PAUSING", "PAUSED", "STOPPING", "FLUSHING"}
TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"}


class CloudWatchPublisher:
    def __init__(self, settings: Settings, client: Any | None = None) -> None:
        self.namespace = settings.cloudwatch_namespace
        self.interval = settings.cloudwatch_interval_seconds
        self._settings = settings
        self._client = client
        self._lock = threading.Lock()
        self._inflight = False
        self._last_sent = 0.0
        self._prev_blocked_s = 0.0
        self._prev_retries = 0
        self._final_sent = False
        self._run_id: str | None = None
        self.published_calls = 0
        self.errors = 0
        self.last_error: str | None = None

    @property
    def client(self) -> Any:
        if self._client is None:
            cfg = Config(region_name=self._settings.aws_region, retries={"max_attempts": 1, "mode": "standard"},
                         connect_timeout=3, read_timeout=5)
            self._client = boto3.client("cloudwatch", config=cfg)
        return self._client

    def build(self, snap: dict[str, Any]) -> list[dict[str, Any]]:
        """Translate one measured snapshot into at most 10 metric data points (no dimensions)."""
        rates, q, w = snap["rates"], snap["queue"], snap["writer"]
        blocked_s = float(q.get("blocked_seconds", 0.0))
        retries = int(w.get("retries", 0))
        data = [
            ("QueueDepth", float(q["depth"]), "Count"),
            ("GeneratedPerSec", rates["generation"], "Count/Second"),
            ("ProcessedPerSec", rates["processing"], "Count/Second"),
            ("PersistedPerSec", rates["persist"], "Count/Second"),
            ("RejectedPerSec", rates["rejected"], "Count/Second"),
            ("DroppedPerSec", rates["dropped"], "Count/Second"),
            ("ProducerBlockedMs", max(0.0, blocked_s - self._prev_blocked_s) * 1000.0, "Milliseconds"),
            ("UploadLatencyMs", float(w.get("recent_upload_ms", 0.0)), "Milliseconds"),
            ("UploadRetries", float(max(0, retries - self._prev_retries)), "Count"),
        ]
        lam = (snap.get("processor") or {}).get("lambda")
        if lam and lam.get("invocations"):
            data.append(("LambdaRttMs", float(lam["rtt_ms_avg"]), "Milliseconds"))
        self._prev_blocked_s, self._prev_retries = blocked_s, retries
        return [{"MetricName": n, "Value": float(v), "Unit": u} for n, v, u in data]

    def maybe_publish(self, snap: dict[str, Any]) -> None:
        state = snap.get("state")
        now = time.monotonic()
        with self._lock:
            if self._inflight:
                return
            if snap.get("run_id") != self._run_id:   # new run: cumulative counters restart at zero
                self._run_id = snap.get("run_id")
                self._prev_blocked_s, self._prev_retries = 0.0, 0
            if state in ACTIVE:
                if now - self._last_sent < self.interval:
                    return
                self._final_sent = False
            elif state in TERMINAL:
                if self._final_sent:
                    return
                self._final_sent = True
            else:
                return
            self._inflight = True
            self._last_sent = now
            metrics = self.build(snap)
        threading.Thread(target=self._send, args=(metrics,), name="cloudwatch-put", daemon=True).start()

    def _send(self, metrics: list[dict[str, Any]]) -> None:
        try:
            self.client.put_metric_data(Namespace=self.namespace, MetricData=metrics)
            self.published_calls += 1
        except Exception as e:  # best effort: never affect a run
            self.errors += 1
            self.last_error = f"{type(e).__name__}: {str(e)[:160]}"
            log.warning("CloudWatch publish failed: %s", self.last_error)
        finally:
            with self._lock:
                self._inflight = False

    def status(self) -> dict[str, Any]:
        return {"enabled": True, "namespace": self.namespace, "interval_seconds": self.interval,
                "published_calls": self.published_calls, "errors": self.errors, "last_error": self.last_error}
