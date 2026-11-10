"""Processor backends: validate a batch in-process ("local") or on AWS Lambda ("lambda").

Both apply the same rules (`events.validate`). The Lambda backend is a real network call:
its round-trip time is measured per call and reported, never estimated. There is no silent
fallback to the local processor: if Lambda fails, the batch fails (and is accounted for).
"""

from __future__ import annotations

import io
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

import boto3
from botocore.config import Config

from app.core.config import Settings
from app.pipeline.events import Rejection, validate
from app.pipeline.lambda_handler import CODE_HASH, lambda_handler
from app.schemas.run_config import RunConfig
from app.storage.s3 import backoff_delay, classify


class ProcessorError(Exception):
    def __init__(self, message: str, code: str = "ProcessorError", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass
class BatchResult:
    rows: list[dict[str, Any]]
    rejected: list[Rejection]
    rtt_ms: float = 0.0          # Lambda: full round trip of the successful call
    handler_ms: float = 0.0      # Lambda: time spent inside the function
    cold_start: bool = False
    attempts: int = 1


class ProcessorBackend(Protocol):
    mode: str
    emulated: bool
    max_batch: int

    def health(self) -> dict[str, Any]: ...
    def process(self, batch: list[dict[str, Any]], num_partitions: int, now: float,
                cancel: threading.Event | None = None) -> BatchResult: ...
    def stats(self) -> dict[str, Any]: ...


class LocalProcessor:
    mode = "local"
    emulated = False
    max_batch = 256

    def health(self) -> dict[str, Any]:
        return {"mode": "local", "ready": True, "checks": [], "warnings": [], "checked_at": time.time()}

    def process(self, batch: list[dict[str, Any]], num_partitions: int, now: float,
                cancel: threading.Event | None = None) -> BatchResult:
        rows: list[dict[str, Any]] = []
        rejected: list[Rejection] = []
        for ev in batch:
            row, rej = validate(ev, num_partitions, now)
            if rej is None:
                rows.append(row)  # type: ignore[arg-type]
            else:
                rejected.append(rej)
        return BatchResult(rows, rejected)

    def stats(self) -> dict[str, Any]:
        return {}


class InProcessLambdaClient:
    """DEV/TEST ONLY stand-in for boto3's Lambda client: runs the handler in this process (no network)."""

    def invoke(self, FunctionName: str, InvocationType: str = "RequestResponse", Payload: bytes = b"") -> dict[str, Any]:
        out = lambda_handler(json.loads(Payload), None)
        return {"StatusCode": 200, "Payload": io.BytesIO(json.dumps(out).encode())}


@dataclass
class _Stats:
    invocations: int = 0
    errors: int = 0
    retries: int = 0
    ok_calls: int = 0
    cold_starts: int = 0
    rtt_total: float = 0.0
    rtt_max: float = 0.0
    handler_total: float = 0.0
    last_error: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)


class LambdaProcessor:
    mode = "lambda"
    max_batch = 512

    def __init__(self, settings: Settings, client: Any | None = None, sleeper=time.sleep) -> None:
        self.settings = settings
        self.emulated = settings.lambda_emulate_local
        self.function = settings.lambda_processor_name or "flowstore-processor"
        self._client = client
        self._client_lock = threading.Lock()
        self._sleep = sleeper
        self._stats = _Stats()

    @property
    def client(self) -> Any:
        with self._client_lock:
            if self._client is None:
                if self.emulated:
                    self._client = InProcessLambdaClient()
                else:
                    cfg = Config(region_name=self.settings.aws_region,
                                 retries={"max_attempts": 1, "mode": "standard"},  # we own the retry policy
                                 connect_timeout=5, read_timeout=self.settings.lambda_timeout_seconds,
                                 max_pool_connections=10)
                    self._client = boto3.client("lambda", config=cfg)
            return self._client

    # --- one network call ------------------------------------------------
    def _invoke(self, payload: bytes) -> tuple[dict[str, Any], float]:
        t0 = time.perf_counter()
        resp = self.client.invoke(FunctionName=self.function, InvocationType="RequestResponse", Payload=payload)
        body = resp["Payload"].read()
        if resp.get("FunctionError"):
            raise ProcessorError(f"FunctionError: {body[:200]!r}", "FunctionError", False)
        data = json.loads(body)
        return data, (time.perf_counter() - t0) * 1000.0

    # --- health / pre-warm ------------------------------------------------
    def health(self) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        warnings: list[str] = []
        res: dict[str, Any] = {"mode": "lambda", "function": self.function, "emulated": self.emulated,
                               "checks": checks, "warnings": warnings, "ready": False, "checked_at": time.time()}
        if not self.settings.lambda_processor_name and not self.emulated:
            checks.append({"name": "configuration", "ok": False, "detail": "LAMBDA_PROCESSOR_NAME is not set",
                           "hint": "Set LAMBDA_PROCESSOR_NAME in the backend environment (.env) and restart."})
            return res
        checks.append({"name": "configuration", "ok": True,
                       "detail": "in-process emulation (development only, not real Lambda)" if self.emulated
                       else f"function '{self.function}'", "hint": None})
        payload = json.dumps({"events": [], "num_partitions": 1, "now": time.time(), "ping": True}).encode()
        try:
            data, rtt = self._invoke(payload)
        except Exception as e:
            code, _ = classify(e) if not isinstance(e, ProcessorError) else (e.code, False)
            hints = {"ResourceNotFoundException": "Function not found: check LAMBDA_PROCESSOR_NAME and AWS_REGION.",
                     "AccessDeniedException": "Allow lambda:InvokeFunction on this function for the instance role.",
                     "FunctionError": "The function crashed: check its CloudWatch logs."}
            checks.append({"name": "invoke", "ok": False, "detail": f"{code}: {str(e)[:160]}",
                           "hint": hints.get(code, "Check region, function name and IAM permissions.")})
            return res
        checks.append({"name": "invoke", "ok": True,
                       "detail": f"pre-warm invoke succeeded in {rtt:.0f} ms"
                                 + (" (cold start)" if data.get("cold_start") else ""), "hint": None})
        res["prewarm"] = {"rtt_ms": rtt, "handler_ms": data.get("handler_ms", 0.0),
                          "cold_start": bool(data.get("cold_start"))}
        res["code_hash"], res["backend_code_hash"] = data.get("code_hash"), CODE_HASH
        res["code_hash_match"] = data.get("code_hash") == CODE_HASH
        if not res["code_hash_match"]:
            warnings.append("The Lambda function's validation code differs from this backend's (events.py). "
                            "Rebuild and re-upload the function; otherwise local-vs-Lambda comparisons are not like for like.")
        res["ready"] = True
        self.reset_stats()   # pre-warm must not count towards the run's invocation/cold-start numbers
        return res

    def reset_stats(self) -> None:
        self._stats = _Stats()

    # --- one batch ---------------------------------------------------------
    def process(self, batch: list[dict[str, Any]], num_partitions: int, now: float,
                cancel: threading.Event | None = None) -> BatchResult:
        payload = json.dumps({"events": batch, "num_partitions": num_partitions, "now": now}).encode()
        st = self._stats
        retries = self.settings.lambda_max_retries
        attempt = 0
        while True:
            attempt += 1
            if cancel is not None and cancel.is_set():
                raise ProcessorError("batch cancelled", "Cancelled", False)
            with st.lock:
                st.invocations += 1
            try:
                data, rtt = self._invoke(payload)
                break
            except Exception as e:
                code, retryable = (e.code, False) if isinstance(e, ProcessorError) else classify(e)
                with st.lock:
                    st.errors += 1
                    st.last_error = f"{code}: {str(e)[:200]}"
                if not retryable or attempt > retries:
                    raise ProcessorError(f"{code}: {str(e)[:200]}", code, retryable) from e
                with st.lock:
                    st.retries += 1
                delay = backoff_delay(attempt)
                if cancel is not None:
                    if cancel.wait(delay):
                        raise ProcessorError("batch cancelled", "Cancelled", False) from e
                else:
                    self._sleep(delay)
        rows, rej_raw = data.get("rows", []), data.get("rejected", [])
        if len(rows) + len(rej_raw) != len(batch):
            with st.lock:
                st.errors += 1
                st.last_error = "BadResponse: row count does not match the batch"
            raise ProcessorError("BadResponse: Lambda returned a different number of records than it was sent",
                                 "BadResponse", False)
        rejected = [Rejection(r.get("event_id"), r.get("reason", "rejected"), r.get("field", "")) for r in rej_raw]
        handler_ms = float(data.get("handler_ms", 0.0))
        cold = bool(data.get("cold_start"))
        with st.lock:
            st.ok_calls += 1
            st.rtt_total += rtt
            st.rtt_max = max(st.rtt_max, rtt)
            st.handler_total += handler_ms
            st.cold_starts += int(cold)
        return BatchResult(rows, rejected, rtt_ms=rtt, handler_ms=handler_ms, cold_start=cold, attempts=attempt)

    def stats(self) -> dict[str, Any]:
        st = self._stats
        with st.lock:
            n = st.ok_calls
            rtt_avg = st.rtt_total / n if n else 0.0
            handler_avg = st.handler_total / n if n else 0.0
            return {"invocations": st.invocations, "errors": st.errors, "retries": st.retries,
                    "cold_starts": st.cold_starts, "rtt_ms_avg": rtt_avg, "rtt_ms_max": st.rtt_max,
                    "handler_ms_avg": handler_avg,
                    "network_overhead_ms_avg": max(0.0, rtt_avg - handler_avg) if n else 0.0,
                    "last_error": st.last_error}


def build_processor(cfg: RunConfig, settings: Settings, client: Any | None = None) -> ProcessorBackend:
    if cfg.processor_mode == "lambda":
        return LambdaProcessor(settings, client=client)
    return LocalProcessor()
