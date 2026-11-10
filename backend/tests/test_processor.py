"""Lambda processor mode, CloudWatch publisher and their accounting. No AWS needed (stubs / moto)."""

import io
import json
import time

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from botocore.stub import Stubber
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.main import create_app
from app.pipeline import lambda_handler as lh
from app.pipeline.engine import RunEngine
from app.pipeline.events import EventGenerator
from app.pipeline.processor_backend import (
    InProcessLambdaClient, LambdaProcessor, LocalProcessor, ProcessorError, build_processor, encode_events,
)
from app.schemas.run_config import RunConfig, estimate
from app.services.run_manager import ApiError, RunManager
from app.storage.s3 import S3Storage
from app.telemetry.cloudwatch import CloudWatchPublisher


def mkset(tmp_path, **kw):
    base = dict(data_dir=tmp_path, s3_bucket_name="flowstore-test", aws_region="us-east-1", s3_max_retries=2,
                telemetry_hz=10, lambda_max_retries=2, _env_file=None)
    base.update(kw)
    return Settings(**base)


def events(n=600, invalid=0.4, seed=3):
    g = EventGenerator(seed, 30, 4, invalid)
    now = time.time()
    return [g.next_event(now) for _ in range(n)], now


def body(obj) -> StreamingBody:
    raw = json.dumps(obj).encode()
    return StreamingBody(io.BytesIO(raw), len(raw))


def lambda_stub(settings):
    client = boto3.client("lambda", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    return client, Stubber(client)


# ---------------------------------------------------------------- settings rules
def test_defaults_and_emulator_rules(tmp_path):
    s = mkset(tmp_path)
    assert RunConfig().processor_mode == "local"
    assert not s.lambda_available and not s.cloudwatch_active
    named = mkset(tmp_path, lambda_processor_name="fn")
    assert named.lambda_available
    emu = mkset(tmp_path, lambda_processor_name="fn", s3_endpoint_url="http://127.0.0.1:5555", cloudwatch_enabled=True)
    assert not emu.lambda_available and not emu.cloudwatch_active      # S3 emulator => in-process only
    assert mkset(tmp_path, s3_endpoint_url="http://x", lambda_emulate_local=True).lambda_available
    assert any("Lambda" in w for w in estimate(RunConfig(processor_mode="lambda"), 10**6, 10**5)["warnings"])


# ---------------------------------------------------------------- handler == local rules
def strict_loads(payload: str):
    """Parse like the real Lambda Invoke API: strict JSON, no NaN/Infinity tokens."""
    def bad(name):
        raise ValueError(name)
    return json.loads(payload, parse_constant=bad)


def test_handler_matches_local_validation_including_nan(tmp_path):
    batch, now = events()
    assert any(isinstance(e.get("sensor_value"), float) and e["sensor_value"] != e["sensor_value"] for e in batch)  # has NaN
    local = LocalProcessor().process(batch, 4, now)
    payload = json.dumps({"events": encode_events(batch), "num_partitions": 4, "now": now}, allow_nan=False)
    out = lh.lambda_handler(strict_loads(payload), None)             # strict JSON on the wire, NaN encoded
    assert out["rows"] == local.rows
    assert [(r["event_id"], r["reason"], r["field"]) for r in out["rejected"]] ==            [(r.event_id, r.reason, r.field) for r in local.rejected]
    assert any(r.reason == "sensor_value is NaN/Infinity" for r in local.rejected)
    assert out["count"] == len(batch) and out["code_hash"] == lh.CODE_HASH and out["protocol"] == lh.PROTOCOL


def test_bare_nan_is_rejected_like_real_lambda_and_encoding_avoids_it(tmp_path):
    batch, now = events()
    naive = json.dumps({"events": batch, "num_partitions": 4, "now": now}).encode()     # emits bare NaN
    assert b"NaN" in naive
    with pytest.raises(ClientError) as ei:
        InProcessLambdaClient().invoke(FunctionName="x", Payload=naive)
    assert ei.value.response["Error"]["Code"] == "InvalidRequestContentException"
    safe = LambdaProcessor(mkset(tmp_path, lambda_emulate_local=True)).process(batch, 4, now)
    assert safe.rejected and len(safe.rows) + len(safe.rejected) == len(batch)


def test_emulated_lambda_processor_equals_local_and_reports_timing(tmp_path):
    s = mkset(tmp_path, lambda_emulate_local=True)
    p = build_processor(RunConfig(processor_mode="lambda"), s)
    assert p.mode == "lambda" and p.emulated
    batch, now = events()
    res, ref = p.process(batch, 4, now), LocalProcessor().process(batch, 4, now)
    assert res.rows == ref.rows and [r.reason for r in res.rejected] == [r.reason for r in ref.rejected]
    st = p.stats()
    assert st["invocations"] == 1 and st["rtt_ms_avg"] > 0 and st["cold_starts"] in (0, 1)
    assert st["rtt_ms_avg"] >= st["handler_ms_avg"] and st["network_overhead_ms_avg"] >= 0


# ---------------------------------------------------------------- retry policy (botocore Stubber)
def test_lambda_throttle_retries_then_succeeds(tmp_path):
    s = mkset(tmp_path, lambda_processor_name="fn")
    client, stub = lambda_stub(s)
    batch, now = events(20, 0.0)
    ok = lh.lambda_handler({"events": batch, "num_partitions": 4, "now": now})
    stub.add_client_error("invoke", "TooManyRequestsException", "slow down", 429)
    stub.add_response("invoke", {"StatusCode": 200, "Payload": body(ok)})
    with stub:
        p = LambdaProcessor(s, client=client, sleeper=lambda _: None)
        res = p.process(batch, 4, now)
    assert len(res.rows) == 20 and res.attempts == 2
    st = p.stats()
    assert st["invocations"] == 2 and st["errors"] == 1 and st["retries"] == 1


def test_lambda_retry_exhaustion_and_fail_fast(tmp_path):
    s = mkset(tmp_path, lambda_processor_name="fn", lambda_max_retries=1)
    client, stub = lambda_stub(s)
    batch, now = events(5, 0.0)
    for _ in range(2):
        stub.add_client_error("invoke", "ServiceException", "boom", 500)
    with stub:
        p = LambdaProcessor(s, client=client, sleeper=lambda _: None)
        with pytest.raises(ProcessorError) as ei:
            p.process(batch, 4, now)
    assert ei.value.retryable and p.stats()["invocations"] == 2

    client, stub = lambda_stub(s)
    stub.add_client_error("invoke", "AccessDeniedException", "no", 403)
    with stub:
        p = LambdaProcessor(s, client=client, sleeper=lambda _: None)
        with pytest.raises(ProcessorError) as ei:
            p.process(batch, 4, now)
    assert not ei.value.retryable and p.stats()["invocations"] == 1      # no retries for non-retryable


def test_lambda_function_error_and_bad_response(tmp_path):
    s = mkset(tmp_path, lambda_processor_name="fn")
    batch, now = events(5, 0.0)
    client, stub = lambda_stub(s)
    stub.add_response("invoke", {"StatusCode": 200, "FunctionError": "Unhandled",
                                 "Payload": body({"errorMessage": "x"})})
    with stub:
        with pytest.raises(ProcessorError) as ei:
            LambdaProcessor(s, client=client, sleeper=lambda _: None).process(batch, 4, now)
    assert ei.value.code == "FunctionError" and not ei.value.retryable
    client, stub = lambda_stub(s)
    stub.add_response("invoke", {"StatusCode": 200, "Payload": body({"rows": [], "rejected": []})})
    with stub:
        with pytest.raises(ProcessorError) as ei:
            LambdaProcessor(s, client=client, sleeper=lambda _: None).process(batch, 4, now)
    assert ei.value.code == "BadResponse"


# ---------------------------------------------------------------- health gate
def test_health_prewarm_and_code_hash_warning(tmp_path):
    s = mkset(tmp_path, lambda_processor_name="fn")
    client, stub = lambda_stub(s)
    stub.add_response("invoke", {"StatusCode": 200, "Payload": body(
        {"rows": [], "rejected": [], "count": 0, "handler_ms": 1.0, "cold_start": True, "code_hash": "deadbeef",
         "protocol": lh.PROTOCOL})})
    with stub:
        h = LambdaProcessor(s, client=client).health()
    assert h["ready"] and h["prewarm"]["cold_start"] and not h["code_hash_match"] and h["warnings"]
    h2 = LambdaProcessor(mkset(tmp_path, lambda_emulate_local=True)).health()
    assert h2["ready"] and h2["code_hash_match"] and h2["emulated"] and not h2["warnings"]


def test_health_refuses_an_older_function_protocol(tmp_path):
    s = mkset(tmp_path, lambda_processor_name="fn")
    client, stub = lambda_stub(s)
    stub.add_response("invoke", {"StatusCode": 200, "Payload": body(
        {"rows": [], "rejected": [], "count": 0, "handler_ms": 1.0, "cold_start": False, "code_hash": lh.CODE_HASH})})
    with stub:
        h = LambdaProcessor(s, client=client).health()
    assert not h["ready"] and h["checks"][-1]["name"] == "function_version" and "build.py" in h["checks"][-1]["hint"]


def test_health_reports_unset_name_and_missing_function(tmp_path):
    h = LambdaProcessor(mkset(tmp_path)).health()
    assert not h["ready"] and h["checks"][0]["name"] == "configuration"
    s = mkset(tmp_path, lambda_processor_name="gone")
    client, stub = lambda_stub(s)
    stub.add_client_error("invoke", "ResourceNotFoundException", "nope", 404)
    with stub:
        h = LambdaProcessor(s, client=client).health()
    assert not h["ready"] and "ResourceNotFound" in h["checks"][-1]["detail"]


def test_run_refused_when_lambda_unavailable_or_unreachable_no_silent_fallback(settings, db, storage, elog):
    mgr = RunManager(settings, db, storage, elog)                      # no function configured
    with pytest.raises(ApiError) as ei:
        mgr.start(RunConfig(processor_mode="lambda", duration_seconds=1))
    assert ei.value.status == 424 and ei.value.code == "PROCESSOR_NOT_READY" and mgr.engine is None

    s = Settings(**{**settings.model_dump(), "lambda_processor_name": "gone"}, _env_file=None)
    client, stub = lambda_stub(s)
    stub.add_client_error("invoke", "ResourceNotFoundException", "nope", 404)
    with stub:
        mgr = RunManager(s, db, storage, elog, lambda_client=client)
        with pytest.raises(ApiError) as ei:
            mgr.start(RunConfig(processor_mode="lambda", duration_seconds=1))
    assert ei.value.status == 424 and mgr.engine is None             # refused, not run locally


def test_capabilities_drive_the_ui_toggle(tmp_path, db, storage, elog):
    s = mkset(tmp_path)
    caps = RunManager(s, db, storage, elog).capabilities()
    assert caps["processor"]["modes"] == ["local"] and caps["processor"]["unavailable_reason"]
    s2 = mkset(tmp_path, lambda_emulate_local=True)
    caps = RunManager(s2, db, storage, elog).capabilities()
    assert caps["processor"]["modes"] == ["local", "lambda"] and caps["processor"]["lambda_emulated"]


# ---------------------------------------------------------------- engine integration
def run_engine(settings, db, storage, elog, rid, processor=None, **kw):
    e = RunEngine(rid, RunConfig(**kw), settings, db, storage, elog, processor=processor)
    e.start()
    e.join(60)
    return e, e.snapshot()


def test_engine_lambda_emulated_balances_and_reports_lambda_numbers(tmp_path, db, storage, elog):
    s = mkset(tmp_path, lambda_emulate_local=True)
    _, snap = run_engine(s, db, storage, elog, "r-lam", processor_mode="lambda", rate_per_second=600,
                         duration_seconds=3, invalid_ratio=0.3, chunk_size_records=100)
    c, p = snap["counters"], snap["processor"]
    assert snap["state"] == "COMPLETED" and snap["accounting"]["balanced"]
    assert c["rejected_validation"] > 0 and c["persisted"] + c["rejected_validation"] == c["generated"]
    assert p["mode"] == "lambda" and p["emulated"] and p["lambda"]["invocations"] > 0
    assert p["batch_ms_avg"] > 0 and p["records_per_batch_avg"] > 0 and p["us_per_record"] > 0
    assert db.list_rejected("r-lam")
    run = db.get_run("r-lam")
    assert run["processor_rtt_ms"] > 0 and run["processor_batch_ms"] > 0 and run["config"]["processor_mode"] == "lambda"


def test_engine_local_mode_unchanged_and_has_batch_timing(tmp_path, db, storage, elog):
    s = mkset(tmp_path)
    _, snap = run_engine(s, db, storage, elog, "r-loc", rate_per_second=500, duration_seconds=2)
    p = snap["processor"]
    assert snap["accounting"]["balanced"] and p["mode"] == "local" and p["lambda"] is None
    assert p["batch_ms_avg"] > 0 and snap["counters"]["processor_failed"] == 0


class Boom:
    def invoke(self, **kw):
        raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no"},
                           "ResponseMetadata": {"HTTPStatusCode": 403}}, "Invoke")


def test_processor_failure_is_counted_never_persisted_and_fails_run(tmp_path, db, storage, elog):
    s = mkset(tmp_path, lambda_processor_name="fn")
    p = LambdaProcessor(s, client=Boom(), sleeper=lambda _: None)
    _, snap = run_engine(s, db, storage, elog, "r-boom", processor=p, processor_mode="lambda",
                         rate_per_second=400, duration_seconds=3)
    c = snap["counters"]
    assert snap["state"] == "FAILED" and snap["nodes"]["processor"] == "error"
    assert c["processor_failed"] > 0 and c["persisted"] == 0
    assert snap["accounting"]["balanced"] and "Lambda" in (db.get_run("r-boom")["note"] or "")


# ---------------------------------------------------------------- API
def test_api_status_and_refusal(settings, storage):
    with TestClient(create_app(settings, storage)) as c:
        st = c.get("/api/system/status").json()
        assert st["processor"]["modes"] == ["local"] and st["cloudwatch"]["enabled"] is False
        r = c.post("/api/runs", json={"processor_mode": "lambda", "duration_seconds": 1})
        assert r.status_code == 424 and r.json()["code"] == "PROCESSOR_NOT_READY"
        assert c.get("/api/runs/current").json()["state"] == "IDLE"


# ---------------------------------------------------------------- CloudWatch publisher
def wait_for(cond, timeout=5.0):
    t = time.time()
    while time.time() - t < timeout:
        if cond():
            return True
        time.sleep(0.02)
    return False


def snap(state="RUNNING", run_id="r1", lam=None):
    return {"run_id": run_id, "state": state,
            "rates": {"generation": 500, "processing": 480, "persist": 470, "rejected": 5, "dropped": 0},
            "queue": {"depth": 12, "blocked_seconds": 1.5}, "writer": {"recent_upload_ms": 40, "retries": 2},
            "processor": {"lambda": lam}}


def test_cloudwatch_publishes_at_most_ten_metrics_without_dimensions(tmp_path, aws, monkeypatch):
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    pub = CloudWatchPublisher(mkset(tmp_path, cloudwatch_enabled=True, cloudwatch_interval_seconds=1), client=cw)
    lam = {"invocations": 3, "rtt_ms_avg": 42.0}
    pub.maybe_publish(snap(lam=lam))
    assert wait_for(lambda: pub.published_calls == 1)
    metrics = cw.list_metrics(Namespace="FlowStore")["Metrics"]
    names = {m["MetricName"] for m in metrics}
    assert len(names) == 10 and "LambdaRttMs" in names and all(not m["Dimensions"] for m in metrics)
    pub.maybe_publish(snap())                       # inside the interval: skipped
    time.sleep(0.1)
    assert pub.published_calls == 1


def test_cloudwatch_local_mode_omits_lambda_metric_and_flushes_once_at_end(tmp_path, aws):
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    pub = CloudWatchPublisher(mkset(tmp_path, cloudwatch_enabled=True, cloudwatch_interval_seconds=60), client=cw)
    assert {m["MetricName"] for m in pub.build(snap())} == {
        "QueueDepth", "GeneratedPerSec", "ProcessedPerSec", "PersistedPerSec", "RejectedPerSec", "DroppedPerSec",
        "ProducerBlockedMs", "UploadLatencyMs", "UploadRetries"}
    pub.maybe_publish(snap("RUNNING"))
    assert wait_for(lambda: pub.published_calls == 1)
    pub.maybe_publish(snap("COMPLETED"))
    assert wait_for(lambda: pub.published_calls == 2)
    pub.maybe_publish(snap("COMPLETED"))            # final flush happens once
    time.sleep(0.1)
    assert pub.published_calls == 2 and pub.status()["errors"] == 0


def test_cloudwatch_errors_are_swallowed(tmp_path):
    class Down:
        def put_metric_data(self, **kw):
            raise RuntimeError("no network")

    pub = CloudWatchPublisher(mkset(tmp_path, cloudwatch_enabled=True), client=Down())
    pub.maybe_publish(snap())
    assert wait_for(lambda: pub.errors == 1) and "RuntimeError" in pub.last_error


def test_api_lambda_emulated_run_end_to_end(tmp_path, aws):
    s = mkset(tmp_path, lambda_emulate_local=True)
    with TestClient(create_app(s, S3Storage(s, client=aws, sleeper=lambda _: None))) as c:
        assert c.get("/api/system/status").json()["processor"]["modes"] == ["local", "lambda"]
        cfg = c.get("/api/presets").json()[0]["config"] | {"processor_mode": "lambda", "duration_seconds": 2,
                                                            "rate_per_second": 300, "chunk_size_records": 100}
        r = c.post("/api/runs", json=cfg)
        assert r.status_code == 202, r.text
        rid = r.json()["run_id"]
        run = None
        for _ in range(300):
            run = c.get(f"/api/runs/{rid}").json()
            if run["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.2)
        assert run["state"] == "COMPLETED" and run["persisted"] == run["generated"] > 0
        assert run["config"]["processor_mode"] == "lambda" and run["processor_rtt_ms"] > 0
        msgs = [e["message"] for e in c.get(f"/api/runs/{rid}/events").json()]
        assert any("Lambda processor ready" in m and "emulation" in m for m in msgs)
        cmp = c.post("/api/runs/compare", json={"run_ids": [rid, rid]}).json()
        assert cmp["runs"][0]["processor_mode"] == "lambda"
