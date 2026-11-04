import time
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture
def client(settings, storage):
    app = create_app(settings, storage)
    with TestClient(app) as c:
        yield c


def wait_done(c, rid, timeout=60):
    t = time.time()
    while time.time() - t < timeout:
        r = c.get(f"/api/runs/{rid}").json()
        if r["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
            return r
        time.sleep(0.2)
    raise AssertionError("timeout")


def test_full_flow(client):
    assert client.get("/api/health").json()["status"] == "ok"
    h = client.get("/api/storage/health").json()
    assert h["ready"] and h["write_verified"]
    presets = client.get("/api/presets").json()
    assert len(presets) == 9
    cfg = presets[0]["config"] | {"duration_seconds": 3, "rate_per_second": 400, "chunk_size_records": 150}
    r = client.post("/api/runs", json=cfg)
    assert r.status_code == 202, r.text
    rid = r.json()["run_id"]
    assert client.post("/api/runs", json=cfg).status_code == 409   # single active run
    run = wait_done(client, rid)
    assert run["state"] == "COMPLETED"
    assert run["persisted"] == run["generated"] > 0
    objs = client.get("/api/storage/objects", params={"run_id": rid}).json()
    assert objs["total"] == run["files"] > 0
    oid = objs["items"][0]["object_id"]
    d = client.get(f"/api/storage/objects/{oid}").json()
    assert d["inspection"]["num_rows"] == objs["items"][0]["record_count"]
    assert [c["name"] for c in d["inspection"]["schema"]][:3] == ["event_id", "timestamp", "device_id"]
    assert len(client.get(f"/api/storage/objects/{oid}/preview?rows=5").json()["rows"]) == 5
    dl = client.get(f"/api/storage/objects/{oid}/download")
    assert dl.status_code == 200 and dl.content[:4] == b"PAR1"
    assert client.delete(f"/api/storage/objects/{oid}").status_code == 400      # needs confirm
    assert client.delete(f"/api/storage/objects/{oid}?confirm=true").status_code == 200
    assert client.get(f"/api/runs/{rid}/metrics").json()["samples"]
    assert client.get(f"/api/runs/{rid}/events").json()
    assert client.get("/api/usage").json()["s3_objects"] == run["files"] - 1


def test_invalid_config_rejected(client):
    assert client.post("/api/runs", json={"rate_per_second": 10**9}).status_code == 422
    assert client.post("/api/runs", json={"bogus": 1}).status_code == 422


def test_pause_resume_stop(client):
    cfg = client.get("/api/presets").json()[0]["config"] | {"duration_seconds": 30, "rate_per_second": 300}
    rid = client.post("/api/runs", json=cfg).json()["run_id"]
    time.sleep(1)
    assert client.post(f"/api/runs/{rid}/pause").json()["state"] in ("PAUSING", "PAUSED")
    time.sleep(1)
    assert client.get(f"/api/runs/{rid}").json()["state"] == "PAUSED"
    g1 = client.get("/api/runs/current").json()["counters"]["generated"]
    time.sleep(0.8)
    assert client.get("/api/runs/current").json()["counters"]["generated"] == g1  # truly paused
    assert client.post(f"/api/runs/{rid}/resume").json()["state"] == "RUNNING"
    assert client.post(f"/api/runs/{rid}/resume").status_code == 409
    client.post(f"/api/runs/{rid}/stop")
    run = wait_done(client, rid)
    assert run["state"] == "COMPLETED" and run["persisted"] == run["generated"]


def test_websocket(client):
    with client.websocket_connect("/ws/telemetry") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello" and hello["snapshot"]["state"] == "IDLE"


def test_start_refused_when_bucket_missing(settings, storage):
    from app.core.config import Settings
    from app.storage.s3 import S3Storage
    s2 = Settings(data_dir=settings.data_dir, s3_bucket_name="no-such-bucket", _env_file=None)
    app = create_app(s2, S3Storage(s2, client=storage.client))
    with TestClient(app) as c:
        r = c.post("/api/runs", json={})
        assert r.status_code == 424 and r.json()["code"] == "S3_NOT_READY"
        assert any(ch["name"] == "bucket_access" and not ch["ok"] for ch in r.json()["checks"])


def test_object_access_is_limited_to_known_objects(client):
    assert client.get("/api/storage/objects/does-not-exist").status_code == 404
    assert client.get("/api/storage/objects/does-not-exist/download").status_code == 404
    assert client.delete("/api/storage/objects/x?confirm=true").status_code == 404


def test_admin_token_protects_deletes(settings, storage):
    from app.core.config import Settings
    s2 = Settings(data_dir=settings.data_dir, s3_bucket_name=settings.s3_bucket_name, admin_token="secret", _env_file=None)
    with TestClient(create_app(s2, storage)) as c:
        assert c.delete("/api/storage/objects/x?confirm=true").status_code == 403
        assert c.delete("/api/storage/objects/x?confirm=true", headers={"X-Admin-Token": "secret"}).status_code == 404


def test_compare_endpoint(client):
    ids = []
    for i in range(2):
        cfg = client.get("/api/presets").json()[0]["config"] | {"duration_seconds": 2, "rate_per_second": 300, "chunk_size_records": 50 if i else 500}
        rid = client.post("/api/runs", json=cfg).json()["run_id"]
        wait_done(client, rid)
        ids.append(rid)
    res = client.post("/api/runs/compare", json={"run_ids": ids}).json()
    assert len(res["runs"]) == 2 and res["observations"]
    assert res["runs"][1]["files"] > res["runs"][0]["files"]
    assert client.post("/api/runs/compare", json={"run_ids": [ids[0], "nope"]}).status_code == 404
