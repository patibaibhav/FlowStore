import threading
import time

import boto3
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from app.core.config import Settings
from app.pipeline import parquet_writer as pw
from app.pipeline.bounded_queue import BoundedQueue
from app.pipeline.engine import TRANSITIONS, InvalidTransition, RunEngine
from app.pipeline.events import EventGenerator, partition_for, validate
from app.schemas.run_config import OverflowPolicy, RunConfig
from app.storage.s3 import S3Storage, StorageError, classify


# ---- generator & validation
def test_generator_is_reproducible():
    a = EventGenerator(7, 10, 4, 0.0).next_event(1000.0)
    b = EventGenerator(7, 10, 4, 0.0).next_event(1000.0)
    assert a == b


def test_invalid_ratio_produces_rejections_with_reasons():
    g = EventGenerator(1, 20, 4, 0.5)
    res = [validate(g.next_event(), 4) for _ in range(2000)]
    rej = [r for _, r in res if r]
    assert 800 < len(rej) < 1200
    assert all(r.reason and r.field for r in rej)
    assert all(row is None for row, r in res if r)


def test_valid_events_pass_and_route_to_stable_partition():
    ev = EventGenerator(3, 5, 8, 0.0).next_event()
    row, rej = validate(ev, 8)
    assert rej is None and row["partition_key"] == partition_for(ev["device_id"], 8)


# ---- queue policies
def test_queue_drop_newest():
    q = BoundedQueue(10, OverflowPolicy.DROP_NEWEST)
    assert q.put_many(list(range(25)), threading.Event()) == (10, 15, 0)
    m = q.metrics()
    assert m["depth"] == 10 and m["dropped_newest"] == 15 and m["max_depth"] == 10


def test_queue_drop_oldest_keeps_newest_and_counts_evictions():
    q = BoundedQueue(5, OverflowPolicy.DROP_OLDEST)
    adm, dropped, _ = q.put_many(list(range(12)), threading.Event())
    assert adm == 12 and dropped == 0
    assert q.get_batch(10) == [7, 8, 9, 10, 11]
    assert q.metrics()["evicted_oldest"] == 7


def test_queue_block_applies_backpressure_and_measures_it():
    q = BoundedQueue(5, OverflowPolicy.BLOCK)
    res = []
    t = threading.Thread(target=lambda: res.append(q.put_many(list(range(8)), threading.Event())))
    t.start()
    time.sleep(0.3)
    assert q.metrics()["blocked_now"] and q.depth() == 5 and t.is_alive()
    q.get_batch(3)
    t.join(2)
    assert res[0] == (8, 0, 0)
    assert q.metrics()["blocked_seconds"] > 0.2


def test_queue_block_abort_abandons():
    q = BoundedQueue(2, OverflowPolicy.BLOCK)
    abort = threading.Event()
    threading.Timer(0.2, abort.set).start()
    assert q.put_many(list(range(5)), abort) == (2, 0, 3)


# ---- buffers / chunks / parquet
def test_buffer_thresholds_and_flush():
    b = pw.PartitionBuffers("r", 2, max_records=5, max_bytes=10**9, max_age=0.2)
    g = EventGenerator(1, 10, 2, 0)
    rows = [validate(g.next_event(), 2)[0] for _ in range(23)]
    sealed = b.add_many(rows)
    assert all(c.record_count == 5 and c.seal_reason == "size" for c in sealed)
    assert b.stats()["records"] + 5 * len(sealed) == 23
    time.sleep(0.3)
    aged = b.seal_aged()
    assert aged and all(c.seal_reason == "age" for c in aged)
    assert b.stats()["records"] == 0


def test_parquet_schema_and_roundtrip(tmp_path):
    g = EventGenerator(1, 10, 2, 0)
    rows = [validate(g.next_event(), 2)[0] for _ in range(300)]
    for codec in ("snappy", "zstd", "gzip", "none"):
        ch = pw.Chunk("c-" + codec, "r", 0, rows, 300, time.time(), 1, "size")
        res = pw.write_parquet(ch, tmp_path, codec)
        t = pq.read_table(res.path)
        assert t.schema.equals(pw.SCHEMA) and t.num_rows == 300
        assert res.size_bytes > 0 and not (tmp_path / f"c-{codec}.parquet.tmp").exists()
    assert (tmp_path / "c-snappy.parquet").stat().st_size < (tmp_path / "c-none.parquet").stat().st_size


# ---- S3 error handling
def client_error(code, status):
    return ClientError({"Error": {"Code": code, "Message": "x"}, "ResponseMetadata": {"HTTPStatusCode": status}}, "PutObject")


def test_classification():
    assert classify(client_error("AccessDenied", 403)) == ("AccessDenied", False)
    assert classify(client_error("SlowDown", 503)) == ("SlowDown", True)
    assert classify(client_error("NoSuchBucket", 404)) == ("NoSuchBucket", False)
    assert classify(client_error("Weird", 500))[1] is True


def stub_storage(tmp_path, responses):
    s = Settings(data_dir=tmp_path, s3_bucket_name="b", s3_max_retries=2, _env_file=None)
    client = boto3.client("s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    stub = Stubber(client)
    for r in responses:
        if isinstance(r, tuple):
            stub.add_client_error("put_object", service_error_code=r[0], http_status_code=r[1])
        else:
            stub.add_response("put_object", {})
    stub.activate()
    return S3Storage(s, client=client, sleeper=lambda s: None), stub


KEY = "flowstore/runs/r/partitions/0/chunks/c.parquet"


def test_upload_retries_then_succeeds(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"PAR1data")
    st, stub = stub_storage(tmp_path, [("SlowDown", 503), ("InternalError", 500), "ok"])
    retries = []
    r = st.upload_file(f, KEY, on_retry=lambda a, d, c: retries.append(c))
    assert r.attempts == 3 and retries == ["SlowDown", "InternalError"]
    stub.assert_no_pending_responses()


def test_upload_retry_exhaustion(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"1")
    st, _ = stub_storage(tmp_path, [("SlowDown", 503)] * 3)
    with pytest.raises(StorageError) as e:
        st.upload_file(f, KEY)
    assert e.value.retryable and e.value.code == "SlowDown"


def test_upload_non_retryable_fails_fast(tmp_path):
    f = tmp_path / "x.parquet"
    f.write_bytes(b"1")
    st, stub = stub_storage(tmp_path, [("AccessDenied", 403)])
    with pytest.raises(StorageError) as e:
        st.upload_file(f, KEY)
    assert not e.value.retryable and e.value.code == "AccessDenied"
    stub.assert_no_pending_responses()


def test_key_safety(tmp_path):
    st, _ = stub_storage(tmp_path, [])
    for bad in ("other/runs/x.parquet", "flowstore/runs/../../secret", "flowstore/runs/r/"):
        with pytest.raises(StorageError):
            st.assert_managed_key(bad)
    with pytest.raises(StorageError):
        st.object_key("../x", 0, "c")


def test_health_reports_missing_bucket_and_config(settings, aws):
    s2 = Settings(data_dir=settings.data_dir, s3_bucket_name="", _env_file=None)
    assert S3Storage(s2, client=aws).health()["ready"] is False
    s3 = Settings(data_dir=settings.data_dir, s3_bucket_name="does-not-exist", _env_file=None)
    h = S3Storage(s3, client=aws).health()
    assert not h["ready"] and any(c["name"] == "bucket_access" and not c["ok"] for c in h["checks"])


# ---- run state machine
def test_state_machine_rejects_invalid_transitions(settings, db, storage, elog):
    e = RunEngine("r1", RunConfig(), settings, db, storage, elog)
    with pytest.raises(InvalidTransition):
        e._set_state("PAUSED")
    assert "COMPLETED" not in TRANSITIONS["RUNNING"]
    with pytest.raises(InvalidTransition):
        e.pause()


# ---- engine integration
def run(settings, db, storage, elog, rid, **kw):
    e = RunEngine(rid, RunConfig(**kw), settings, db, storage, elog)
    e.start()
    e.join(60)
    return e, e.snapshot()


def test_drop_policy_conserves_records(settings, db, storage, elog):
    _, s = run(settings, db, storage, elog, "r-drop", rate_per_second=4000, duration_seconds=3, queue_capacity=50,
               overflow_policy="drop_newest", processor_rate_limit=500)
    assert s["state"] == "COMPLETED" and s["counters"]["dropped_newest"] > 0
    assert s["accounting"]["balanced"] and s["counters"]["persisted"] == s["counters"]["accepted"]
    assert s["queue"]["max_depth"] <= 50


def test_block_policy_never_drops_and_blocks(settings, db, storage, elog):
    _, s = run(settings, db, storage, elog, "r-block", rate_per_second=3000, duration_seconds=3, queue_capacity=50,
               processor_rate_limit=500)
    assert s["counters"]["dropped"] == 0 and s["queue"]["blocked_seconds"] > 0.5
    assert s["counters"]["generated"] < 3000 * 3 * 0.5   # generator slowed by backpressure
    assert s["accounting"]["balanced"] and s["counters"]["persisted"] == s["counters"]["generated"]


def test_invalid_records_are_rejected_and_never_persisted(settings, db, storage, elog):
    _, s = run(settings, db, storage, elog, "r-inv", rate_per_second=500, duration_seconds=3, invalid_ratio=0.4,
               chunk_size_records=100)
    c = s["counters"]
    assert c["rejected_validation"] > 0 and c["persisted"] + c["rejected_validation"] == c["generated"]
    assert db.list_rejected("r-inv") and s["accounting"]["balanced"]
    assert sum(o["record_count"] for o in db.list_objects("r-inv")[0]) == c["persisted"]


def test_upload_failure_never_counts_as_persisted(settings, db, aws, elog):
    class Broken(S3Storage):
        def upload_file(self, *a, **k):
            raise StorageError("AccessDenied: nope", "AccessDenied", False)

    _, s = run(settings, db, Broken(settings, client=aws), elog, "r-fail", rate_per_second=300, duration_seconds=2,
               chunk_size_records=50)
    assert s["state"] == "FAILED" and s["counters"]["persisted"] == 0
    assert s["counters"]["failed"] + s["counters"]["discarded"] > 0 and s["accounting"]["balanced"]
    assert db.list_objects("r-fail")[1] == 0


def test_force_cancel_accounts_unpersisted(settings, db, storage, elog):
    e = RunEngine("r-cancel", RunConfig(rate_per_second=2000, duration_seconds=30, processor_rate_limit=400,
                                        queue_capacity=2000), settings, db, storage, elog)
    e.start()
    time.sleep(1.2)
    e.stop(force=True)
    e.join(30)
    s = e.snapshot()
    assert s["state"] == "CANCELLED" and s["counters"]["discarded"] > 0 and s["accounting"]["balanced"]
    assert db.get_run("r-cancel")["unpersisted"] > 0


def test_restart_marks_interrupted_runs(settings, db):
    db.create_run("zombie", "x", RunConfig().model_dump(mode="json"), time.time())
    db.update_run_state("zombie", "RUNNING")
    assert db.mark_interrupted(time.time()) == ["zombie"]
    assert db.get_run("zombie")["state"] == "INTERRUPTED"


def test_reconcile_recovers_object_missing_from_metadata(settings, db, storage, elog):
    from app.services import storage_service
    run(settings, db, storage, elog, "r-rec", rate_per_second=300, duration_seconds=2, chunk_size_records=100)
    obj = db.list_objects("r-rec")[0][0]
    db.delete_object_row(obj["object_id"])
    r = storage_service.reconcile(db, storage, "r-rec")
    assert obj["object_key"] in r["repaired"]
    assert db.get_object(obj["object_id"])["record_count"] == obj["record_count"]
    storage.delete(obj["object_key"])
    assert obj["object_key"] in storage_service.reconcile(db, storage, "r-rec")["missing_in_s3"]


def test_storage_cap_blocks_uploads(tmp_path, db, aws, elog):
    s = Settings(data_dir=tmp_path, s3_bucket_name="flowstore-test", s3_max_storage_mb=0, _env_file=None)
    _, snap = run(s, db, S3Storage(s, client=aws), elog, "r-cap", rate_per_second=200, duration_seconds=1,
                  chunk_size_records=50)
    assert snap["counters"]["persisted"] == 0 and snap["counters"]["failed"] > 0
