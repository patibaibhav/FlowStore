import time
from app.pipeline.engine import RunEngine
from app.schemas.run_config import RunConfig


def test_baseline_run_conserves_records(settings, db, storage, elog):
    cfg = RunConfig(rate_per_second=800, duration_seconds=3, chunk_size_records=200, num_partitions=3)
    e = RunEngine("run-smoke", cfg, settings, db, storage, elog)
    e.start(); e.join(30)
    s = e.snapshot()
    print(s["counters"], s["accounting"])
    assert s["state"] == "COMPLETED"
    assert s["accounting"]["balanced"] is True
    assert s["counters"]["persisted"] == s["counters"]["generated"]
    assert len(storage.list_keys("run-smoke")) == s["counters"]["files"]
