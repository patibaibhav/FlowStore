"""Opt-in integration test against REAL Amazon S3.

Skipped unless AWS_INTEGRATION=1 and S3_BUCKET_NAME are set. Uploads one tiny Parquet file under
``{prefix}/_integration/`` ... then reads it back, validates it and deletes it. Costs a fraction of a cent.
"""

import os
import time
import uuid

import pyarrow.parquet as pq
import pytest

from app.core.config import Settings
from app.pipeline import parquet_writer as pw
from app.pipeline.events import EventGenerator, validate
from app.storage.s3 import S3Storage

pytestmark = pytest.mark.skipif(
    os.getenv("AWS_INTEGRATION") != "1" or not os.getenv("S3_BUCKET_NAME"),
    reason="set AWS_INTEGRATION=1 and S3_BUCKET_NAME to run against real AWS",
)


def test_real_s3_roundtrip(tmp_path):
    settings = Settings(data_dir=tmp_path, _env_file=None)
    storage = S3Storage(settings)
    health = storage.health(write_probe=True)
    assert health["ready"] and health["write_verified"], health["checks"]

    g = EventGenerator(5, 5, 1, 0)
    rows = [validate(g.next_event(), 1)[0] for _ in range(50)]
    chunk = pw.Chunk("it-" + uuid.uuid4().hex[:8], "integration", 0, rows, 50, time.time(), 1, "size")
    res = pw.write_parquet(chunk, tmp_path, "snappy")
    key = f"{storage.prefix}/runs/integration/partitions/0/chunks/{chunk.chunk_id}.parquet"
    try:
        up = storage.upload_file(res.path, key)
        assert up.size_bytes == res.size_bytes
        info = storage.inspect(key, sample_rows=5)
        assert info["num_rows"] == 50 and len(info["sample"]) == 5
        downloaded = tmp_path / "back.parquet"
        downloaded.write_bytes(b"".join(storage.download_stream(key)))
        assert pq.read_table(downloaded).num_rows == 50
    finally:
        storage.delete(key)
    assert key not in {o["key"] for o in storage.list_keys("integration")}
