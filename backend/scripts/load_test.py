"""Bounded LOCAL load test: pushes the engine hard against an in-process mocked S3 (no AWS cost).

    python scripts/load_test.py [rate] [seconds]      # defaults: 15000 ev/s for 10 s

Reports achieved generation/processing rates, queue peak, files, and memory (RSS) so you can judge
what instance size you need. Never run this against real S3.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

from app.core.config import Settings  # noqa: E402
from app.models.db import Database  # noqa: E402
from app.pipeline.engine import RunEngine  # noqa: E402
from app.schemas.run_config import RunConfig  # noqa: E402
from app.storage.s3 import S3Storage  # noqa: E402
from app.telemetry.eventlog import EventLog  # noqa: E402


def rss_mb() -> float:
    try:
        import psutil  # type: ignore
        return psutil.Process().memory_info().rss / 1e6
    except Exception:
        return float("nan")


def main() -> None:
    rate = int(sys.argv[1]) if len(sys.argv) > 1 else 15000
    secs = min(int(sys.argv[2]) if len(sys.argv) > 2 else 10, 60)
    with mock_aws(), tempfile.TemporaryDirectory() as tmp:
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="load")
        settings = Settings(data_dir=Path(tmp), s3_bucket_name="load", _env_file=None)
        db = Database(settings.db_path)
        eng = RunEngine("load-run", RunConfig(rate_per_second=min(rate, 20000), duration_seconds=secs, queue_capacity=5000,
                                              num_partitions=8, chunk_size_records=2000), settings, db,
                        S3Storage(settings, client=client), EventLog(db))
        t0 = time.time()
        eng.start()
        peak_rss = 0.0
        while eng.is_active():
            peak_rss = max(peak_rss, rss_mb())
            time.sleep(0.5)
        eng.join()
        s, wall = eng.snapshot(), time.time() - t0
        c = s["counters"]
        print(f"state={s['state']} wall={wall:.1f}s generated={c['generated']:,} ({c['generated'] / wall:,.0f}/s) "
              f"persisted={c['persisted']:,} files={c['files']} queue_peak={s['queue']['max_depth']:,}/{s['queue']['capacity']:,} "
              f"blocked={s['queue']['blocked_seconds']:.1f}s peak_rss={peak_rss:.0f}MB balanced={s['accounting']['balanced']}")


if __name__ == "__main__":
    main()
