"""Chunk model, partition buffers and the PyArrow Parquet writer."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from app.pipeline.events import approx_row_bytes

SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("device_id", pa.string(), nullable=False),
    pa.field("sensor_type", pa.string(), nullable=False),
    pa.field("sensor_value", pa.float64(), nullable=False),
    pa.field("partition_key", pa.int32(), nullable=False),
    pa.field("quality_flag", pa.string(), nullable=False),
    pa.field("metadata", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
])

ROW_GROUP_SIZE = 10_000
CODECS = {"snappy": "snappy", "zstd": "zstd", "gzip": "gzip", "none": "NONE"}

# Chunk lifecycle
BUFFERING, READY, WRITING, UPLOADING, PERSISTED = "BUFFERING", "READY", "WRITING", "UPLOADING", "PERSISTED"
WRITE_FAILED, UPLOAD_FAILED, RETRY_PENDING, FAILED = "WRITE_FAILED", "UPLOAD_FAILED", "RETRY_PENDING", "FAILED"


@dataclass
class Chunk:
    chunk_id: str
    run_id: str
    partition_id: int
    rows: list[dict[str, Any]] | None
    record_count: int
    created_at: float
    approx_bytes: int
    seal_reason: str
    status: str = READY
    parquet_bytes: int | None = None
    serialize_ms: float | None = None
    history: list[tuple[str, float]] = field(default_factory=list)

    def set_status(self, status: str) -> None:
        self.status = status
        self.history.append((status, time.time()))


class PartitionBuffers:
    """Thread-safe per-partition record buffers with size/bytes/age thresholds."""

    def __init__(self, run_id: str, num_partitions: int, max_records: int, max_bytes: int, max_age: float) -> None:
        self.run_id = run_id
        self.num_partitions = num_partitions
        self.max_records = max_records
        self.max_bytes = max_bytes
        self.max_age = max_age
        self._lock = threading.Lock()
        self._rows: list[list[dict[str, Any]]] = [[] for _ in range(num_partitions)]
        self._bytes = [0] * num_partitions
        self._first_ts = [0.0] * num_partitions
        self._seq = [0] * num_partitions
        self.chunks_created = 0

    def _seal_locked(self, p: int, reason: str) -> Chunk:
        rows = self._rows[p]
        chunk = Chunk(
            chunk_id=f"{self.run_id}-p{p}-{self._seq[p]:06d}",
            run_id=self.run_id, partition_id=p, rows=rows, record_count=len(rows),
            created_at=time.time(), approx_bytes=self._bytes[p], seal_reason=reason,
        )
        chunk.history.append((BUFFERING, self._first_ts[p] or chunk.created_at))
        chunk.set_status(READY)
        self._seq[p] += 1
        self._rows[p] = []
        self._bytes[p] = 0
        self._first_ts[p] = 0.0
        self.chunks_created += 1
        return chunk

    def add_many(self, rows: list[dict[str, Any]]) -> list[Chunk]:
        """Route rows to partitions; return chunks sealed by record/byte thresholds."""
        sealed: list[Chunk] = []
        now = time.monotonic()
        with self._lock:
            for row in rows:
                p = row["partition_key"]
                if not self._rows[p]:
                    self._first_ts[p] = now
                self._rows[p].append(row)
                self._bytes[p] += approx_row_bytes(row)
                if len(self._rows[p]) >= self.max_records:
                    sealed.append(self._seal_locked(p, "size"))
                elif self._bytes[p] >= self.max_bytes:
                    sealed.append(self._seal_locked(p, "bytes"))
        return sealed

    def seal_aged(self) -> list[Chunk]:
        now = time.monotonic()
        out: list[Chunk] = []
        with self._lock:
            for p in range(self.num_partitions):
                if self._rows[p] and now - self._first_ts[p] >= self.max_age:
                    out.append(self._seal_locked(p, "age"))
        return out

    def seal_all(self, reason: str = "end_of_run") -> list[Chunk]:
        with self._lock:
            return [self._seal_locked(p, reason) for p in range(self.num_partitions) if self._rows[p]]

    def discard_all(self) -> int:
        with self._lock:
            n = sum(len(r) for r in self._rows)
            self._rows = [[] for _ in range(self.num_partitions)]
            self._bytes = [0] * self.num_partitions
            return n

    def stats(self) -> dict[str, Any]:
        with self._lock:
            parts = [
                {"partition": p, "records": len(self._rows[p]), "bytes": self._bytes[p],
                 "age_s": (time.monotonic() - self._first_ts[p]) if self._rows[p] else 0.0}
                for p in range(self.num_partitions)
            ]
        total = sum(x["records"] for x in parts)
        return {
            "partitions": parts,
            "records": total,
            "bytes": sum(x["bytes"] for x in parts),
            "active_partitions": sum(1 for x in parts if x["records"]),
            "capacity": self.max_records * self.num_partitions,
            "max_age_s": max((x["age_s"] for x in parts), default=0.0),
        }


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    cols = {name: [r[name] for r in rows] for name in SCHEMA.names}
    # epoch seconds (float) -> microsecond timestamps
    for tcol in ("timestamp", "ingested_at"):
        cols[tcol] = pa.array([int(v * 1_000_000) for v in cols[tcol]], type=pa.int64()).cast(SCHEMA.field(tcol).type)
    arrays = [
        cols[f.name] if isinstance(cols[f.name], pa.Array) else pa.array(cols[f.name], type=f.type)
        for f in SCHEMA
    ]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


@dataclass
class WriteResult:
    path: Path
    size_bytes: int
    uncompressed_bytes: int
    serialize_ms: float
    record_count: int
    compression: str

    @property
    def compression_ratio(self) -> float:
        return self.uncompressed_bytes / self.size_bytes if self.size_bytes else 0.0


def write_parquet(chunk: Chunk, staging_dir: Path, compression: str) -> WriteResult:
    """Serialize a chunk to a temp file, validate it is readable, atomically rename."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    final = staging_dir / f"{chunk.chunk_id}.parquet"
    tmp = staging_dir / f"{chunk.chunk_id}.parquet.tmp"
    assert chunk.rows is not None
    t0 = time.perf_counter()
    try:
        table = rows_to_table(chunk.rows)
        pq.write_table(table, tmp, compression=CODECS[compression], row_group_size=ROW_GROUP_SIZE,
                       write_statistics=True)
        serialize_ms = (time.perf_counter() - t0) * 1000
        # validate: footer readable and record count matches
        md = pq.ParquetFile(tmp).metadata
        if md.num_rows != chunk.record_count:
            raise ValueError(f"row count mismatch: wrote {md.num_rows}, expected {chunk.record_count}")
        os.replace(tmp, final)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return WriteResult(final, final.stat().st_size, table.nbytes, serialize_ms, chunk.record_count, compression)


def cleanup_staging(staging_dir: Path, max_age_s: float = 3600) -> int:
    """Remove abandoned temp/staged files older than ``max_age_s``. Returns count removed."""
    n = 0
    if not staging_dir.exists():
        return 0
    cutoff = time.time() - max_age_s
    for f in staging_dir.iterdir():
        if f.is_file() and (f.suffix in (".tmp", ".parquet")) and f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)
            n += 1
    return n
