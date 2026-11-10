"""Run configuration, presets and validation. One frozen snapshot per run."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OverflowPolicy(StrEnum):
    BLOCK = "block"
    DROP_NEWEST = "drop_newest"
    DROP_OLDEST = "drop_oldest"


class Compression(StrEnum):
    SNAPPY = "snappy"
    ZSTD = "zstd"
    GZIP = "gzip"
    NONE = "none"


class RunConfig(BaseModel):
    """Effective configuration of one run. Frozen after the run starts."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario: str = Field(default="Baseline", max_length=60)
    seed: int = Field(default=42, ge=0, le=2**32 - 1)

    # Generator
    rate_per_second: int = Field(default=500, ge=1, le=20_000)
    duration_seconds: int = Field(default=20, ge=1, le=300)
    num_devices: int = Field(default=50, ge=1, le=10_000)
    invalid_ratio: float = Field(default=0.0, ge=0.0, le=0.9)
    burst_enabled: bool = False
    burst_multiplier: float = Field(default=4.0, ge=1.0, le=20.0)
    burst_every_seconds: int = Field(default=10, ge=2, le=120)
    burst_duration_seconds: int = Field(default=3, ge=1, le=30)

    # Queue
    queue_capacity: int = Field(default=2000, ge=10, le=100_000)
    overflow_policy: OverflowPolicy = OverflowPolicy.BLOCK

    # Processor
    processor_rate_limit: int = Field(
        default=0, ge=0, le=100_000,
        description="Max records/s the processor may handle (0 = unthrottled)",
    )
    processor_mode: Literal["local", "lambda"] = Field(
        default="local",
        description="'local' validates in the backend process; 'lambda' sends each batch to an AWS Lambda function",
    )

    # Buffers / chunks
    num_partitions: int = Field(default=4, ge=1, le=32)
    chunk_size_records: int = Field(default=1000, ge=10, le=100_000)
    chunk_max_bytes: int = Field(default=8_000_000, ge=10_000, le=64_000_000)
    chunk_max_age_seconds: float = Field(default=5.0, ge=0.5, le=120.0)

    # Parquet / S3
    compression: Compression = Compression.SNAPPY
    max_pending_uploads: int = Field(default=4, ge=1, le=32)
    upload_concurrency: int = Field(default=2, ge=1, le=16)
    injected_upload_delay_ms: int = Field(
        default=0, ge=0, le=5000,
        description="EXPERIMENT CONDITION: artificial delay added before each upload",
    )

    @property
    def planned_records(self) -> int:
        base = self.rate_per_second * self.duration_seconds
        if self.burst_enabled:
            cycles = self.duration_seconds / self.burst_every_seconds
            extra = (
                cycles * self.burst_duration_seconds
                * self.rate_per_second * (self.burst_multiplier - 1)
            )
            base += int(extra)
        return base

    @model_validator(mode="after")
    def _check(self) -> "RunConfig":
        if self.burst_enabled and self.burst_duration_seconds >= self.burst_every_seconds:
            raise ValueError("burst_duration_seconds must be < burst_every_seconds")
        return self


def estimate(cfg: RunConfig, max_records: int, max_rate: int) -> dict[str, Any]:
    """Pre-run workload estimate and warnings (all heuristic, labelled as such)."""
    planned = cfg.planned_records
    valid = planned * (1 - cfg.invalid_ratio)
    est_chunks = max(1, int(valid / cfg.chunk_size_records) + cfg.num_partitions)
    warnings: list[str] = []
    if planned > max_records:
        warnings.append(f"Planned records ({planned:,}) exceed the cap of {max_records:,}.")
    if cfg.rate_per_second > max_rate:
        warnings.append(f"Rate exceeds the cap of {max_rate:,}/s.")
    if est_chunks > 150:
        warnings.append(
            f"~{est_chunks} S3 objects (PUT requests) will be created; "
            "consider a larger chunk size to limit request overhead."
        )
    if cfg.chunk_size_records / cfg.num_partitions < 20:
        warnings.append("Very small per-partition chunks create many tiny objects.")
    if cfg.overflow_policy != OverflowPolicy.BLOCK:
        warnings.append("Non-blocking overflow policies will lose records when the queue is full.")
    if cfg.processor_mode == "lambda":
        warnings.append("Lambda processor: every batch makes a network round trip to AWS Lambda. This adds latency "
                        "and caps processor throughput at roughly batch size / round-trip time.")
    return {
        "planned_records": planned,
        "estimated_valid_records": int(valid),
        "estimated_chunks_upper_bound": est_chunks,
        "estimated_put_requests": est_chunks,
        "warnings": warnings,
        "note": "Estimates are heuristic; actual counts are measured during the run.",
    }


PRESETS: list[dict[str, Any]] = [
    {
        "id": "baseline", "name": "Baseline Ingestion",
        "description": "Balanced producer and consumer. Expect a stable queue and steady S3 uploads.",
        "expected": "Stable queue, consistent chunk creation, minimal backpressure.",
        "config": {"scenario": "Baseline Ingestion", "rate_per_second": 500, "duration_seconds": 20,
                   "queue_capacity": 2000, "chunk_size_records": 1000},
    },
    {
        "id": "high_load", "name": "High Ingestion Load",
        "description": "Generator rate far above the baseline.",
        "expected": "Higher queue use, more buffer activity, more frequent chunks.",
        "config": {"scenario": "High Ingestion Load", "rate_per_second": 5000, "duration_seconds": 15,
                   "queue_capacity": 4000, "chunk_size_records": 2000, "num_partitions": 8},
    },
    {
        "id": "slow_processing", "name": "Slow Processing",
        "description": "Processor capped well below the producer rate; queue drops newest events when full.",
        "expected": "Queue builds to capacity, then newest events are dropped.",
        "config": {"scenario": "Slow Processing", "rate_per_second": 1500, "duration_seconds": 15,
                   "processor_rate_limit": 600, "queue_capacity": 1000,
                   "overflow_policy": "drop_newest"},
    },
    {
        "id": "queue_saturation", "name": "Queue Saturation",
        "description": "Small queue, fast producer, blocking overflow: shows backpressure.",
        "expected": "Queue pinned at capacity; producer blocked time grows; generator rate falls.",
        "config": {"scenario": "Queue Saturation", "rate_per_second": 3000, "duration_seconds": 15,
                   "processor_rate_limit": 800, "queue_capacity": 200, "overflow_policy": "block"},
    },
    {
        "id": "invalid_records", "name": "Invalid Records",
        "description": "30% of generated records are deliberately malformed.",
        "expected": "Many rejections with visible reasons; fewer persisted records.",
        "config": {"scenario": "Invalid Records", "rate_per_second": 800, "duration_seconds": 15,
                   "invalid_ratio": 0.3},
    },
    {
        "id": "small_chunks", "name": "Small Chunks",
        "description": "Tiny chunk threshold. Run it, then run Large Chunks and compare.",
        "expected": "Many small Parquet files, higher upload overhead.",
        "config": {"scenario": "Small Chunks", "rate_per_second": 1000, "duration_seconds": 15,
                   "chunk_size_records": 100, "num_partitions": 4},
    },
    {
        "id": "large_chunks", "name": "Large Chunks",
        "description": "Large chunk threshold: fewer, larger files.",
        "expected": "Few big Parquet files, better compression, higher flush latency.",
        "config": {"scenario": "Large Chunks", "rate_per_second": 1000, "duration_seconds": 15,
                   "chunk_size_records": 5000, "num_partitions": 4, "chunk_max_age_seconds": 30},
    },
    {
        "id": "storage_bottleneck", "name": "Storage Bottleneck",
        "description": "Upload concurrency 1 plus an injected 600 ms upload delay (an experiment "
                       "condition, not real AWS latency).",
        "expected": "Pending uploads fill, chunk builder stalls, queue builds upstream.",
        "config": {"scenario": "Storage Bottleneck", "rate_per_second": 2000, "duration_seconds": 15,
                   "chunk_size_records": 200, "upload_concurrency": 1, "max_pending_uploads": 2,
                   "injected_upload_delay_ms": 600, "queue_capacity": 1000},
    },
    {
        "id": "burst", "name": "Burst Traffic",
        "description": "Periodic 5x traffic spikes on top of a modest base rate.",
        "expected": "Transient queue growth during bursts and recovery afterward.",
        "config": {"scenario": "Burst Traffic", "rate_per_second": 600, "duration_seconds": 30,
                   "burst_enabled": True, "burst_multiplier": 5.0, "burst_every_seconds": 10,
                   "burst_duration_seconds": 3, "processor_rate_limit": 1500, "queue_capacity": 1500},
    },
]
