# Architecture

## 1. Seven-stage pipeline
```
Generator ─▶ BoundedQueue ─▶ Processor ─▶ PartitionBuffers ─▶ Chunk (sealed) ─▶ upload queue ─▶ Parquet write ─▶ S3 PUT ─▶ SQLite record
 thread        (bounded)      thread        (per partition)    ticker + proc     (bounded)        uploader threads
```
Implemented in `backend/app/pipeline/`:
| File | Responsibility |
|---|---|
| `events.py` | seeded generator, event schema, validation + transformation |
| `bounded_queue.py` | bounded queue, overflow policies, blocked-time metrics |
| `parquet_writer.py` | `PartitionBuffers`, `Chunk`, PyArrow writer, staging cleanup |
| `engine.py` | `RunEngine`: threads, run state machine, counters, snapshots |
| `processor_backend.py` | `LocalProcessor` (in-process, default) and `LambdaProcessor` (optional AWS Lambda call) behind one interface |
| `lambda_handler.py` | the Lambda function source; packaged with `events.py` by `lambda/build.py` |
| `../storage/s3.py` | boto3 adapter: health, upload+retry, listing, ranged Parquet inspection |

## 2. Concurrency model
Plain threads (PyArrow/boto3 release the GIL during I/O and serialization). Per run: 1 generator, 1 processor, 1 age-flush ticker, *N* uploader threads (`upload_concurrency`) and 1 sampler. FastAPI's event loop only does HTTP/WebSocket work: engine threads never run on it, and the WebSocket hub reads the latest snapshot at a bounded rate (`TELEMETRY_HZ`), so a slow client can never slow ingestion.

Generation is time-based, not a busy loop: every ~10 ms it produces `rate × Δt` events (Δt capped at 100 ms so there is **no catch-up burst** after being blocked). Run duration is wall-clock time (pauses excluded), so backpressure reduces *how much* is generated, exactly as it would in reality.

## 3. Backpressure chain
```
S3 slow → upload slots full (max_pending_uploads) → chunk builder stalls → processor stalls
        → queue fills → generator blocks (policy=block) | records dropped (drop_newest / drop_oldest)
```
Memory and temp-disk are bounded at every hop: queue capacity, partitions × chunk size, `max_pending_uploads` sealed chunks plus those in the uploaders. Rows are released as soon as a chunk is serialized.

Overflow policies (`overflow_policy`): `block` (producer waits; blocked time is measured), `drop_newest` (incoming rejected, counted as `dropped_newest`), `drop_oldest` (queued records evicted, counted separately as `evicted_oldest`).

## 4. Run state machine
`IDLE → STARTING → RUNNING ⇄ PAUSING → PAUSED → … → STOPPING → FLUSHING → COMPLETED | FAILED | CANCELLED`, plus `INTERRUPTED` (set at startup for runs that were active when the backend died). Transitions are validated (`TRANSITIONS` in `engine.py`); illegal ones return HTTP 409. Only one run may be active.

* **Pause**: generator and processor stop at the next batch boundary. Chunks already sealed and uploads already in flight **continue**; buffers still age-flush. Resume continues the *same* run with the same counters.
* **Stop (graceful)**: generator stops, the processor drains the queue, buffers are sealed as partial chunks (`end_of_run`), uploads finish → `COMPLETED` (`stopped_early=true`).
* **Force cancel**: abort flag; queue and buffers are discarded; the discarded count is stored (`unpersisted`) → `CANCELLED`.
* **Fatal storage error**: 3 consecutive non-retryable upload failures (e.g. AccessDenied) abort the run → `FAILED`.

## 5. Chunk lifecycle
`BUFFERING → READY → WRITING → UPLOADING → PERSISTED`, failures `WRITE_FAILED`, `UPLOAD_FAILED`, `RETRY_PENDING`, `FAILED`. A chunk is `PERSISTED` only **after** the S3 PUT succeeded **and** `s3_objects` + the chunk status were committed in one SQLite transaction. If the DB write fails after a successful PUT the chunk is *not* counted as persisted; `POST /api/storage/reconcile` later lists the S3 prefix, reads the Parquet footer by ranged GET and records the missing metadata.

Chunk id = `{run_id}-p{partition}-{seq:06d}`; object key = `{prefix}/runs/{run_id}/partitions/{p}/chunks/{chunk_id}.parquet`. Deterministic names make retries idempotent (re-PUT overwrites the same key).

## 6. Parquet
Explicit schema (`event_id`, `timestamp[us,UTC]`, `device_id`, `sensor_type`, `sensor_value: float64`, `partition_key: int32`, `quality_flag`, `metadata`, `ingested_at`). Written to `*.parquet.tmp`, footer re-read and row-count verified, then atomically renamed. Compression: snappy / zstd / gzip / none. Reported "compression ratio" = in-memory Arrow bytes ÷ file bytes (measured, never guessed). Stale staging files are removed at startup.

## 7. S3 behaviour
* Credentials from the default provider chain (IAM role on EC2). No keys in code, frontend, or compose file.
* **Health check before every run**: config present → credentials → `head_bucket` → controlled PUT+DELETE probe (`write_verified`). The run is refused (HTTP 424) with actionable messages otherwise. There is **no** fallback to local storage.
* Retries: bounded (`S3_MAX_RETRIES`), exponential backoff with jitter. Retryable: throttling/`SlowDown`, 5xx, timeouts, connection errors. Non-retryable: `AccessDenied`, `NoSuchBucket`, invalid/expired credentials, other 4xx.
* The app only touches keys under `{prefix}/runs/`; `..` and foreign prefixes are rejected. Downloads stream through the backend (no public URLs); deletes need `confirm=true` (and `X-Admin-Token` if configured) and are refused for the active run. Object inspection uses ranged GETs (footer + one row group), never a full download.
* `S3_MAX_STORAGE_MB` stops uploads when the cap would be exceeded.

## 8. SQLite metadata (`models/db.py`)
Tables: `runs`, `run_metrics` (sampled ~1/s, bounded with thinning), `pipeline_events` (bounded, INFO rows pruned first), `chunks`, `s3_objects`, `rejected_records` (bounded sample), `schema_migrations`. Foreign keys with cascade, unique keys on `chunk_id`/`object_key`, WAL mode, one shared connection guarded by an `RLock`. Sensor events themselves are **never** stored in SQLite.

## 9. Telemetry
`WS /ws/telemetry`: a `hello` message with the full snapshot and recent events, then `telemetry` (during a run) or `heartbeat` (idle, every ~5 s) messages carrying a monotonically increasing `seq`, the snapshot and the events since the client's last delivery. The frontend ignores any message with `seq` ≤ last seen, and reconnects with exponential backoff. Slow/dead clients are dropped (send timeout 2 s) without affecting the pipeline.

## 10. Metrics and accounting
See `frontend/src/learning/content.ts` (Learning Center → Metrics). Invariant, checked in tests and shown in the UI once a run ends:

`generated = abandoned + dropped_newest + evicted_oldest + discarded + rejected_validation + persisted + failed`

During a run, in-flight records (queue, processing, buffered, chunks in flight) are reported separately, so `generated ≠ persisted` while running.

## 11. Failure handling
| Failure | Behaviour |
|---|---|
| Missing credentials / bucket / AccessDenied | Run refused with actionable checks; mid-run AccessDenied → `FAILED` after 3 |
| Throttling / 5xx / timeout | retried with backoff; counted in `upload_retries` |
| Retries exhausted | chunk `FAILED`, records counted `failed` (never `persisted`) |
| SQLite failure after upload | chunk not counted persisted; reconcile recovers |
| Parquet error | `WRITE_FAILED`, records `failed`, temp file removed |
| Backend restart | active runs become `INTERRUPTED`; staging cleaned; history intact |
| WebSocket drop | client auto-reconnects and receives a fresh `hello` |

No exactly-once claim is made: after a crash, in-memory records (queue, buffers) are lost and runs are marked `INTERRUPTED`; uploaded objects are not duplicated because keys are deterministic.

## 12. Processor modes (optional AWS Lambda)
`RunConfig.processor_mode` is `local` (default) or `lambda`. Only the per-batch validation step moves: the queue, partition buffers, rejected-record storage (SQLite), counters, pause/stop and accounting stay in the backend process.
* **Local**: `LocalProcessor` runs `events.validate()` in the processor thread (batches of up to 256).
* **Lambda**: `LambdaProcessor` sends a batch (up to 512 records) to the function with a synchronous `Invoke`, which runs the *same* `validate()` and returns `{rows, rejected, handler_ms, cold_start, code_hash}`. Retries use the S3 backoff helper (`backoff_delay`); throttling/5xx/timeouts are retried, AccessDenied/ResourceNotFound/function crashes are not. Validation is pure, so retrying a call is safe.
* **No silent fallback**: before a Lambda-mode run `RunManager.start` makes a real pre-warm call; failure returns HTTP 424 `PROCESSOR_NOT_READY` with the failing check. A batch that fails after retries is counted in `processor_failed` (never persisted); 3 consecutive failed batches fail the run (the Processor node turns `error`). The accounting invariant becomes `generated = abandoned + dropped + evicted + discarded + rejected + persisted + failed + processor_failed`.
* **Availability**: the UI toggle is enabled only when the server reports `lambda_available` (`LAMBDA_PROCESSOR_NAME` set, or `LAMBDA_EMULATE_LOCAL=true`); with the S3 emulator the option is disabled and the processor is always in-process. No AWS call is made at startup.
* **Measured timing** (both modes): per-batch time (avg/max), records per batch, µs per record. Lambda adds measured round trip, handler time, network + invoke overhead (round trip − handler), cold starts, invocations/errors/retries. `processor.latency_ms` is queue age at dequeue and does not include Lambda time; use the batch/round-trip numbers.
* **Version check**: the handler returns a hash of its `events.py`; the pre-warm warns if it differs from the backend's (otherwise local-vs-Lambda comparisons are not like for like).
* Stored per run (`runs.processor_failed`, `processor_batch_ms`, `processor_rtt_ms`) so Compare can show them.

## 13. CloudWatch (optional)
`telemetry/cloudwatch.py` publishes, best-effort and off by default (`CLOUDWATCH_ENABLED`, never with the S3 emulator), at most 10 custom metrics in one `PutMetricData` call per `CLOUDWATCH_INTERVAL_SECONDS` while a run is active plus a final flush: QueueDepth, GeneratedPerSec, ProcessedPerSec, PersistedPerSec, RejectedPerSec, DroppedPerSec, ProducerBlockedMs, UploadLatencyMs, UploadRetries and (Lambda mode) LambdaRttMs. **No dimensions**: every dimension combination is a separate billable custom metric. Values come from the same measured snapshot the UI uses; failures are logged and swallowed. Lambda (Invocations, Duration, Errors, Throttles) and EC2 basic metrics are emitted by AWS automatically.
