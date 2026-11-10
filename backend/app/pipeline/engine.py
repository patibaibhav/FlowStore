"""The concurrent ingestion engine for one run.

Threads: generator -> [BoundedQueue] -> processor -> partition buffers -> (chunks) ->
bounded upload queue -> N uploader threads (Parquet write + S3 upload + SQLite record).
A sampler thread aggregates real counters into telemetry snapshots.
"""

from __future__ import annotations

import queue as stdqueue
import threading
import time
import uuid
from collections import Counter, deque
from pathlib import Path
from typing import Any, Callable

from app.core.config import Settings
from app.models.db import Database
from app.pipeline import parquet_writer as pw
from app.pipeline.bounded_queue import BoundedQueue
from app.pipeline.events import EventGenerator
from app.pipeline.processor_backend import ProcessorBackend, ProcessorError, build_processor
from app.schemas.run_config import RunConfig
from app.storage.s3 import S3Storage, StorageError
from app.telemetry.bottleneck import diagnose
from app.telemetry.eventlog import EventLog

IDLE, STARTING, RUNNING, PAUSING, PAUSED = "IDLE", "STARTING", "RUNNING", "PAUSING", "PAUSED"
STOPPING, FLUSHING, COMPLETED, FAILED, CANCELLED = "STOPPING", "FLUSHING", "COMPLETED", "FAILED", "CANCELLED"
INTERRUPTED = "INTERRUPTED"
ACTIVE_STATES = {STARTING, RUNNING, PAUSING, PAUSED, STOPPING, FLUSHING}
TERMINAL_STATES = {COMPLETED, FAILED, CANCELLED, INTERRUPTED}

TRANSITIONS: dict[str, set[str]] = {
    IDLE: {STARTING},
    STARTING: {RUNNING, STOPPING, FAILED, CANCELLED},
    RUNNING: {PAUSING, STOPPING, FLUSHING, FAILED, CANCELLED},
    PAUSING: {PAUSED, RUNNING, STOPPING, FAILED, CANCELLED},
    PAUSED: {RUNNING, STOPPING, FAILED, CANCELLED},
    STOPPING: {FLUSHING, FAILED, CANCELLED},
    FLUSHING: {COMPLETED, FAILED, CANCELLED},
}

SENTINEL = object()


class InvalidTransition(Exception):
    pass


class RunEngine:
    def __init__(self, run_id: str, cfg: RunConfig, settings: Settings, db: Database, storage: S3Storage,
                 eventlog: EventLog, on_snapshot: Callable[[dict[str, Any]], None] | None = None,
                 processor: ProcessorBackend | None = None, cloudwatch: Any | None = None) -> None:
        self.run_id, self.cfg, self.settings, self.db, self.storage = run_id, cfg, settings, db, storage
        self.elog = eventlog
        self.on_snapshot = on_snapshot
        self.processor: ProcessorBackend = processor or build_processor(cfg, settings)
        self.cloudwatch = cloudwatch
        self.state = IDLE
        self._state_lock = threading.RLock()
        self.started_at = 0.0
        self.ended_at: float | None = None
        self.note: str | None = None
        self.stopped_early = False

        self.queue = BoundedQueue(cfg.queue_capacity, cfg.overflow_policy)
        self.buffers = pw.PartitionBuffers(run_id, cfg.num_partitions, cfg.chunk_size_records,
                                           cfg.chunk_max_bytes, cfg.chunk_max_age_seconds)
        self.upload_q: stdqueue.Queue = stdqueue.Queue(maxsize=cfg.max_pending_uploads)

        # events controlling threads
        self.resume_ev = threading.Event(); self.resume_ev.set()
        self.gen_stop = threading.Event()   # stop producing (graceful)
        self.gen_done = threading.Event()
        self.abort = threading.Event()      # force cancel / fatal
        self.ticker_stop = threading.Event()
        self.sampler_stop = threading.Event()
        self.gen_paused_ack = threading.Event()
        self.proc_paused_ack = threading.Event()

        # counters, guarded by c_lock
        self.c_lock = threading.Lock()
        self.c = Counter()
        self.upload_ms: deque[float] = deque(maxlen=50)
        self.serialize_ms: deque[float] = deque(maxlen=50)
        self.latency_ema = 0.0
        self.inflight_writes = 0
        self.inflight_uploads = 0
        self.builder_blocked = False
        self.consecutive_fatal = 0
        self.consecutive_proc_failures = 0
        self.processor_fatal = False
        self.proc_batch_ms_max = 0.0
        self.fatal: str | None = None
        self.last_upload_at: float | None = None
        self.last_upload_error: str | None = None
        self.reject_reasons: Counter = Counter()
        self.peak_throughput = 0.0
        self.peak_queue = 0

        self._hist: deque[tuple[float, dict[str, float]]] = deque(maxlen=60)
        self._rates: dict[str, float] = {k: 0.0 for k in ("generation", "enqueue", "processing", "persist", "upload_bytes", "chunks", "rejected", "dropped")}
        self._threads: list[threading.Thread] = []
        self._uploaders: list[threading.Thread] = []
        self._main: threading.Thread | None = None
        self._q_hi = False
        self._was_blocked = False
        self._last_drop_log = 0
        self._last_rej_log = 0.0
        self._final_summary: dict[str, Any] | None = None
        self._last_db_sample = 0.0
        self._pending_discard = 0

    # ------------------------------------------------------------------ state
    def _set_state(self, new: str) -> None:
        with self._state_lock:
            if new == self.state:
                return
            if new not in TRANSITIONS.get(self.state, set()):
                raise InvalidTransition(f"{self.state} -> {new}")
            old, self.state = self.state, new
        try:
            self.db.update_run_state(self.run_id, new)
        except Exception:
            pass
        self.elog.log(self.run_id, "Run Controller", "INFO", f"State {old} → {new}", {"from": old, "to": new})

    def _try_state(self, new: str) -> bool:
        try:
            self._set_state(new)
            return True
        except InvalidTransition:
            return False

    def is_active(self) -> bool:
        return self.state in ACTIVE_STATES

    # --------------------------------------------------------------- controls
    def start(self) -> None:
        self.started_at = time.time()
        self.db.create_run(self.run_id, self.cfg.scenario, self.cfg.model_dump(mode="json"), self.started_at)
        self._set_state(STARTING)
        self.elog.log(self.run_id, "Run Controller", "SUCCESS", "Run started",
                      {"scenario": self.cfg.scenario, "planned_records": self.cfg.planned_records})
        self._main = threading.Thread(target=self._run, name=f"run-{self.run_id}", daemon=True)
        self._main.start()

    def pause(self) -> None:
        with self._state_lock:
            if self.state != RUNNING:
                raise InvalidTransition(f"cannot pause from {self.state}")
            self.gen_paused_ack.clear(); self.proc_paused_ack.clear()
            self.resume_ev.clear()
            self._set_state(PAUSING)
        self.elog.log(self.run_id, "Run Controller", "INFO",
                      "Pause requested: generator and processor pause at the next batch boundary; "
                      "in-flight chunks and uploads continue")

    def resume(self) -> None:
        with self._state_lock:
            if self.state not in (PAUSED, PAUSING):
                raise InvalidTransition(f"cannot resume from {self.state}")
            self._set_state(RUNNING)
            self.resume_ev.set()
        self.elog.log(self.run_id, "Run Controller", "INFO", "Run resumed (same run, counters preserved)")

    def stop(self, force: bool = False) -> None:
        with self._state_lock:
            if self.state == FLUSHING and force:
                self.abort.set()
                self.note = "Force-cancelled during flush: unpersisted records were discarded."
            elif self.state not in (RUNNING, PAUSING, PAUSED, STARTING):
                raise InvalidTransition(f"cannot stop from {self.state}")
            else:
                self.stopped_early = True
                if force:
                    self.abort.set()
                    self.note = "Force-cancelled: accepted records that were not yet persisted were discarded."
                self.gen_stop.set()
                self._set_state(STOPPING)
            self.resume_ev.set()
        self.elog.log(self.run_id, "Run Controller", "WARNING" if force else "INFO",
                      "Force cancel requested" if force else "Graceful stop requested: draining queue and flushing buffers")

    def join(self, timeout: float | None = None) -> None:
        if self._main:
            self._main.join(timeout)

    # ---------------------------------------------------------------- threads
    def _run(self) -> None:
        try:
            self._try_state(RUNNING)
            for target, name in ((self._generator, "gen"), (self._processor, "proc"), (self._ticker, "ticker")):
                t = threading.Thread(target=self._guard(target), name=f"{self.run_id}-{name}", daemon=True)
                t.start(); self._threads.append(t)
            for i in range(self.cfg.upload_concurrency):
                t = threading.Thread(target=self._guard(self._uploader), name=f"{self.run_id}-up{i}", daemon=True)
                t.start(); self._uploaders.append(t)
            sampler = threading.Thread(target=self._sampler, name=f"{self.run_id}-sampler", daemon=True)
            sampler.start()

            gen_t, proc_t, tick_t = self._threads
            gen_t.join()
            proc_t.join()          # processor ends after draining queue (or abort)
            self.ticker_stop.set(); tick_t.join()
            self._finalize_pipeline()
            for t in self._uploaders:
                t.join()
            self._finish()
            self.sampler_stop.set(); sampler.join(timeout=2)
            self._publish()
        except BaseException as e:  # pragma: no cover - defensive
            self.fatal = f"{type(e).__name__}: {e}"
            self.elog.log(self.run_id, "Run Controller", "ERROR", f"Engine crashed: {self.fatal}")
            self.abort.set(); self.sampler_stop.set()
            self._force_terminal(FAILED)

    def _guard(self, fn: Callable[[], None]) -> Callable[[], None]:
        def wrapper() -> None:
            try:
                fn()
            except BaseException as e:
                self.fatal = f"{fn.__name__} crashed: {type(e).__name__}: {e}"
                self.elog.log(self.run_id, "System", "ERROR", self.fatal)
                self.abort.set(); self.gen_stop.set()
        return wrapper

    # ----- generator
    def _rate_at(self, active: float) -> float:
        cfg = self.cfg
        rate = float(cfg.rate_per_second)
        if cfg.burst_enabled and (active % cfg.burst_every_seconds) >= cfg.burst_every_seconds - cfg.burst_duration_seconds:
            rate *= cfg.burst_multiplier
        return min(rate, self.settings.max_rate_per_second)

    def _generator(self) -> None:
        cfg = self.cfg
        gen = EventGenerator(cfg.seed, cfg.num_devices, cfg.num_partitions, cfg.invalid_ratio)
        max_total = self.settings.max_records_per_run
        duration = min(cfg.duration_seconds, self.settings.max_run_seconds)
        active, acc, produced = 0.0, 0.0, 0
        last = time.monotonic()
        try:
            while not self.gen_stop.is_set():
                if not self.resume_ev.is_set():
                    self.gen_paused_ack.set()
                    self.resume_ev.wait(0.05)
                    last = time.monotonic()
                    continue
                self.gen_paused_ack.clear()
                now = time.monotonic()
                elapsed_tick = now - last
                last = now
                if active >= duration or produced >= max_total:
                    break
                active += elapsed_tick           # wall-clock run time (pauses excluded), incl. blocked time
                acc += self._rate_at(active) * min(elapsed_tick, 0.1)   # no catch-up burst after a block
                n = min(int(acc), max_total - produced)
                acc -= int(acc)
                if n > 0:
                    t = time.time()
                    batch = [gen.next_event(t) for _ in range(n)]
                    produced += n
                    with self.c_lock:
                        self.c["generated"] += n
                    _, dropped, abandoned = self.queue.put_many(batch, self.abort)
                    if abandoned:
                        with self.c_lock:
                            self.c["abandoned"] += abandoned
                self.gen_stop.wait(0.01)
        finally:
            self.gen_done.set()
            self.elog.log(self.run_id, "Generator", "INFO", "Generator finished", {"generated": self.c["generated"]})

    # ----- processor
    def _processor(self) -> None:
        cfg = self.cfg
        proc = self.processor
        limit = cfg.processor_rate_limit
        cap = proc.max_batch
        batch_size = cap if not limit else max(1, min(cap, limit // 20))
        while True:
            if self.abort.is_set():
                break
            if not self.resume_ev.is_set():
                self.proc_paused_ack.set()
                self.resume_ev.wait(0.05)
                continue
            self.proc_paused_ack.clear()
            batch = self.queue.get_batch(batch_size, 0.05)
            if not batch:
                if self.gen_done.is_set() and self.queue.depth() == 0:
                    break
                continue
            t0 = time.perf_counter()
            now = time.time()
            lat_sum = 0.0
            for ev in batch:
                ts = ev.get("timestamp")
                if isinstance(ts, (int, float)) and ts == ts:
                    lat_sum += now - ts
            try:
                res = proc.process(batch, cfg.num_partitions, now, self.abort)
            except ProcessorError as e:
                self._processor_failed(batch, e)
                continue
            batch_ms = (time.perf_counter() - t0) * 1000.0
            rows, rejected = res.rows, len(res.rejected)
            for rej in res.rejected:
                self.reject_reasons[rej.reason] += 1
                try:
                    self.db.add_rejected(self.run_id, rej.event_id, now, rej.reason, rej.field,
                                         self.settings.max_rejected_rows_per_run)
                except Exception:
                    pass
            sealed = self.buffers.add_many(rows) if rows else []
            with self.c_lock:
                self.consecutive_proc_failures = 0
                self.c["valid"] += len(rows)
                self.c["rejected_validation"] += rejected
                self.c["sealed"] += sum(ch.record_count for ch in sealed)
                self.c["chunks_created"] += len(sealed)
                self.c["proc_batches"] += 1
                self.c["proc_records"] += len(batch)
                self.c["proc_batch_us"] += int(batch_ms * 1000)
                self.proc_batch_ms_max = max(self.proc_batch_ms_max, batch_ms)
                avg = lat_sum / len(batch)
                self.latency_ema = avg if self.latency_ema == 0 else self.latency_ema * 0.8 + avg * 0.2
            for ch in sealed:
                self._submit(ch)
            if limit:
                need = len(batch) / limit
                spent = time.perf_counter() - t0
                if need > spent:
                    self.abort.wait(need - spent)

    def _processor_failed(self, batch: list[dict[str, Any]], err: ProcessorError) -> None:
        """A batch the processor could not handle (Lambda unreachable/crashed): count it, never persist it."""
        n = len(batch)
        cancelled = err.code == "Cancelled"
        with self.c_lock:
            if cancelled:
                self.c["discarded"] += n
                self.c["discarded_processing"] += n
            else:
                self.c["processor_failed"] += n
                self.c["processor_failed_batches"] += 1
                self.consecutive_proc_failures += 1
                if self.consecutive_proc_failures >= 3 and not self.fatal:
                    self.processor_fatal = True
                    self.fatal = f"Lambda processor failed 3 consecutive batches ({err.code}); aborting run"
                    self.abort.set(); self.gen_stop.set()
        if not cancelled:
            self.elog.log(self.run_id, "Processor", "ERROR",
                          f"Processor failed for a batch of {n} records: {err}",
                          {"code": err.code, "records": n})

    def _submit(self, chunk: pw.Chunk) -> None:
        """Hand a sealed chunk to the bounded upload queue (storage → upstream backpressure)."""
        self.elog.log(self.run_id, "Chunk Builder", "INFO",
                      f"Chunk {chunk.chunk_id} sealed ({chunk.record_count} records, reason={chunk.seal_reason})",
                      {"chunk_id": chunk.chunk_id, "partition": chunk.partition_id})
        self._save_chunk(chunk)
        blocked_start = None
        while True:
            if self.abort.is_set():
                self._discard_chunk(chunk)
                break
            try:
                self.upload_q.put(chunk, timeout=0.1)
                break
            except stdqueue.Full:
                if blocked_start is None:
                    blocked_start = time.monotonic()
                    self.builder_blocked = True
        if blocked_start is not None:
            self.builder_blocked = False
            with self.c_lock:
                self.c["builder_blocked_ms"] += int((time.monotonic() - blocked_start) * 1000)

    def _discard_chunk(self, chunk: pw.Chunk) -> None:
        with self.c_lock:
            self.c["discarded"] += chunk.record_count
            self.c["discarded_chunk_records"] += chunk.record_count
        chunk.set_status(pw.FAILED)
        self._save_chunk(chunk, error="discarded: run cancelled")
        chunk.rows = None

    # ----- ticker (age-based flush)
    def _ticker(self) -> None:
        while not self.ticker_stop.wait(0.25):
            if self.abort.is_set():
                continue
            aged = self.buffers.seal_aged()
            if aged:
                with self.c_lock:
                    self.c["sealed"] += sum(ch.record_count for ch in aged)
                    self.c["chunks_created"] += len(aged)
                for ch in aged:
                    self._submit(ch)

    def _finalize_pipeline(self) -> None:
        if self.abort.is_set():
            q = self.queue.clear()
            b = self.buffers.discard_all()
            with self.c_lock:
                self.c["discarded"] += q + b
                self.c["discarded_buffer"] += b
            self.elog.log(self.run_id, "Run Controller", "WARNING",
                          f"Discarded {q} queued and {b} buffered records (force cancel / fatal error)")
        else:
            self._try_state(FLUSHING)
            final = self.buffers.seal_all("end_of_run")
            with self.c_lock:
                self.c["sealed"] += sum(ch.record_count for ch in final)
                self.c["chunks_created"] += len(final)
            self.elog.log(self.run_id, "Buffer", "INFO", f"End-of-run flush: {len(final)} partial chunks")
            for ch in final:
                self._submit(ch)
        for _ in self._uploaders:
            self.upload_q.put(SENTINEL)

    # ----- uploader
    def _save_chunk(self, chunk: pw.Chunk, error: str | None = None) -> None:
        try:
            self.db.upsert_chunk({
                "chunk_id": chunk.chunk_id, "run_id": chunk.run_id, "partition_id": chunk.partition_id,
                "record_count": chunk.record_count, "status": chunk.status, "seal_reason": chunk.seal_reason,
                "created_at": chunk.created_at, "uncompressed_bytes": chunk.approx_bytes,
                "parquet_bytes": chunk.parquet_bytes, "serialize_ms": chunk.serialize_ms,
                "updated_at": time.time(), "error": error,
            })
        except Exception as e:  # metadata failure must be visible, not fatal to the loop
            self.elog.log(self.run_id, "System", "ERROR", f"SQLite write failed: {type(e).__name__}")

    def _uploader(self) -> None:
        cfg = self.cfg
        staging = self.settings.staging_dir / self.run_id
        while True:
            chunk = self.upload_q.get()
            if chunk is SENTINEL:
                break
            if self.abort.is_set():
                self._discard_chunk(chunk)
                continue
            self._process_chunk(chunk, staging, cfg.compression.value)
        try:
            staging.rmdir()
        except OSError:
            pass

    def _process_chunk(self, chunk: pw.Chunk, staging: Path, compression: str) -> None:
        cfg = self.cfg
        n = chunk.record_count
        # ---- write parquet
        chunk.set_status(pw.WRITING); self._save_chunk(chunk)
        with self.c_lock:
            self.inflight_writes += 1
        try:
            res = pw.write_parquet(chunk, staging, compression)
        except BaseException as e:
            chunk.set_status(pw.WRITE_FAILED)
            self.elog.log(self.run_id, "Parquet Writer", "ERROR", f"Parquet write failed for {chunk.chunk_id}: {type(e).__name__}: {e}")
            self._fail_chunk(chunk, f"write: {type(e).__name__}: {e}")
            return
        finally:
            with self.c_lock:
                self.inflight_writes -= 1
        chunk.parquet_bytes, chunk.serialize_ms = res.size_bytes, res.serialize_ms
        with self.c_lock:
            self.serialize_ms.append(res.serialize_ms)
            self.c["uncompressed_bytes"] += res.uncompressed_bytes
            self.c["files_written"] += 1
        chunk.rows = None  # free memory once serialized
        self.elog.log(self.run_id, "Parquet Writer", "SUCCESS",
                      f"Parquet chunk written: {chunk.chunk_id} ({res.size_bytes:,} B, ratio {res.compression_ratio:.2f}x)",
                      {"serialize_ms": round(res.serialize_ms, 2)})
        # ---- storage cap
        cap = self.settings.s3_max_storage_mb * 1024 * 1024
        used = self.db.usage_summary()["bytes"]
        if used + res.size_bytes > cap:
            chunk.set_status(pw.UPLOAD_FAILED)
            msg = f"S3_MAX_STORAGE_MB cap ({self.settings.s3_max_storage_mb} MB) would be exceeded"
            self.elog.log(self.run_id, "Amazon S3", "ERROR", msg)
            self._fail_chunk(chunk, msg, path=res.path)
            return
        # ---- upload
        try:
            key = self.storage.object_key(self.run_id, chunk.partition_id, chunk.chunk_id)
        except StorageError as e:
            self._fail_chunk(chunk, str(e), path=res.path); return
        chunk.set_status(pw.UPLOADING); self._save_chunk(chunk)
        self.elog.log(self.run_id, "Amazon S3", "INFO", f"S3 upload started: {key}")
        with self.c_lock:
            self.inflight_uploads += 1
        try:
            if cfg.injected_upload_delay_ms:
                self.abort.wait(cfg.injected_upload_delay_ms / 1000)  # EXPERIMENT CONDITION

            def on_retry(attempt: int, delay: float, code: str) -> None:
                chunk.set_status(pw.RETRY_PENDING)
                with self.c_lock:
                    self.c["upload_retries"] += 1
                self.elog.log(self.run_id, "Amazon S3", "WARNING",
                              f"Retry scheduled for {chunk.chunk_id}: attempt {attempt} failed ({code}), waiting {delay:.2f}s")

            up = self.storage.upload_file(res.path, key, cancel=self.abort, on_retry=on_retry)
        except StorageError as e:
            chunk.set_status(pw.UPLOAD_FAILED)
            self.last_upload_error = str(e)
            self.elog.log(self.run_id, "Amazon S3", "ERROR", f"S3 upload failed for {chunk.chunk_id}: {e}")
            with self.c_lock:
                if not e.retryable and e.code != "Cancelled":
                    self.consecutive_fatal += 1
                    if self.consecutive_fatal >= 3 and not self.fatal:
                        self.fatal = f"S3 rejected 3 consecutive uploads ({e.code}); aborting run"
                        self.abort.set(); self.gen_stop.set()
            self._fail_chunk(chunk, str(e), path=res.path)
            return
        finally:
            with self.c_lock:
                self.inflight_uploads -= 1
        # ---- durable metadata (S3 succeeded). Only now may the chunk be PERSISTED.
        obj = {"object_id": uuid.uuid5(uuid.NAMESPACE_URL, key).hex, "run_id": self.run_id,
               "chunk_id": chunk.chunk_id, "partition_id": chunk.partition_id, "bucket": self.storage.bucket,
               "object_key": key, "size_bytes": up.size_bytes, "record_count": n, "compression": compression,
               "uploaded_at": time.time(), "upload_ms": up.duration_ms, "attempts": up.attempts}
        recorded = False
        for attempt in range(3):
            try:
                self.db.record_persisted(obj, {"parquet_bytes": up.size_bytes, "updated_at": time.time()})
                recorded = True
                break
            except Exception as e:
                time.sleep(0.05 * (attempt + 1))
                err = e
        if not recorded:
            chunk.set_status(pw.FAILED)
            with self.c_lock:
                self.c["failed"] += n
                self.c["unrecorded_uploads"] += 1
            self.elog.log(self.run_id, "System", "ERROR",
                          f"Uploaded {key} but SQLite metadata write failed ({type(err).__name__}); run reconcile to recover",
                          {"key": key})
            res.path.unlink(missing_ok=True)
            return
        chunk.set_status(pw.PERSISTED)
        with self.c_lock:
            self.c["persisted"] += n
            self.c["persisted_bytes"] += up.size_bytes
            self.c["files"] += 1
            self.c["upload_ms_total"] += int(up.duration_ms)
            self.upload_ms.append(up.duration_ms)
            self.consecutive_fatal = 0
            self.last_upload_at = time.time()
        res.path.unlink(missing_ok=True)
        self.elog.log(self.run_id, "Amazon S3", "SUCCESS",
                      f"S3 upload completed: {key} ({up.size_bytes:,} B in {up.duration_ms:.0f} ms, attempts={up.attempts})")

    def _fail_chunk(self, chunk: pw.Chunk, error: str, path: Path | None = None) -> None:
        with self.c_lock:
            self.c["failed"] += chunk.record_count
            self.c["failed_chunks"] += 1
        chunk.set_status(pw.FAILED)
        chunk.rows = None
        self._save_chunk(chunk, error=error)
        if path:
            path.unlink(missing_ok=True)

    # ------------------------------------------------------------- finishing
    def _finish(self) -> None:
        summary = self._summary()
        if self.abort.is_set() and self.fatal:
            terminal = FAILED; self.note = self.fatal
        elif self.abort.is_set():
            terminal = CANCELLED
        else:
            terminal = COMPLETED
            if self.stopped_early:
                self.note = "Stopped early by user: queue drained, buffers flushed, uploads completed."
            if self.c["failed"]:
                self.note = (self.note or "") + f" {self.c['failed']} records failed to persist."
        with self._state_lock:
            if self.state not in TERMINAL_STATES:
                if terminal == COMPLETED:
                    self._try_state(FLUSHING)
                if not self._try_state(terminal):
                    self.state = terminal
        self.ended_at = time.time()
        self.db.finish_run(self.run_id, self.state, self.ended_at, summary, self.note)
        sev = "SUCCESS" if self.state == COMPLETED else "ERROR" if self.state == FAILED else "WARNING"
        self.elog.log(self.run_id, "Run Controller", sev, f"Run {self.state.lower()}",
                      {"persisted": summary["persisted"], "files": summary["files"], "unpersisted": summary["unpersisted"]})
        self._final_summary = summary

    def _force_terminal(self, terminal: str) -> None:
        with self._state_lock:
            self.state = terminal
        try:
            self.ended_at = self.ended_at or time.time()
            self.db.finish_run(self.run_id, terminal, self.ended_at, self._summary(), self.fatal or self.note)
        except Exception:
            pass

    def _summary(self) -> dict[str, Any]:
        q = self.queue.metrics()
        with self.c_lock:
            c = dict(self.c)
        elapsed = max(0.001, (self.ended_at or time.time()) - self.started_at)
        files = c.get("files", 0)
        dropped = q["dropped_newest"] + q["evicted_oldest"]
        accepted = q["enqueued"]
        persisted = c.get("persisted", 0)
        return {
            "generated": c.get("generated", 0), "accepted": accepted,
            "rejected_validation": c.get("rejected_validation", 0), "dropped": dropped,
            "persisted": persisted,
            "unpersisted": max(0, c.get("valid", 0) - persisted),
            "peak_queue_depth": q["max_depth"],
            "avg_throughput": c.get("valid", 0) / elapsed, "peak_throughput": self.peak_throughput,
            "files": files, "s3_bytes": c.get("persisted_bytes", 0),
            "uncompressed_bytes": c.get("uncompressed_bytes", 0),
            "avg_upload_ms": (c.get("upload_ms_total", 0) / files) if files else 0.0,
            "failures": c.get("failed_chunks", 0) + c.get("unrecorded_uploads", 0),
            "processor_failed": c.get("processor_failed", 0),
            "processor_batch_ms": (c.get("proc_batch_us", 0) / 1000.0 / c["proc_batches"]) if c.get("proc_batches") else 0.0,
            "processor_rtt_ms": (self.processor.stats() or {}).get("rtt_ms_avg", 0.0),
        }

    # ------------------------------------------------------------- telemetry
    def _sampler(self) -> None:
        period = 1.0 / self.settings.telemetry_hz
        while not self.sampler_stop.wait(period):
            try:
                self._promote_pause()
                self._publish()
            except Exception as e:  # pragma: no cover
                self.elog.log(self.run_id, "System", "ERROR", f"sampler error: {e}")

    def _promote_pause(self) -> None:
        if self.state == PAUSING and (self.gen_paused_ack.is_set() or self.gen_done.is_set()) and \
                (self.proc_paused_ack.is_set() or self.gen_done.is_set()):
            if self._try_state(PAUSED):
                self.elog.log(self.run_id, "Run Controller", "INFO", "Run paused")

    def _update_rates(self, now: float, snap: dict[str, float]) -> None:
        self._hist.append((now, snap))
        # find sample ~1s old
        base = self._hist[0]
        for t, s in self._hist:
            if now - t <= 1.2:
                base = (t, s)
                break
        dt = now - base[0]
        if dt < 0.2:
            return
        keymap = {"generation": "generated", "enqueue": "enqueued", "processing": "dequeued", "persist": "persisted",
                  "upload_bytes": "persisted_bytes", "chunks": "chunks_created", "rejected": "rejected_validation", "dropped": "dropped"}
        for rate_key, ckey in keymap.items():
            self._rates[rate_key] = max(0.0, (snap[ckey] - base[1][ckey]) / dt)
        if self._rates["processing"] > self.peak_throughput:
            self.peak_throughput = self._rates["processing"]

    def snapshot(self) -> dict[str, Any]:
        cfg = self.cfg
        q = self.queue.metrics()
        bstats = self.buffers.stats()
        with self.c_lock:
            c = dict(self.c)
            inflight_w, inflight_u = self.inflight_writes, self.inflight_uploads
            ups = list(self.upload_ms)
            sers = list(self.serialize_ms)
            lat = self.latency_ema
        now = time.time()
        elapsed = (self.ended_at or now) - self.started_at if self.started_at else 0.0
        dropped = q["dropped_newest"] + q["evicted_oldest"]
        valid = c.get("valid", 0)
        sealed = c.get("sealed", 0)
        persisted, failed, discarded = c.get("persisted", 0), c.get("failed", 0), c.get("discarded", 0)
        buffered = max(0, valid - sealed - c.get("discarded_buffer", 0))
        chunk_inflight = max(0, sealed - persisted - failed - c.get("discarded_chunk_records", 0))
        proc_failed = c.get("processor_failed", 0)
        processing_inflight = max(0, q["dequeued"] - valid - c.get("rejected_validation", 0)
                                  - proc_failed - c.get("discarded_processing", 0))
        proc_batches, proc_records = c.get("proc_batches", 0), c.get("proc_records", 0)
        lam = self.processor.stats() or None
        gen_pending = max(0, c.get("generated", 0) - c.get("abandoned", 0) - q["dropped_newest"] - q["enqueued"])
        pending_uploads = self.upload_q.qsize() + inflight_w + inflight_u
        self._update_rates(time.monotonic(), {
            "generated": c.get("generated", 0), "enqueued": q["enqueued"], "dequeued": q["dequeued"],
            "persisted": persisted, "persisted_bytes": c.get("persisted_bytes", 0),
            "chunks_created": c.get("chunks_created", 0), "rejected_validation": c.get("rejected_validation", 0),
            "dropped": dropped})
        self.peak_queue = max(self.peak_queue, q["max_depth"])
        files = c.get("files", 0)
        avg_up = (c.get("upload_ms_total", 0) / files) if files else 0.0
        state = self.state
        terminal = state in TERMINAL_STATES
        rej_rate = c.get("rejected_validation", 0) / max(1, q["dequeued"])
        snap: dict[str, Any] = {
            "run_id": self.run_id, "state": state, "scenario": cfg.scenario, "ts": now,
            "elapsed_s": elapsed, "planned_records": cfg.planned_records, "note": self.note,
            "stopped_early": self.stopped_early,
            "counters": {
                "generated": c.get("generated", 0), "accepted": q["enqueued"], "dropped": dropped,
                "dropped_newest": q["dropped_newest"], "evicted_oldest": q["evicted_oldest"],
                "rejected_validation": c.get("rejected_validation", 0), "persisted": persisted,
                "failed": failed, "discarded": discarded, "abandoned": c.get("abandoned", 0),
                "processor_failed": proc_failed,
                "persisted_bytes": c.get("persisted_bytes", 0), "files": files,
            },
            "rates": dict(self._rates),
            "generator": {"configured_rate": cfg.rate_per_second, "actual_rate": self._rates["generation"],
                          "generated": c.get("generated", 0), "done": self.gen_done.is_set(),
                          "burst_enabled": cfg.burst_enabled},
            "queue": q,
            "processor": {"processed": valid + c.get("rejected_validation", 0),
                          "rate": self._rates["processing"], "rejected": c.get("rejected_validation", 0),
                          "reject_ratio": rej_rate, "latency_ms": lat * 1000,
                          "rate_limit": cfg.processor_rate_limit, "top_rejections": self.reject_reasons.most_common(5),
                          "mode": self.processor.mode, "emulated": self.processor.emulated,
                          "failed": proc_failed,
                          # measured per-batch processing time (local: validation loop; lambda: full round trip)
                          "batch_ms_avg": (c.get("proc_batch_us", 0) / 1000.0 / proc_batches) if proc_batches else 0.0,
                          "batch_ms_max": self.proc_batch_ms_max,
                          "records_per_batch_avg": (proc_records / proc_batches) if proc_batches else 0.0,
                          "us_per_record": (c.get("proc_batch_us", 0) / proc_records) if proc_records else 0.0,
                          "lambda": lam},
            "buffers": {**bstats, "utilization": bstats["records"] / bstats["capacity"] if bstats["capacity"] else 0.0},
            "chunks": {"created": c.get("chunks_created", 0), "pending": self.upload_q.qsize(),
                       "target_size": cfg.chunk_size_records, "current_max_fill": max((p["records"] for p in bstats["partitions"]), default=0),
                       "builder_blocked": self.builder_blocked, "completed": files + c.get("failed_chunks", 0),
                       "blocked_ms": c.get("builder_blocked_ms", 0)},
            "writer": {"writing": inflight_w, "uploading": inflight_u, "pending_uploads": pending_uploads,
                       "max_pending": cfg.max_pending_uploads, "succeeded": files,
                       "failed": c.get("failed_chunks", 0), "retries": c.get("upload_retries", 0),
                       "avg_serialize_ms": sum(sers) / len(sers) if sers else 0.0,
                       "avg_upload_ms": avg_up, "recent_upload_ms": ups[-1] if ups else 0.0,
                       "compression_ratio": (c.get("uncompressed_bytes", 0) / c.get("persisted_bytes", 1)) if c.get("persisted_bytes") else 0.0,
                       "injected_delay_ms": cfg.injected_upload_delay_ms},
            "s3": {"bucket": self.storage.bucket, "objects": files, "bytes": c.get("persisted_bytes", 0),
                   "last_upload_at": self.last_upload_at, "last_error": self.last_upload_error,
                   "upload_bytes_rate": self._rates["upload_bytes"], "retries": c.get("upload_retries", 0),
                   "failures": c.get("failed_chunks", 0)},
            "accounting": {
                "generated": c.get("generated", 0), "generator_pending": gen_pending,
                "abandoned": c.get("abandoned", 0), "dropped_newest": q["dropped_newest"],
                "evicted_oldest": q["evicted_oldest"], "in_queue": q["depth"],
                "processing": processing_inflight, "rejected_validation": c.get("rejected_validation", 0),
                "buffered": buffered, "chunks_in_flight": chunk_inflight, "persisted": persisted,
                "failed": failed, "discarded": discarded, "processor_failed": proc_failed,
            },
            "config_summary": {"max_pending_uploads": cfg.max_pending_uploads, "processor_rate_limit": cfg.processor_rate_limit,
                               "processor_mode": cfg.processor_mode},
        }
        acc = snap["accounting"]
        # Conservation: generated = abandoned + dropped_newest + pending + queue + evicted + processing + rejected
        #               + buffered + chunks_in_flight + persisted + failed(+discarded handled below)
        total = (acc["abandoned"] + acc["dropped_newest"] + acc["generator_pending"] + acc["evicted_oldest"]
                 + acc["in_queue"] + acc["processing"] + acc["rejected_validation"] + acc["buffered"]
                 + acc["chunks_in_flight"] + acc["persisted"] + acc["failed"] + acc["discarded"]
                 + acc["processor_failed"])
        # discarded records are also counted inside the location they were discarded from only at the end;
        acc["unaccounted"] = acc["generated"] - total if terminal else None
        acc["balanced"] = (acc["unaccounted"] == 0) if terminal else None
        snap["nodes"] = self._node_states(snap, state)
        snap["bottleneck"] = diagnose(snap)
        return snap

    def _node_states(self, s: dict[str, Any], state: str) -> dict[str, str]:
        q, w = s["queue"], s["writer"]
        done = state in TERMINAL_STATES
        if done:
            fail = state == FAILED
            bad = "processor" if self.processor_fatal else "s3"
            return {k: ("error" if fail and k == bad else "completed") for k in
                    ("generator", "queue", "processor", "buffers", "chunk_builder", "parquet_writer", "s3")}
        paused = state in (PAUSING, PAUSED)
        n: dict[str, str] = {}
        n["generator"] = "completed" if s["generator"]["done"] else ("waiting" if paused else ("backpressured" if q["blocked_now"] else "running"))
        n["queue"] = "backpressured" if (q["blocked_now"] or q["utilization"] >= 0.9) else ("warning" if q["utilization"] >= 0.7 else ("idle" if q["depth"] == 0 else "running"))
        n["processor"] = "error" if self.processor_fatal else ("waiting" if (paused or q["depth"] == 0) else "running")
        n["buffers"] = "flushing" if state == FLUSHING else ("running" if s["buffers"]["records"] else "idle")
        n["chunk_builder"] = "backpressured" if s["chunks"]["builder_blocked"] else ("flushing" if state == FLUSHING else "running" if s["chunks"]["created"] else "idle")
        n["parquet_writer"] = "uploading" if w["uploading"] else ("running" if w["writing"] else ("waiting" if w["pending_uploads"] else "idle"))
        n["s3"] = "error" if ((self.fatal and not self.processor_fatal) or (w["failed"] and s["counters"]["persisted"] == 0 and w["failed"] >= 3)) else (
            "warning" if (w["retries"] and w["uploading"]) else ("uploading" if w["uploading"] else "idle"))
        return n

    def _publish(self) -> None:
        snap = self.snapshot()
        self._edge_events(snap)
        now = time.time()
        if now - self._last_db_sample >= 1.0:
            self._last_db_sample = now
            try:
                self.db.add_metric_sample(self.run_id, now, {
                    "elapsed": snap["elapsed_s"], "gen_rate": snap["rates"]["generation"],
                    "proc_rate": snap["rates"]["processing"], "persist_rate": snap["rates"]["persist"],
                    "queue_depth": snap["queue"]["depth"], "queue_capacity": snap["queue"]["capacity"],
                    "buffered": snap["buffers"]["records"], "chunks": snap["counters"]["files"],
                    "chunk_rate": snap["rates"]["chunks"], "upload_ms": snap["writer"]["recent_upload_ms"],
                    "persisted": snap["counters"]["persisted"], "rejected": snap["counters"]["rejected_validation"],
                    "dropped": snap["counters"]["dropped"], "reject_rate": snap["rates"]["rejected"],
                    "drop_rate": snap["rates"]["dropped"], "pending_uploads": snap["writer"]["pending_uploads"],
                    "batch_ms": snap["processor"]["batch_ms_avg"],
                    "lambda_rtt_ms": (snap["processor"]["lambda"] or {}).get("rtt_ms_avg", 0.0),
                }, self.settings.max_metric_rows_per_run)
            except Exception:
                pass
        if self.cloudwatch is not None:
            try:
                self.cloudwatch.maybe_publish(snap)
            except Exception:  # best effort, never affects a run
                pass
        if self.on_snapshot:
            self.on_snapshot(snap)

    def _edge_events(self, snap: dict[str, Any]) -> None:
        q = snap["queue"]
        if q["utilization"] >= 0.9 and not self._q_hi:
            self._q_hi = True
            self.elog.log(self.run_id, "Queue", "WARNING", f"Queue reached {q['utilization']:.0%} capacity",
                          {"depth": q["depth"], "capacity": q["capacity"]})
        elif q["utilization"] < 0.7 and self._q_hi:
            self._q_hi = False
            self.elog.log(self.run_id, "Queue", "INFO", "Queue utilization recovered below 70%")
        if q["blocked_now"] and not self._was_blocked:
            self._was_blocked = True
            self.elog.log(self.run_id, "Generator", "WARNING", "Producer blocked due to backpressure (queue full)")
        elif not q["blocked_now"] and self._was_blocked:
            self._was_blocked = False
            self.elog.log(self.run_id, "Generator", "INFO", f"Producer unblocked (total blocked {q['blocked_seconds']:.2f}s)")
        d = q["dropped_newest"] + q["evicted_oldest"]
        if d > self._last_drop_log and time.time() - getattr(self, "_drop_t", 0) > 2:
            self.elog.log(self.run_id, "Queue", "WARNING", f"{d - self._last_drop_log} records dropped by '{q['policy']}' policy", {"total_dropped": d})
            self._last_drop_log, self._drop_t = d, time.time()
        rej = snap["counters"]["rejected_validation"]
        if rej > self._last_rej_log and time.time() - getattr(self, "_rej_t", 0) > 2:
            top = snap["processor"]["top_rejections"]
            self.elog.log(self.run_id, "Validation", "WARNING",
                          f"{int(rej - self._last_rej_log)} events rejected; top reason: {top[0][0] if top else 'n/a'}",
                          {"top_reasons": top, "total_rejected": rej})
            self._last_rej_log, self._rej_t = rej, time.time()
