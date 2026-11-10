"""SQLite metadata store (runs, sampled telemetry, events, chunks, S3 objects).

Only metadata lives here. Sensor events live in Parquet objects on S3.
A single shared connection guarded by an RLock keeps concurrent writes safe.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable

MIGRATIONS: list[str] = [
    # v1
    """
    CREATE TABLE runs (
        run_id TEXT PRIMARY KEY,
        scenario TEXT NOT NULL,
        config_json TEXT NOT NULL,
        state TEXT NOT NULL,
        started_at REAL NOT NULL,
        ended_at REAL,
        duration_s REAL,
        generated INTEGER NOT NULL DEFAULT 0,
        accepted INTEGER NOT NULL DEFAULT 0,
        rejected_validation INTEGER NOT NULL DEFAULT 0,
        dropped INTEGER NOT NULL DEFAULT 0,
        persisted INTEGER NOT NULL DEFAULT 0,
        unpersisted INTEGER NOT NULL DEFAULT 0,
        peak_queue_depth INTEGER NOT NULL DEFAULT 0,
        avg_throughput REAL NOT NULL DEFAULT 0,
        peak_throughput REAL NOT NULL DEFAULT 0,
        files INTEGER NOT NULL DEFAULT 0,
        s3_bytes INTEGER NOT NULL DEFAULT 0,
        uncompressed_bytes INTEGER NOT NULL DEFAULT 0,
        avg_upload_ms REAL NOT NULL DEFAULT 0,
        failures INTEGER NOT NULL DEFAULT 0,
        note TEXT
    );
    CREATE INDEX idx_runs_started ON runs(started_at DESC);

    CREATE TABLE run_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        t REAL NOT NULL,
        payload TEXT NOT NULL
    );
    CREATE INDEX idx_metrics_run ON run_metrics(run_id, t);

    CREATE TABLE pipeline_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT,
        ts REAL NOT NULL,
        component TEXT NOT NULL,
        severity TEXT NOT NULL,
        message TEXT NOT NULL,
        details TEXT
    );
    CREATE INDEX idx_events_run ON pipeline_events(run_id, id);

    CREATE TABLE chunks (
        chunk_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        partition_id INTEGER NOT NULL,
        record_count INTEGER NOT NULL,
        status TEXT NOT NULL,
        seal_reason TEXT,
        created_at REAL NOT NULL,
        uncompressed_bytes INTEGER,
        parquet_bytes INTEGER,
        serialize_ms REAL,
        updated_at REAL NOT NULL,
        error TEXT
    );
    CREATE INDEX idx_chunks_run ON chunks(run_id);

    CREATE TABLE s3_objects (
        object_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        chunk_id TEXT NOT NULL UNIQUE,
        partition_id INTEGER NOT NULL,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL UNIQUE,
        size_bytes INTEGER NOT NULL,
        record_count INTEGER NOT NULL,
        compression TEXT NOT NULL,
        uploaded_at REAL NOT NULL,
        upload_ms REAL NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 1,
        status TEXT NOT NULL DEFAULT 'PERSISTED'
    );
    CREATE INDEX idx_objects_run ON s3_objects(run_id);

    CREATE TABLE rejected_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        event_id TEXT,
        ts REAL NOT NULL,
        reason TEXT NOT NULL,
        field TEXT
    );
    CREATE INDEX idx_rej_run ON rejected_records(run_id);
    """,
    # v2: processor timing (local or Lambda) and batches lost by the processor
    """
    ALTER TABLE runs ADD COLUMN processor_failed INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE runs ADD COLUMN processor_batch_ms REAL NOT NULL DEFAULT 0;
    ALTER TABLE runs ADD COLUMN processor_rtt_ms REAL NOT NULL DEFAULT 0;
    """,
]


class Database:
    def __init__(self, path: Path | str) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._migrate()

    # ------------------------------------------------------------------
    def _migrate(self) -> None:
        with self._lock:
            self._conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)")
            row = self._conn.execute("SELECT COALESCE(MAX(version),0) FROM schema_migrations").fetchone()
            current = row[0]
            for i, script in enumerate(MIGRATIONS, start=1):
                if i <= current:
                    continue
                self._conn.execute("BEGIN")
                try:
                    for stmt in script.split(";\n"):
                        if stmt.strip():
                            self._conn.execute(stmt)
                    self._conn.execute("INSERT INTO schema_migrations(version) VALUES (?)", (i,))
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise

    def execute(self, sql: str, params: Iterable[Any] | dict[str, Any] = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params if isinstance(params, dict) else tuple(params))

    def transaction(self, statements: list[tuple[str, tuple]]) -> None:
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                for sql, params in statements:
                    self._conn.execute(sql, params)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, tuple(params)).fetchall()]

    def one(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- runs ----------------------------------------------------------
    def create_run(self, run_id: str, scenario: str, config: dict, started_at: float) -> None:
        self.execute(
            "INSERT INTO runs(run_id, scenario, config_json, state, started_at) VALUES (?,?,?,?,?)",
            (run_id, scenario, json.dumps(config), "STARTING", started_at),
        )

    def update_run_state(self, run_id: str, state: str) -> None:
        self.execute("UPDATE runs SET state=? WHERE run_id=?", (state, run_id))

    def finish_run(self, run_id: str, state: str, ended_at: float, summary: dict[str, Any], note: str | None = None) -> None:
        cols = (
            "generated", "accepted", "rejected_validation", "dropped", "persisted", "unpersisted",
            "peak_queue_depth", "avg_throughput", "peak_throughput", "files", "s3_bytes",
            "uncompressed_bytes", "avg_upload_ms", "failures",
            "processor_failed", "processor_batch_ms", "processor_rtt_ms",
        )
        sets = ", ".join(f"{c}=?" for c in cols)
        started = self.one("SELECT started_at FROM runs WHERE run_id=?", (run_id,))
        dur = ended_at - started["started_at"] if started else 0
        self.execute(
            f"UPDATE runs SET state=?, ended_at=?, duration_s=?, note=?, {sets} WHERE run_id=?",
            (state, ended_at, dur, note, *[summary.get(c, 0) for c in cols], run_id),
        )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        r = self.one("SELECT * FROM runs WHERE run_id=?", (run_id,))
        if r:
            r["config"] = json.loads(r.pop("config_json"))
        return r

    def list_runs(self, search: str = "", state: str = "", sort: str = "started_at", desc: bool = True,
                  limit: int = 100) -> list[dict[str, Any]]:
        allowed = {"started_at", "duration_s", "generated", "persisted", "files", "s3_bytes", "avg_throughput", "scenario"}
        sort = sort if sort in allowed else "started_at"
        where, params = [], []
        if search:
            where.append("(run_id LIKE ? OR scenario LIKE ?)")
            params += [f"%{search}%", f"%{search}%"]
        if state:
            where.append("state = ?")
            params.append(state)
        sql = "SELECT * FROM runs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {sort} {'DESC' if desc else 'ASC'} LIMIT ?"
        params.append(limit)
        rows = self.query(sql, params)
        for r in rows:
            r["config"] = json.loads(r.pop("config_json"))
        return rows

    def mark_interrupted(self, now: float) -> list[str]:
        """Runs left in a non-terminal state by a crash become INTERRUPTED."""
        rows = self.query(
            "SELECT run_id FROM runs WHERE state IN ('STARTING','RUNNING','PAUSING','PAUSED','STOPPING','FLUSHING')"
        )
        for r in rows:
            self.execute(
                "UPDATE runs SET state='INTERRUPTED', ended_at=?, note=? WHERE run_id=?",
                (now, "Backend restarted while this run was active; counters are not final.", r["run_id"]),
            )
        return [r["run_id"] for r in rows]

    # --- metrics -------------------------------------------------------
    def add_metric_sample(self, run_id: str, t: float, payload: dict, max_rows: int) -> None:
        self.execute("INSERT INTO run_metrics(run_id,t,payload) VALUES (?,?,?)", (run_id, t, json.dumps(payload)))
        cnt = self.one("SELECT COUNT(*) c FROM run_metrics WHERE run_id=?", (run_id,))["c"]
        if cnt > max_rows:
            # bounded retention: drop every other oldest row (keeps coverage of whole run)
            self.execute(
                "DELETE FROM run_metrics WHERE id IN (SELECT id FROM run_metrics WHERE run_id=? "
                "ORDER BY id LIMIT ?) AND id % 2 = 0",
                (run_id, cnt // 2),
            )

    def metric_samples(self, run_id: str) -> list[dict[str, Any]]:
        rows = self.query("SELECT t,payload FROM run_metrics WHERE run_id=? ORDER BY t", (run_id,))
        return [{"t": r["t"], **json.loads(r["payload"])} for r in rows]

    # --- events --------------------------------------------------------
    def add_event(self, run_id: str | None, ts: float, component: str, severity: str, message: str,
                  details: dict | None, max_rows: int) -> None:
        self.execute(
            "INSERT INTO pipeline_events(run_id,ts,component,severity,message,details) VALUES (?,?,?,?,?,?)",
            (run_id, ts, component, severity, message, json.dumps(details) if details else None),
        )
        if run_id:
            row = self.one("SELECT COUNT(*) c FROM pipeline_events WHERE run_id=?", (run_id,))
            if row and row["c"] > max_rows:
                self.execute(
                    "DELETE FROM pipeline_events WHERE id IN (SELECT id FROM pipeline_events WHERE run_id=? "
                    "AND severity='INFO' ORDER BY id LIMIT ?)",
                    (run_id, row["c"] - max_rows),
                )

    def list_events(self, run_id: str | None, component: str = "", severity: str = "", search: str = "",
                    limit: int = 200) -> list[dict[str, Any]]:
        where, params = [], []
        if run_id:
            where.append("run_id=?")
            params.append(run_id)
        if component:
            where.append("component=?")
            params.append(component)
        if severity:
            where.append("severity=?")
            params.append(severity)
        if search:
            where.append("message LIKE ?")
            params.append(f"%{search}%")
        sql = "SELECT * FROM pipeline_events"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        rows = self.query(sql, params)
        for r in rows:
            r["details"] = json.loads(r["details"]) if r["details"] else None
        return rows

    # --- chunks / objects ---------------------------------------------
    def upsert_chunk(self, c: dict[str, Any]) -> None:
        self.execute(
            """INSERT INTO chunks(chunk_id,run_id,partition_id,record_count,status,seal_reason,created_at,
                   uncompressed_bytes,parquet_bytes,serialize_ms,updated_at,error)
               VALUES (:chunk_id,:run_id,:partition_id,:record_count,:status,:seal_reason,:created_at,
                   :uncompressed_bytes,:parquet_bytes,:serialize_ms,:updated_at,:error)
               ON CONFLICT(chunk_id) DO UPDATE SET status=excluded.status, uncompressed_bytes=excluded.uncompressed_bytes,
                   parquet_bytes=excluded.parquet_bytes, serialize_ms=excluded.serialize_ms,
                   updated_at=excluded.updated_at, error=excluded.error""",
            c,
        )

    def record_persisted(self, obj: dict[str, Any], chunk_update: dict[str, Any]) -> None:
        """Atomically record the S3 object AND mark the chunk PERSISTED."""
        self.transaction([
            (
                """INSERT OR REPLACE INTO s3_objects(object_id,run_id,chunk_id,partition_id,bucket,object_key,
                       size_bytes,record_count,compression,uploaded_at,upload_ms,attempts,status)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'PERSISTED')""",
                (obj["object_id"], obj["run_id"], obj["chunk_id"], obj["partition_id"], obj["bucket"],
                 obj["object_key"], obj["size_bytes"], obj["record_count"], obj["compression"],
                 obj["uploaded_at"], obj["upload_ms"], obj["attempts"]),
            ),
            (
                "UPDATE chunks SET status='PERSISTED', parquet_bytes=?, updated_at=?, error=NULL WHERE chunk_id=?",
                (chunk_update["parquet_bytes"], chunk_update["updated_at"], obj["chunk_id"]),
            ),
        ])

    def list_objects(self, run_id: str = "", partition: int | None = None, search: str = "",
                     limit: int = 100, offset: int = 0) -> tuple[list[dict[str, Any]], int]:
        where, params = [], []
        if run_id:
            where.append("run_id=?")
            params.append(run_id)
        if partition is not None:
            where.append("partition_id=?")
            params.append(partition)
        if search:
            where.append("(object_key LIKE ? OR chunk_id LIKE ?)")
            params += [f"%{search}%", f"%{search}%"]
        w = (" WHERE " + " AND ".join(where)) if where else ""
        total = self.one(f"SELECT COUNT(*) c FROM s3_objects{w}", params)["c"]
        rows = self.query(
            f"SELECT * FROM s3_objects{w} ORDER BY uploaded_at DESC LIMIT ? OFFSET ?", [*params, limit, offset]
        )
        return rows, total

    def get_object(self, object_id: str) -> dict[str, Any] | None:
        return self.one("SELECT * FROM s3_objects WHERE object_id=?", (object_id,))

    def delete_object_row(self, object_id: str) -> None:
        self.execute("DELETE FROM s3_objects WHERE object_id=?", (object_id,))

    def chunks_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM chunks WHERE run_id=? ORDER BY created_at", (run_id,))

    # --- rejected ------------------------------------------------------
    def add_rejected(self, run_id: str, event_id: str | None, ts: float, reason: str, field: str | None,
                     max_rows: int) -> bool:
        cnt = self.one("SELECT COUNT(*) c FROM rejected_records WHERE run_id=?", (run_id,))["c"]
        if cnt >= max_rows:
            return False
        self.execute(
            "INSERT INTO rejected_records(run_id,event_id,ts,reason,field) VALUES (?,?,?,?,?)",
            (run_id, event_id, ts, reason, field),
        )
        return True

    def list_rejected(self, run_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return self.query("SELECT * FROM rejected_records WHERE run_id=? ORDER BY id DESC LIMIT ?", (run_id, limit))

    # --- usage ---------------------------------------------------------
    def usage_summary(self) -> dict[str, Any]:
        r = self.one(
            "SELECT COUNT(*) runs, COALESCE(SUM(generated),0) generated FROM runs"
        ) or {}
        o = self.one(
            "SELECT COUNT(*) objects, COALESCE(SUM(size_bytes),0) bytes, COALESCE(SUM(record_count),0) records FROM s3_objects"
        ) or {}
        return {**r, **o}

    def prune_old_runs(self, keep: int) -> list[str]:
        rows = self.query(
            "SELECT run_id FROM runs WHERE state NOT IN ('STARTING','RUNNING','PAUSING','PAUSED','STOPPING','FLUSHING') "
            "ORDER BY started_at DESC LIMIT -1 OFFSET ?", (keep,)
        )
        ids = [r["run_id"] for r in rows]
        for i in ids:
            self.execute("DELETE FROM runs WHERE run_id=?", (i,))
        return ids
