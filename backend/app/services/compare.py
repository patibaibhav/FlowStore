"""Deterministic run comparison: measured deltas plus carefully-worded observations."""

from __future__ import annotations

from typing import Any

from app.models.db import Database


def run_metrics(db: Database, run: dict[str, Any]) -> dict[str, Any]:
    cfg = run["config"]
    files = run["files"] or 0
    samples = db.metric_samples(run["run_id"])
    peak_util = max((s["queue_depth"] / s["queue_capacity"] for s in samples if s.get("queue_capacity")), default=0.0)
    generated = run["generated"] or 0
    return {
        "run_id": run["run_id"], "scenario": run["scenario"], "state": run["state"],
        "generated": generated, "accepted": run["accepted"], "persisted": run["persisted"],
        "rejected_validation": run["rejected_validation"], "dropped": run["dropped"],
        "rejection_rate": (run["rejected_validation"] / generated) if generated else 0.0,
        "peak_queue_utilization": max(peak_util, (run["peak_queue_depth"] / cfg["queue_capacity"]) if cfg.get("queue_capacity") else 0),
        "avg_throughput": run["avg_throughput"], "peak_throughput": run["peak_throughput"],
        "files": files, "s3_bytes": run["s3_bytes"],
        "avg_file_size": (run["s3_bytes"] / files) if files else 0.0,
        "compression_ratio": (run["uncompressed_bytes"] / run["s3_bytes"]) if run["s3_bytes"] else 0.0,
        "avg_upload_ms": run["avg_upload_ms"], "duration_s": run["duration_s"] or 0.0, "failures": run["failures"],
        "processor_mode": cfg.get("processor_mode", "local"),
        "processor_batch_ms": run.get("processor_batch_ms", 0.0) or 0.0,
        "processor_rtt_ms": run.get("processor_rtt_ms", 0.0) or 0.0,
        "processor_failed": run.get("processor_failed", 0) or 0,
        "config": cfg,
    }


CONFIG_FIELDS = ("rate_per_second", "duration_seconds", "num_devices", "num_partitions", "queue_capacity",
                 "overflow_policy", "processor_rate_limit", "invalid_ratio", "chunk_size_records",
                 "chunk_max_age_seconds", "compression", "max_pending_uploads", "upload_concurrency",
                 "injected_upload_delay_ms", "burst_enabled", "processor_mode")


def explain(a: dict[str, Any], b: dict[str, Any]) -> list[dict[str, str]]:
    """Observations comparing run A and B. 'measured' items are facts; 'hypothesis' items are not proven."""
    out: list[dict[str, str]] = []

    def add(kind: str, text: str) -> None:
        out.append({"kind": kind, "text": text})

    def pct(x: float, y: float) -> str:
        return f"{(y - x) / x * 100:+.0f}%" if x else "n/a"

    changed = [f for f in CONFIG_FIELDS if a["config"].get(f) != b["config"].get(f)]
    add("measured", "Configuration differences: " + (", ".join(f"{f} ({a['config'].get(f)} → {b['config'].get(f)})" for f in changed) or "none"))
    if a["files"] != b["files"]:
        add("measured", f"Parquet files: A={a['files']}, B={b['files']} ({pct(a['files'], b['files'])}); "
                        f"average file size A={a['avg_file_size']:,.0f} B, B={b['avg_file_size']:,.0f} B.")
        if "chunk_size_records" in changed:
            lo, hi = (a, b) if a["config"]["chunk_size_records"] < b["config"]["chunk_size_records"] else (b, a)
            add("hypothesis", f"The run with the smaller chunk threshold (chunk_size_records={lo['config']['chunk_size_records']}) "
                              f"produced {'more' if lo['files'] >= hi['files'] else 'fewer'} objects. A smaller threshold can contribute "
                              "to more objects and more S3 request overhead; this run pair does not prove it was the only cause.")
    if a["avg_upload_ms"] and b["avg_upload_ms"] and abs(a["avg_upload_ms"] - b["avg_upload_ms"]) > 1:
        add("measured", f"Average upload latency: A={a['avg_upload_ms']:.0f} ms, B={b['avg_upload_ms']:.0f} ms.")
        if "injected_upload_delay_ms" in changed:
            add("note", "An artificial upload delay differs between runs; that is an experiment condition, not AWS latency.")
    if abs(a["rejection_rate"] - b["rejection_rate"]) > 0.001:
        add("measured", f"Validation rejection rate: A={a['rejection_rate']:.1%}, B={b['rejection_rate']:.1%}; "
                        f"persisted A={a['persisted']:,}, B={b['persisted']:,}.")
    if abs(a["peak_queue_utilization"] - b["peak_queue_utilization"]) > 0.05:
        add("measured", f"Peak queue utilization: A={a['peak_queue_utilization']:.0%}, B={b['peak_queue_utilization']:.0%}.")
        add("hypothesis", "Higher peak utilization indicates the consumer side was slower relative to the producer; "
                          "check processor_rate_limit, upload settings and queue_capacity differences above.")
    if a["processor_mode"] != b["processor_mode"] or a["processor_rtt_ms"] or b["processor_rtt_ms"]:
        add("measured", f"Processor time per batch: A={a['processor_batch_ms']:.1f} ms ({a['processor_mode']}), "
                        f"B={b['processor_batch_ms']:.1f} ms ({b['processor_mode']})."
                        + (f" Measured Lambda round trip: A={a['processor_rtt_ms']:.1f} ms, B={b['processor_rtt_ms']:.1f} ms."
                           if a["processor_rtt_ms"] or b["processor_rtt_ms"] else ""))
        if "processor_mode" in changed:
            add("hypothesis", "The processing stage runs on AWS Lambda in one run and in-process in the other. Each Lambda batch "
                              "pays a network round trip (network + invoke overhead + time inside the function), which adds "
                              "latency and lowers the throughput one processor thread can sustain; this run pair does not "
                              "prove it was the only cause of any difference in throughput or queue use.")
    if a["dropped"] != b["dropped"]:
        add("measured", f"Records lost to the overflow policy: A={a['dropped']:,}, B={b['dropped']:,}.")
    if a["processor_failed"] != b["processor_failed"]:
        add("measured", f"Records lost because the processor failed: A={a['processor_failed']:,}, B={b['processor_failed']:,}.")
    if a["compression_ratio"] and b["compression_ratio"] and abs(a["compression_ratio"] - b["compression_ratio"]) > 0.05:
        add("measured", f"Compression ratio (in-memory columnar bytes ÷ file bytes): A={a['compression_ratio']:.2f}×, B={b['compression_ratio']:.2f}×.")
    return out


def compare(db: Database, run_ids: list[str]) -> dict[str, Any]:
    runs = []
    for rid in run_ids:
        r = db.get_run(rid)
        if r is None:
            raise KeyError(rid)
        runs.append(r)
    metrics = [run_metrics(db, r) for r in runs]
    series = {m["run_id"]: db.metric_samples(m["run_id"]) for m in metrics}
    result: dict[str, Any] = {"runs": metrics, "series": series}
    if len(metrics) >= 2:
        result["observations"] = explain(metrics[0], metrics[1])
        result["delta_b_minus_a"] = {k: metrics[1][k] - metrics[0][k] for k in
                                     ("generated", "accepted", "persisted", "files", "s3_bytes", "avg_file_size",
                                      "avg_upload_ms", "avg_throughput", "duration_s", "rejection_rate",
                                      "peak_queue_utilization", "compression_ratio",
                                      "processor_batch_ms", "processor_rtt_ms")}
    return result
