"""Heuristic bottleneck inspector. Diagnoses are hypotheses from measured metrics, not proofs."""

from __future__ import annotations

from typing import Any


def diagnose(snap: dict[str, Any]) -> dict[str, Any]:
    q = snap["queue"]
    w = snap["writer"]
    rates = snap["rates"]
    cfg = snap.get("config_summary", {})
    util = q["utilization"]
    evidence: list[str] = []
    pending_full = w["pending_uploads"] >= max(1, cfg.get("max_pending_uploads", 1))
    gen = rates["generation"]
    proc = rates["processing"]

    stage, conf = "none", "low"
    if snap["state"] in ("IDLE", "COMPLETED", "FAILED", "CANCELLED", "INTERRUPTED"):
        return {"stage": "none", "confidence": "n/a", "summary": "No active run.", "evidence": [], "heuristic": True}

    if q.get("dropped_newest", 0) + q.get("evicted_oldest", 0) > 0 and util > 0.8:
        evidence.append(f"{q['dropped_newest'] + q['evicted_oldest']} records lost to overflow policy.")
    if util >= 0.8 or q.get("blocked_now"):
        evidence.append(f"Queue utilization {util:.0%}" + (" and producer is blocked." if q.get("blocked_now") else "."))
        if pending_full:
            stage, conf = "Parquet Writer / Amazon S3", "medium"
            evidence.append(f"Pending uploads at limit ({w['pending_uploads']}); chunk builder is stalled behind storage.")
        else:
            stage, conf = "Processor", "medium"
            evidence.append(f"Processing throughput {proc:.0f}/s is below generation {gen:.0f}/s.")
            if cfg.get("processor_rate_limit"):
                evidence.append(f"Processor is throttled to {cfg['processor_rate_limit']}/s by the experiment.")
                conf = "high"
    elif pending_full and w["pending_uploads"] > 0:
        stage, conf = "Parquet Writer / Amazon S3", "low"
        evidence.append("Pending uploads are at the configured limit but the queue is not yet saturated.")
    else:
        evidence.append(f"Queue utilization {util:.0%}; no stage appears saturated.")

    summary = ("Likely limiting stage: " + stage + ".") if stage != "none" else "No clear bottleneck detected."
    return {"stage": stage, "confidence": conf, "summary": summary, "evidence": evidence, "heuristic": True,
            "note": "Heuristic diagnosis from measured metrics; not a proven root cause."}
