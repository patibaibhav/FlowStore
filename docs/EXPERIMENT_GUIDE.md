# Experiment guide

The same four experiments are available interactively in **Learning Center → Guided experiments** (each has a *Load preset* button). Every experiment: objective → initial configuration → steps → observations → explanation → reflection question.

## Experiment A — Understanding backpressure
**Objective.** See how a slow consumer plus a bounded queue forces the producer to slow down.
**Initial configuration.** Preset *Queue Saturation* (3,000 ev/s requested, processor capacity 800/s, queue 200, policy *block*).
**Steps.** Start → watch queue utilization → compare *Ingestion vs processing* → click the **Queue** node.
**Expected observations.** Queue pinned near capacity; Generator node `backpressured`; actual generation rate converges toward ~800/s; blocked-producer time grows; the Bottleneck Inspector names the *Processor* (heuristic).
**Explanation.** With *block* nothing is lost, so end-to-end speed equals the slowest stage. Switch the policy to *drop newest* and rerun: the producer keeps its rate but records are dropped.
**Reflect.** What would you give up and gain by dropping instead of blocking?

## Experiment B — Chunk size trade-offs
**Objective.** Quantify file count, file size and upload overhead.
**Steps.** Run *Small Chunks* (100 records/chunk) then *Large Chunks* (5,000) → **Compare**.
**Expected observations.** Small: many small objects, lower compression ratio. Large: few large objects, higher ratio. (Reference numbers from a local emulator run with 8 s at 1,000 ev/s: 82 files / 8.7 KB average / 1.48× vs 4 files / 116.5 KB average / 2.15×.) Upload latency per object depends on size and your network.
**Explanation.** Each object costs one PUT and fixed overhead; small groups of rows compress worse. Larger chunks keep data in memory longer (more loss on a crash, higher flush latency).
**Reflect.** Which chunk size would you pick if dashboards must show data within 5 seconds?

## Experiment C — Validation quality
**Steps.** Run *Baseline* (0 % invalid) and *Invalid Records* (30 %); open Run History → rejected records.
**Expected.** Rejection reasons (invalid device id, bad timestamp, non-numeric/NaN value, unknown sensor, bad quality flag); `persisted = generated − rejected`; invalid rows never appear in Parquet.
**Reflect.** Should rejected events be dead-lettered instead of dropped? What would that cost?

## Experiment D — Cloud persistence
**Steps.** Run *Baseline* → watch Amazon S3 entries in the Event Log → Storage Explorer → select an object → inspect schema/row groups/codec/sample → download it.
**Expected.** Keys `flowstore/runs/<run>/partitions/<p>/chunks/<chunk>.parquet`; record counts add up to the *Persisted* counter; the inspector reports bytes read from S3 (footer + one row group, not the whole object).
**Reflect.** Why must S3 objects be written whole rather than appended?

## Other presets worth trying
* **Storage Bottleneck** — upload concurrency 1 plus an *injected* 600 ms delay (an experiment condition, **not** AWS latency). Pending uploads fill, the chunk builder stalls, then the queue fills.
* **Burst Traffic** — 5× spikes every 10 s; watch transient queue growth and recovery.
* **High Ingestion Load / Slow Processing** — with *drop newest* you can read exactly how many records the policy discarded.
