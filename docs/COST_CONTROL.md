# Cost control (AWS Academy credits)

Prices change and differ by region. **Check the current AWS pricing pages** for EC2, EBS, S3 and public IPv4 before relying on any figure. Numbers shown inside the app are *estimates* computed from editable assumptions (`PRICE_S3_PUT_PER_1000`, `PRICE_S3_GB_MONTH`); they are **not** your AWS billing balance, and the app has no billing integration.

## What costs money
| Resource | Charged when | Notes |
|---|---|---|
| **EC2 instance** | per second while *running* | The main cost. Stop it when not demonstrating. |
| **EBS volume** | while the volume exists, even if the instance is stopped | Keep it small (≈20 GB). |
| **Public IPv4 address** | AWS bills for public IPv4 addresses (in-use and idle) | A stopped instance releases its auto-assigned public IP; an Elastic IP keeps billing while allocated. Prefer the auto-assigned IP. |
| **S3 storage** | per GB-month | Parquet files from experiments are small, but they accumulate. |
| **S3 requests** | per PUT/GET/LIST | One PUT per Parquet chunk (+2 per run for the health probe). Tiny chunks = many PUTs. |
| Data transfer out | per GB to the internet | Downloads from Storage Explorer; negligible for a lab. |

**S3 storage and request charges continue even when EC2 is stopped.**

## Safeguards built into the app
1. One EC2 instance, no other managed services; nothing is auto-provisioned.
2. No background ingestion: data is generated only during an explicit run.
3. Caps (env-configurable): `MAX_RUN_SECONDS=300`, `MAX_RATE_PER_SECOND=20000`, `MAX_RECORDS_PER_RUN=500000`, `max_pending_uploads ≤ 32`, `S3_MAX_STORAGE_MB` (uploads stop at the cap).
4. The configuration screen warns about runs that will create many S3 objects and about very small chunks.
5. Storage Explorer lists from SQLite metadata (no S3 polling), paginates S3 listings, and previews Parquet via ranged reads — it never re-downloads whole files.
6. One-click cleanup: *Delete run objects* (Run History) and per-object delete, both confirmed and limited to the `flowstore/runs/` prefix.
7. A usage panel (Storage Explorer) shows objects, bytes, estimated PUTs and clearly-labelled estimated costs.
8. No continuous AWS load tests exist; automated tests use mocks/emulators. The real-AWS test is opt-in and uploads a few KB.

## Practical habits
* Prefer **chunk size ≥ 1,000 records** and 2–4 partitions: a 20 s Baseline run creates roughly 10–20 objects.
* Use the *Small Chunks* preset deliberately and briefly (it creates ~100+ objects); delete its objects afterwards.
* Add an S3 lifecycle rule (expire `flowstore/runs/` after 1–3 days) so forgotten data disappears. Console → bucket → Management → Lifecycle rules.
* After a session: `docker compose down`, **Stop** (not just disconnect) the instance, **End Lab**.
* Occasionally check *Billing → Cost Explorer* (or the Learner Lab budget header) yourself; the app cannot.

## Deleting test data safely
```bash
aws s3 ls s3://<bucket>/flowstore/runs/ --recursive --summarize | tail -3    # see what you have
aws s3 rm s3://<bucket>/flowstore/runs/<run-id>/ --recursive                  # one run
aws s3 rm s3://<bucket>/flowstore/runs/ --recursive                           # everything FlowStore created
```
Deleting objects does not remove run history rows; Storage Explorer's *Verify with S3* marks such objects `MISSING`.

## Optional Lambda processor and CloudWatch
* **Lambda** (only when you choose "Processor runs on: AWS Lambda"): about 40 invocations for a 10,000-record run (batches of up to 512, usually smaller at low rates). The always-free tier (1 M requests and 400,000 GB-seconds per month) covers this many times over. A cold start adds one slow call, not extra cost.
* **CloudWatch custom metrics**: the app publishes at most 10 metrics with **no dimensions**; the free tier includes 10 custom metrics, 10 alarms and 3 dashboards. Adding a per-run or per-preset dimension would multiply the metric count (for example 9 presets x 10 metrics = 90 billable metrics), so don't. PutMetricData calls are about 1 per 10 s only while a run is active.
* Lambda's own metrics and logs in CloudWatch are automatic; log storage is tiny (3 lines per call).
* Turn both off when not needed: leave `LAMBDA_PROCESSOR_NAME` empty and `CLOUDWATCH_ENABLED=false`.
