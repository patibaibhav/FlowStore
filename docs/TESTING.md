# Testing

## Automated suites
| Suite | Command | Needs AWS? |
|---|---|---|
| Backend unit + integration | `cd backend && python -m pytest tests` | No — S3 is mocked with `moto` (`mock_aws`) and botocore `Stubber` |
| Real-AWS roundtrip (opt-in) | `AWS_INTEGRATION=1 S3_BUCKET_NAME=<b> python -m pytest tests/test_real_aws.py` | **Yes** (skipped by default) |
| Frontend component tests | `cd frontend && npm test` | No (fetch + WebSocket are faked) |
| End-to-end (Playwright) | start the stack, then `cd frontend && npx playwright test` | Uses whatever S3 the backend points at (emulator locally) |
| Local load test | `cd backend && python scripts/load_test.py 12000 8` | No (mocked S3), bounded to ≤ 60 s |

Backend coverage: generator reproducibility, validation reasons, queue policies (block / drop-newest / drop-oldest, abort while blocked), partition buffers (size/age), Parquet schema round-trip for every codec, S3 classification, retry-then-succeed, retry exhaustion, non-retryable fail-fast, key safety, health checks, state-machine validity, record conservation under every policy, invalid-record rejection, "failed upload is never persisted", force-cancel accounting, restart → `INTERRUPTED`, reconcile (S3 object missing from metadata / metadata missing from S3), storage cap, REST flow (start, single active run, pause really stops generation, resume, stop, objects, inspect, preview, download, delete confirmation, admin token, compare, 424 when bucket is missing), WebSocket hello.

Frontend coverage: control enable/disable per run state, preset → posted config, stale telemetry ignored, live metric rendering, all seven stages rendered, range validation + backend warnings, Storage Explorer / Run History / Compare / Learning Center states, S3-not-ready banner.

## Manual verification checklist
1. `GET /api/storage/health` → all checks green (real S3: confirm the *write_probe* passes).
2. Run *Baseline*; confirm objects appear in the S3 console under `flowstore/runs/<run>/…`.
3. Run *Queue Saturation*: queue pinned at capacity, Generator `backpressured`, blocked-producer time > 0.
4. Run *Invalid Records*: rejected counter and reasons visible in Run History.
5. Pause → counters freeze; Resume → continue; Stop → `COMPLETED` with all records persisted; Force cancel → `CANCELLED` with `unpersisted > 0`.
6. Restart the backend during a run → run shows `INTERRUPTED`, history intact.
7. Revoke S3 write permission (or use a wrong bucket) → start is refused with an actionable message.
8. Resize the browser to 1366×768 and 1440×900 and check the pipeline remains readable.

## Verification status (honest summary)
* **Verified locally** (S3 emulator / mocks): backend suite, frontend suite, Playwright workflow, visual check of Live Lab / Storage Explorer / Compare at 1440 px, run pause/resume/stop through the real UI.
* **Not verified on real AWS**: real S3 latency/throttling behaviour, IAM role credential discovery inside the container, Learner Lab permission limits, instance-size benchmarks, the opt-in real-AWS test.
* **Docker (verified locally)**: both images build, both containers become healthy, and through Nginx on the published port: the index page loads, the storage health check passes, a WebSocket delivers `hello`/`telemetry`/`heartbeat`, and a run completes with its objects listed. This used the local S3 emulator, not real AWS.

## Known limitations
* Single active run; single backend worker (the engine and WebSocket hub are in-process).
* Python threads: very high rates (≈10–15k events/s locally) are limited by the GIL; the UI shows the *actual* rate.
* Run charts keep 240 live points; stored history is thinned to ≤ 3,000 samples/run.
* Pause does not pause uploads already in flight (by design, documented).
* Exactly-once delivery is **not** claimed; records in memory are lost on a crash (the run is marked `INTERRUPTED`).
* No HTTPS termination in the provided Nginx config (see the deployment guide).
* Only SQLite-recorded objects are listed by default; use *Verify with S3* to reconcile.
