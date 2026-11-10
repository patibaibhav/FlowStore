# FlowStore 2.0 — Time-Series Storage & Ingestion Pipeline Lab

An interactive virtual laboratory for **Software System Design**. It runs a *real* concurrent ingestion pipeline and persists *real* Apache Parquet files to **Amazon S3**:

```
Generator → Queue → Processor → Buffers → Chunk Builder → Parquet Writer → Amazon S3
```

Every number on screen is measured from the running pipeline (queue depth, blocked-producer time, upload latency, bytes…). Nothing is animated from made-up values.

## What it demonstrates
Stream ingestion · bounded queues & backpressure (block / drop-newest / drop-oldest) · validation · partition buffering · chunking by records/bytes/age · columnar Parquet (PyArrow, snappy/zstd/gzip) · S3 object storage with retries, backoff + jitter · persistence latency · run comparison · guided experiments.

## Stack
| Layer | Technology |
|---|---|
| Frontend | React 19, TypeScript, Vite, Tailwind CSS 4, React Flow (`@xyflow/react`), Recharts, Lucide, WebSocket |
| Backend | Python 3.12+, FastAPI, Uvicorn, Pydantic, PyArrow, boto3, SQLite (`sqlite3`, WAL, migrations) |
| Infra | Docker Compose, Nginx (static + reverse proxy + WebSocket upgrade), EC2, S3 |

Not used (by design): Kinesis, Redis, Kafka, RDS, Kubernetes. **AWS Lambda is optional**: by default the processing stage runs in-process; if you deploy the small function in `lambda/` and set `LAMBDA_PROCESSOR_NAME`, the Experiment page offers "Processor runs on: AWS Lambda" per run (see *Optional: Lambda processor* below). Amazon S3 is the **only** destination for Parquet files; there is no local/EBS storage selector and no silent fallback to local files. SQLite (on the EC2/EBS disk) stores only metadata, run history and sampled telemetry. **CloudWatch** application metrics are optional and off by default.

## Repository layout
```
backend/app/
  api/          REST routes            pipeline/   engine, queue, events/validator, parquet writer
  core/         settings (env)         storage/    S3 adapter (health, upload, inspect, safety)
  models/       SQLite store+migrations services/  run manager, reconcile/usage, comparison
  schemas/      run config + presets   telemetry/  event log, WebSocket hub, bottleneck inspector
backend/tests/  pytest suite (moto-mocked S3, botocore Stubber)
frontend/src/   pages, pipeline (React Flow), charts, hooks (live store), learning content
frontend/e2e/   Playwright end-to-end test
docs/           ARCHITECTURE, AWS_LEARNER_LAB_DEPLOYMENT, COST_CONTROL, EXPERIMENT_GUIDE, TESTING
docker-compose.yml  .env.example
```

## Run locally

### Option A — against a real S3 bucket
Prerequisites: Python 3.12+, Node 20+, AWS credentials available to boto3 (env vars / `aws configure`) and a **private** bucket you created.
```bash
cd backend && pip install -r requirements-dev.txt
# PowerShell: $env:S3_BUCKET_NAME="your-bucket"; $env:AWS_REGION="us-east-1"
S3_BUCKET_NAME=your-bucket AWS_REGION=us-east-1 uvicorn app.main:app --port 8000
```
```bash
cd frontend && npm install && npm run dev      # http://localhost:5173 (proxies /api and /ws to :8000)
```

### Option B — no AWS (development emulator, NOT S3)
Uses the `moto` S3-compatible emulator so you can try everything offline. The code path (boto3, retries, ranged reads) is identical, but it is an emulator, not Amazon S3.
```bash
cd backend && python scripts/dev_s3.py                       # terminal 1
S3_ENDPOINT_URL=http://127.0.0.1:5555 S3_BUCKET_NAME=flowstore-dev \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test uvicorn app.main:app --port 8000   # terminal 2
cd frontend && npm run dev                                   # terminal 3
```

With the emulator the **processor always runs in-process** and CloudWatch is off: the "AWS Lambda" option is disabled in the UI ("Lambda is not available with the S3 emulator"). To try Lambda mode without AWS add `LAMBDA_EMULATE_LOCAL=true` (the handler then runs inside the backend; it is labelled *emulated* and its timings are in-process call time, not real Lambda latency).

### Optional: Lambda processor and CloudWatch
* **Lambda processor** — `python lambda/build.py` writes `lambda/dist/flowstore-processor.zip` (the handler plus the *same* validation rules the in-process processor uses). Create a Python 3.12 function from it in the Lambda console (handler `lambda_handler.lambda_handler`, 256 MB, 10 s), put its name in `.env` as `LAMBDA_PROCESSOR_NAME`, restart the backend, then choose **Processor runs on → AWS Lambda** on the Experiment page. Each batch is sent to Lambda and back; the measured round trip (handler time + network/invoke overhead), cold starts and failures are shown on the Processor node and in Compare. Lambda mode is refused (HTTP 424) if the function is unreachable — it never falls back to in-process silently. Rebuild and re-upload when `backend/app/pipeline/events.py` changes (the app warns if the function's rules differ).
* **CloudWatch** — set `CLOUDWATCH_ENABLED=true` to publish up to 10 custom metrics (queue depth, rates, producer blocked ms, upload latency, retries, Lambda round trip) while a run is active. Lambda's own metrics/logs and EC2's basic metrics appear in CloudWatch automatically. See [docs/AWS_LEARNER_LAB_DEPLOYMENT.md](docs/AWS_LEARNER_LAB_DEPLOYMENT.md).

### Docker Compose (what you deploy on EC2)
```bash
cp .env.example .env        # set S3_BUCKET_NAME, AWS_REGION
docker compose up -d --build
# open http://<host>/    (OpenAPI docs: /docs)
```

## Configuration (environment variables)
| Variable | Default | Meaning |
|---|---|---|
| `S3_BUCKET_NAME` | *(required)* | Private bucket for Parquet objects |
| `AWS_REGION` | `us-east-1` | Bucket region |
| `S3_PREFIX` | `flowstore` | Key prefix; the app only touches `{prefix}/runs/…` |
| `S3_MAX_STORAGE_MB` | `512` | Upload cap (cost safety) |
| `S3_UPLOAD_CONCURRENCY` | `2` | Baseline upload parallelism (run config may go up to 4×) |
| `S3_UPLOAD_TIMEOUT_SECONDS` | `30` | Per-request timeout |
| `MAX_RUN_SECONDS` / `MAX_RATE_PER_SECOND` / `MAX_RECORDS_PER_RUN` | `300` / `20000` / `500000` | Run caps |
| `TELEMETRY_HZ` | `5` | WebSocket updates per second (1–20) |
| `DATA_DIR` | `./data` | SQLite + temporary staging |
| `ADMIN_TOKEN` | *(empty)* | If set, deletes need `X-Admin-Token` |
| `S3_ENDPOINT_URL` | *(empty)* | **Development emulator only** |
| `PRICE_S3_PUT_PER_1000`, `PRICE_S3_GB_MONTH` | `0.005`, `0.023` | Assumptions for the *estimated* cost panel |
| `LAMBDA_PROCESSOR_NAME` | *(empty)* | Name of the Lambda function for "Processor runs on: AWS Lambda". Empty = option disabled |
| `LAMBDA_TIMEOUT_SECONDS`, `LAMBDA_MAX_RETRIES` | `15`, `3` | Per-call timeout and bounded retries (exponential backoff + jitter) |
| `LAMBDA_EMULATE_LOCAL` | `false` | **Development only**: run the handler inside the backend (no AWS), labelled "emulated" |
| `CLOUDWATCH_ENABLED` | `false` | Publish up to 10 app metrics to CloudWatch (never used with the S3 emulator) |
| `CLOUDWATCH_NAMESPACE`, `CLOUDWATCH_INTERVAL_SECONDS` | `FlowStore`, `10` | Metric namespace and publish interval while a run is active |

AWS credentials are **never** read from or sent to the browser; boto3 uses its default provider chain (IAM role on EC2).

## Doing an experiment
1. **Experiment** tab → pick a preset (Baseline, High Load, Slow Processing, Queue Saturation, Invalid Records, Small/Large Chunks, Storage Bottleneck, Burst) and tweak parameters. The estimate panel shows planned records, S3 PUTs and warnings.
2. **Start & watch live.** Click any pipeline node for its inspector. Pause/Resume/Stop (graceful) or Force cancel (discards unpersisted records and reports how many).
3. **Storage Explorer** → select an object → schema, row groups, codec, sample rows (ranged S3 reads), download, delete (with confirmation).
4. **Run History / Compare** → compare two finished runs; observations separate *measured* facts from *hypotheses*.
5. **Learning Center** → five guided experiments, glossary, exact metric definitions.

See [docs/EXPERIMENT_GUIDE.md](docs/EXPERIMENT_GUIDE.md).

## Tests
```bash
cd backend && python -m pytest tests            # 50 tests, no AWS needed (moto + Stubber)
cd frontend && npm test                         # 12 component tests
cd frontend && npx playwright test              # e2e; needs the stack running (see docs/TESTING.md)
```

## Documentation
[ARCHITECTURE](docs/ARCHITECTURE.md) · [AWS Learner Lab deployment](docs/AWS_LEARNER_LAB_DEPLOYMENT.md) · [Cost control](docs/COST_CONTROL.md) · [Experiment guide](docs/EXPERIMENT_GUIDE.md) · [Testing](docs/TESTING.md)

## Known limitations
See the end of [docs/TESTING.md](docs/TESTING.md): real-AWS behaviour has not been verified in this repository's test runs (only an S3 emulator and mocks), and a few items are intentionally out of scope.
