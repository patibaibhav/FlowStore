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

Not used (by design): Kinesis, Redis, Kafka, RDS, Lambda, Kubernetes. Amazon S3 is the **only** destination for Parquet files; there is no local/EBS storage selector and no silent fallback to local files. SQLite (on the EC2/EBS disk) stores only metadata, run history and sampled telemetry.

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

AWS credentials are **never** read from or sent to the browser; boto3 uses its default provider chain (IAM role on EC2).

## Doing an experiment
1. **Experiment** tab → pick a preset (Baseline, High Load, Slow Processing, Queue Saturation, Invalid Records, Small/Large Chunks, Storage Bottleneck, Burst) and tweak parameters. The estimate panel shows planned records, S3 PUTs and warnings.
2. **Start & watch live.** Click any pipeline node for its inspector. Pause/Resume/Stop (graceful) or Force cancel (discards unpersisted records and reports how many).
3. **Storage Explorer** → select an object → schema, row groups, codec, sample rows (ranged S3 reads), download, delete (with confirmation).
4. **Run History / Compare** → compare two finished runs; observations separate *measured* facts from *hypotheses*.
5. **Learning Center** → four guided experiments, glossary, exact metric definitions.

See [docs/EXPERIMENT_GUIDE.md](docs/EXPERIMENT_GUIDE.md).

## Tests
```bash
cd backend && python -m pytest tests            # 29 tests, no AWS needed (moto + Stubber)
cd frontend && npm test                         # 12 component tests
cd frontend && npx playwright test              # e2e; needs the stack running (see docs/TESTING.md)
```

## Documentation
[ARCHITECTURE](docs/ARCHITECTURE.md) · [AWS Learner Lab deployment](docs/AWS_LEARNER_LAB_DEPLOYMENT.md) · [Cost control](docs/COST_CONTROL.md) · [Experiment guide](docs/EXPERIMENT_GUIDE.md) · [Testing](docs/TESTING.md)

## Known limitations
See the end of [docs/TESTING.md](docs/TESTING.md): real-AWS behaviour has not been verified in this repository's test runs (only an S3 emulator and mocks), and a few items are intentionally out of scope.
