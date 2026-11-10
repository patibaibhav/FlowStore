# Deploying FlowStore 2.0 on AWS Academy Learner Lab

> **Status of this guide.** The application, Docker images and Nginx config were verified locally (see [TESTING.md](TESTING.md)). The steps below on real AWS Academy resources have **not** been executed by the author of this repository; Learner Lab permissions vary by course. If a step is blocked, report the exact error instead of working around it. Nothing here creates AWS resources automatically — you create them by hand.

## 0. Architecture
`Browser → Nginx (port 80) → React static + FastAPI (internal :8000) → pipeline → PyArrow → S3`. SQLite and temporary Parquet staging live on the instance's EBS volume (Docker volume `flowstore-data`). One EC2 instance, one private S3 bucket. No Kinesis, RDS, load balancer, NAT gateway or Kubernetes. Optional: the processing stage can call an AWS Lambda function (section 8b) and the app can publish metrics to CloudWatch.

## 1. Start the lab
1. AWS Academy → your course → *Modules → Learner Lab* → **Start Lab**. Wait for the AWS indicator to turn green, then **AWS** to open the console.
2. Check the budget shown in the lab header before you continue, and note the region (usually `us-east-1`).

## 2. Create the private S3 bucket
Console → S3 → *Create bucket*:
* Name: globally unique, e.g. `flowstore-<yourname>-<random>`; same region as EC2.
* **Block all public access: ON** (default). Object Ownership: bucket owner enforced. Default encryption (SSE-S3): on.
* Do **not** enable versioning (it multiplies stored data and cost for a lab).
* Optional lifecycle rule (recommended): prefix `flowstore/runs/`, expire current objects after **3 days**, and abort incomplete multipart uploads after 1 day.

## 3. IAM permissions
Learner Lab normally gives EC2 instances the pre-created **`LabInstanceProfile`** (role `LabRole`). Attach it when launching the instance; you usually cannot create IAM roles. `LabRole` normally already allows S3. If you *can* create a role, use the least-privilege policy in [`iam-policy.json`](iam-policy.json) (replace `YOUR_BUCKET`).

Verify from the instance later with `GET /api/storage/health`; the UI shows each check (configuration, credentials, bucket access, write probe). If the write probe fails with AccessDenied, that is the exact Learner Lab limitation to report.

## 4. Launch EC2
Console → EC2 → *Launch instance*:
* AMI: Amazon Linux 2023 (or Ubuntu 22.04/24.04).
* Instance type: start with **t3.small (2 vCPU / 2 GB)** if available; **t3.medium** gives more headroom for >5,000 events/s runs. Learner Lab restricts the allowed types — pick the smallest one that works. *Measure, don't assume:* run the High Ingestion preset and watch `GET /api/system/status`/`docker stats`; if CPU is pegged at 100 % the achieved rate will be lower than the target (the UI shows the actual rate).
* Key pair: use the lab-provided `vockey` (or create one) if you need SSH.
* **Advanced → IAM instance profile: `LabInstanceProfile`.**
* Storage: 20 GB gp3 is plenty.
* Security group (inbound only): **TCP 80 from *your IP* (`My IP`)** and TCP 22 from *your IP* if you need SSH. Do not open 8000; it is not published anyway. Never open SQLite, Docker, or database ports.

## 5. Install Docker and deploy
```bash
# Amazon Linux 2023
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user && newgrp docker
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose && sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
```
Copy the repository to the instance (`git clone`, or `scp -r`), then:
```bash
cd FlowStore
cp .env.example .env
nano .env        # set AWS_REGION and S3_BUCKET_NAME; leave AWS_* keys unset (IAM role is used)
docker compose up -d --build
docker compose ps          # both services should be "healthy"
```
If the instance role's credentials are not reachable from inside the container (IMDSv2 hop limit), set the instance metadata option *HttpPutResponseHopLimit* to **2**:
`aws ec2 modify-instance-metadata-options --instance-id <id> --http-put-response-hop-limit 2 --http-tokens required`.

## 6. Verify
1. Open `http://<EC2 public IPv4>/`. Header should show **S3 verified** and **Live**.
2. *WebSockets*: the "Live" indicator plus moving numbers on a run confirm `/ws/telemetry` passes through Nginx. (`docker compose logs web` should show no 400/502 on `/ws/telemetry`.)
3. *Real uploads*: run **Baseline Ingestion**, then in the S3 console open `flowstore/runs/<run-id>/partitions/…` and confirm the `.parquet` objects exist with the sizes shown in Storage Explorer. In the app, **Storage Explorer → Verify with S3** compares metadata to a live listing.
4. Download an object from Storage Explorer and open it with any Parquet reader to confirm the schema.

## 7. Real-AWS integration test (explicit, opt-in)
```bash
cd backend && AWS_INTEGRATION=1 S3_BUCKET_NAME=<bucket> AWS_REGION=<region> python -m pytest tests/test_real_aws.py -v
```
It uploads one tiny Parquet file under `flowstore/_integration/`, reads it back, validates it and deletes it. Without `AWS_INTEGRATION=1` it is skipped.

## 8. HTTPS
The lab deployment serves plain HTTP on port 80 restricted to your IP. Learner Lab typically offers no custom domain/ACM path, so HTTPS is out of scope; options if you need it: a self-signed certificate on Nginx (browser warning), or Let's Encrypt with a domain you own. Do not send secrets over the HTTP endpoint (the app itself transmits none; only the optional `X-Admin-Token`).

## 8b. Optional: Lambda processor and CloudWatch
Both are off by default. To run the processing stage on AWS Lambda for selected runs and to see app metrics in CloudWatch, follow [LAMBDA_CLOUDWATCH_SETUP.md](LAMBDA_CLOUDWATCH_SETUP.md) (click-by-click). The function is created by hand and lives in your account independently of EC2; the backend only needs `LAMBDA_PROCESSOR_NAME` in `.env` and makes no AWS call for it at startup. The instance role needs `lambda:InvokeFunction` (and `cloudwatch:PutMetricData` for metrics): see [`iam-policy.json`](iam-policy.json); LabRole normally allows both.

## 9. Stop and clean up
* After each session: `docker compose down` (data volume survives) and **Stop** the EC2 instance. Also click **End Lab** in AWS Academy when finished.
* Stopped instances still bill for EBS and may incur public-IPv4 charges; S3 storage/requests continue regardless. See [COST_CONTROL.md](COST_CONTROL.md).
* Delete test data: Storage Explorer → *Delete run objects* (Run History) or `aws s3 rm s3://<bucket>/flowstore/runs/ --recursive`.
* When completely done: terminate the instance and delete the bucket.
