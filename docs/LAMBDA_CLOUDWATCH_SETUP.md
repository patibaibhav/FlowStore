# Lambda processor + CloudWatch: click-by-click setup (AWS Academy Learner Lab)

> **Status.** The code paths are tested with stubs, an S3 emulator and an in-process Lambda emulation (see [TESTING.md](TESTING.md)). The console steps below have **not** been executed against a real Learner Lab by the author; screens and permissions vary by course. If a step is blocked, note the exact error instead of working around it. Nothing here is required: without it the processor simply stays in-process.

## A. Build the deployment zip (on your own computer)
1. Open a terminal in the repository root (`FlowStore/`).
2. Run `python lambda/build.py`.
3. It writes `lambda/dist/flowstore-processor.zip` and prints a **validation code hash** (16 hex characters). Keep it for step B9.

## B. Create the Lambda function
1. AWS Academy → your course → **Modules → Learner Lab** → **Start Lab**. Wait for the AWS dot to turn **green**, then click **AWS** (opens the console). Check the region (top right) is the same as your EC2 instance and bucket (usually **us-east-1**).
2. In the search bar type **Lambda** and open it → **Create function**.
3. Choose **Author from scratch**. Function name: `flowstore-processor`. Runtime: **Python 3.12**. Architecture: **x86_64**.
4. Expand **Change default execution role** → **Use an existing role** → pick **LabRole**. (Learner Lab does not let you create roles.)
5. Click **Create function**.
6. **Code** tab → **Upload from** → **.zip file** → **Upload** → choose `flowstore-processor.zip` → **Save**.
7. Still on the **Code** tab, scroll to **Runtime settings** → **Edit** → **Handler** = `lambda_handler.lambda_handler` → **Save**.
8. **Configuration** tab → **General configuration** → **Edit** → **Memory** `256 MB`, **Timeout** `0 min 10 sec` → **Save**.
9. **Test** tab → **Create new event** → name `ping` → Event JSON:
   ```json
   {"events": [], "num_partitions": 4, "now": 0}
   ```
   → **Save** → **Test**. You should see *Execution result: succeeded* and a response containing `"code_hash"`. It must equal the hash printed in step A3 (if not, you uploaded an old zip).
10. Optional second test event `bad-device`:
    ```json
    {"events":[{"event_id":"e1","device_id":"???","timestamp":1,"sensor_type":"temperature","sensor_value":1.0,"quality_flag":"GOOD"}],"num_partitions":4,"now":1}
    ```
    Expected: `"rejected": [{"event_id": "e1", "reason": "invalid device identifier", ...}]`.

## C. Point the backend at it (EC2)
1. Get the new code onto the instance (for example `git pull` in the repository you cloned earlier).
2. `nano .env` and add:
   ```
   LAMBDA_PROCESSOR_NAME=flowstore-processor
   CLOUDWATCH_ENABLED=true
   ```
   `AWS_REGION` must be the region of the function. Save (Ctrl+O, Enter, Ctrl+X).
3. `docker compose up -d --build`, then `docker compose ps` (both services healthy).
4. Open the app → **Experiment**. **Processor runs on** now offers **AWS Lambda** (before the restart it is greyed out with the reason).
5. Select **AWS Lambda** → go to **Live Lab** → **Start**. At run start the backend makes a real pre-warm call; if it fails the run is **refused** with the exact reason (function not found, AccessDenied, timeout…). It never falls back to in-process silently.
6. During the run click the **Processor** node: you see *Lambda round trip*, *Inside Lambda (handler)*, *Network + invoke overhead*, *Cold starts* and invocation/error counts. These are measured per call.

If the run is refused with **AccessDenied**, the instance role (LabRole) is not allowed `lambda:InvokeFunction` in your course. Report it; you cannot change LabRole in Learner Lab.

## D. CloudWatch
Metrics from the app appear only after a run with `CLOUDWATCH_ENABLED=true` (allow 1-2 minutes). Use runs of 60 s or longer: CloudWatch aggregates per minute, so a 20 s run draws only one or two points.

**See the metrics**
1. Console → **CloudWatch** → **Metrics** → **All metrics**.
2. **FlowStore** (custom namespace): QueueDepth, GeneratedPerSec, ProcessedPerSec, PersistedPerSec, RejectedPerSec, DroppedPerSec, ProducerBlockedMs, UploadLatencyMs, UploadRetries, LambdaRttMs (Lambda mode only).
3. **AWS/Lambda → By Function Name → flowstore-processor**: Invocations, Duration, Errors, Throttles, ConcurrentExecutions. These are automatic (nothing to configure).
4. **AWS/EC2 → Per-Instance Metrics**: CPUUtilization, NetworkIn/Out, StatusCheckFailed (automatic, 5-minute resolution).

**Build a dashboard** (free tier: 3 dashboards)
1. **Dashboards → Create dashboard** → name `FlowStore-Lab` → **Create dashboard**.
2. **Add widget → Line → Metrics**. Add four widgets: (a) FlowStore: QueueDepth + ProducerBlockedMs, (b) FlowStore: GeneratedPerSec + ProcessedPerSec + PersistedPerSec, (c) FlowStore: LambdaRttMs plus AWS/Lambda Duration for `flowstore-processor`, (d) AWS/EC2: CPUUtilization of your instance. For each: select the metrics → **Create widget**.
3. Top right set the range to **1h** and click **Save dashboard**.

**Alarms** (free tier: 10 alarms; skip if your course blocks them)
1. **Alarms → All alarms → Create alarm → Select metric → EC2 → Per-Instance Metrics →** your instance **CPUUtilization** → Statistic *Average*, period *5 minutes* → **Next**.
2. Condition *Greater than* `80` → **Next** → under notifications click **Remove** (SNS is often unavailable in Learner Lab) → name `flowstore-ec2-cpu-high` → **Create alarm**.
3. Repeat for **Lambda → By Function Name → flowstore-processor → Errors**, Statistic *Sum*, period *1 minute*, *Greater than* `0`, name `flowstore-lambda-errors`.

**Lambda logs** — **CloudWatch → Log groups → `/aws/lambda/flowstore-processor`** → newest log stream. Each call writes START / END / REPORT lines; REPORT shows *Duration*, *Billed Duration* and, on a cold start, *Init Duration*.

## E. Check it works (about 5 minutes)
1. Run **Baseline Ingestion** with *In-process*, then again with *AWS Lambda* (duration 60 s for nice graphs).
2. Open **Compare** with both runs: *Processor time per batch* is a few ms in-process and tens of ms on Lambda; *Lambda round trip (avg)* is filled for the Lambda run; persisted counts match the generated minus rejected for each run.
3. Console → CloudWatch → the dashboard shows both runs; Lambda **Invocations** equals the *Invocations* shown on the Processor node (plus the one pre-warm call).

## F. Clean up and cost
* Lambda, CloudWatch and S3 requests are tiny at this scale (a 10,000-record run is about 40 Lambda calls). Keep to the 10 custom metrics: **never add per-run or per-preset dimensions** (each combination is billed as a separate metric).
* When finished: set `CLOUDWATCH_ENABLED=false`, delete the function (**Lambda → select → Actions → Delete**), delete the dashboard and alarms, stop the EC2 instance and **End Lab**.
* After you change `backend/app/pipeline/events.py`, rerun `python lambda/build.py`, upload the zip again (step B6) — the run log warns if the function's rules differ from the backend's.
