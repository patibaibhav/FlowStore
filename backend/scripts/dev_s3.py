"""Local DEVELOPMENT ONLY: start a moto S3-compatible emulator on :5555 and create the bucket.

Usage:  python scripts/dev_s3.py
Then run the backend with:
  S3_ENDPOINT_URL=http://127.0.0.1:5555 S3_BUCKET_NAME=flowstore-dev
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
This is an emulator, NOT Amazon S3. Real deployments never set S3_ENDPOINT_URL.
"""
import os
import time

import boto3
from moto.server import ThreadedMotoServer

PORT = int(os.getenv("DEV_S3_PORT", "5555"))
BUCKET = os.getenv("S3_BUCKET_NAME", "flowstore-dev")

if __name__ == "__main__":
    server = ThreadedMotoServer(port=PORT)
    server.start()
    c = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{PORT}", region_name="us-east-1",
                     aws_access_key_id="test", aws_secret_access_key="test")
    c.create_bucket(Bucket=BUCKET)
    print(f"moto S3 emulator on :{PORT}, bucket '{BUCKET}' ready (Ctrl+C to stop)", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.stop()
