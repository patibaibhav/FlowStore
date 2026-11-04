import os
import boto3
import pytest
from moto import mock_aws

from app.core.config import Settings
from app.models.db import Database
from app.storage.s3 import S3Storage
from app.telemetry.eventlog import EventLog

BUCKET = "flowstore-test"


@pytest.fixture
def settings(tmp_path):
    return Settings(data_dir=tmp_path, s3_bucket_name=BUCKET, aws_region="us-east-1", s3_prefix="flowstore",
                    s3_max_retries=2, telemetry_hz=10, _env_file=None)


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def db(settings):
    d = Database(settings.db_path)
    yield d
    d.close()


@pytest.fixture
def storage(settings, aws):
    return S3Storage(settings, client=aws, sleeper=lambda s: None)


@pytest.fixture
def elog(db):
    return EventLog(db)
