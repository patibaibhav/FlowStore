"""Amazon S3 storage adapter: health checks, resilient uploads, safe listing/preview.

Credentials come from the AWS default provider chain (IAM role on EC2). Nothing is
hardcoded and no local fallback exists: if S3 is unavailable, uploads fail loudly.
"""

from __future__ import annotations

import io
import random
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError, ClientError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError,
)

from app.core.config import Settings

RETRYABLE_CODES = {
    "Throttling", "ThrottlingException", "SlowDown", "RequestTimeout", "RequestTimeTooSkewed",
    "InternalError", "ServiceUnavailable", "RequestLimitExceeded", "ProvisionedThroughputExceededException",
    "BandwidthLimitExceeded", "OperationAborted",
}
FATAL_CODES = {
    "AccessDenied", "NoSuchBucket", "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken",
    "InvalidToken", "AllAccessDisabled", "AccountProblem", "InvalidBucketName", "NotSignedUp",
}


class StorageError(Exception):
    def __init__(self, message: str, code: str = "StorageError", retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def classify(exc: BaseException) -> tuple[str, bool]:
    """Return (error_code, retryable)."""
    if isinstance(exc, ClientError):
        code = exc.response.get("Error", {}).get("Code", "ClientError")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        if code in FATAL_CODES:
            return code, False
        if code in RETRYABLE_CODES or status >= 500 or status == 429:
            return code, True
        return code, False
    if isinstance(exc, (ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError)):
        return type(exc).__name__, True
    if isinstance(exc, BotoCoreError):
        # NoCredentialsError, PartialCredentialsError etc. are not retryable; others connection-ish
        name = type(exc).__name__
        return name, name not in {"NoCredentialsError", "PartialCredentialsError", "ParamValidationError"}
    if isinstance(exc, (ConnectionError, TimeoutError)):
        return type(exc).__name__, True
    return type(exc).__name__, False


@dataclass
class UploadResult:
    key: str
    size_bytes: int
    duration_ms: float
    attempts: int


class S3Storage:
    def __init__(self, settings: Settings, client: Any | None = None,
                 sleeper: Callable[[float], None] = time.sleep) -> None:
        self.settings = settings
        self.bucket = settings.s3_bucket_name
        self.prefix = settings.s3_prefix.strip("/")
        self._client = client
        self._lock = threading.Lock()
        self._sleep = sleeper
        # telemetry (global, since process start)
        self.stats_lock = threading.Lock()
        self.last_error: str | None = None
        self.last_success_at: float | None = None

    # ------------------------------------------------------------------
    @property
    def client(self) -> Any:
        with self._lock:
            if self._client is None:
                cfg = Config(
                    region_name=self.settings.aws_region,
                    retries={"max_attempts": 1, "mode": "standard"},  # we own the retry policy
                    connect_timeout=min(10, self.settings.s3_upload_timeout_seconds),
                    read_timeout=self.settings.s3_upload_timeout_seconds,
                    max_pool_connections=max(10, self.settings.s3_upload_concurrency * 2),
                )
                kwargs: dict[str, Any] = {"config": cfg}
                if self.settings.s3_endpoint_url:
                    kwargs["endpoint_url"] = self.settings.s3_endpoint_url
                self._client = boto3.client("s3", **kwargs)
            return self._client

    @property
    def runs_prefix(self) -> str:
        return f"{self.prefix}/runs/"

    def object_key(self, run_id: str, partition: int, chunk_id: str) -> str:
        for part in (run_id, chunk_id):
            if "/" in part or ".." in part or not part:
                raise StorageError("unsafe identifier in object key", "InvalidKey")
        return f"{self.prefix}/runs/{run_id}/partitions/{partition}/chunks/{chunk_id}.parquet"

    def assert_managed_key(self, key: str) -> None:
        if not key.startswith(self.runs_prefix) or ".." in key or key.endswith("/"):
            raise StorageError("key outside the FlowStore prefix", "Forbidden")

    # --- health --------------------------------------------------------
    def health(self, write_probe: bool = True) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []

        def add(name: str, ok: bool, detail: str, hint: str | None = None) -> bool:
            checks.append({"name": name, "ok": ok, "detail": detail, "hint": hint})
            return ok

        result = {"bucket": self.bucket, "region": self.settings.aws_region, "prefix": self.prefix,
                  "checks": checks, "ready": False, "write_verified": False, "checked_at": time.time()}
        if not add("configuration", bool(self.bucket), "S3_BUCKET_NAME is set" if self.bucket else "S3_BUCKET_NAME is not set",
                   "Set S3_BUCKET_NAME in the backend environment (.env)."):
            return result
        try:
            creds = boto3.session.Session().get_credentials() if self._client is None else True
        except Exception as e:  # pragma: no cover
            creds = None
            add("credentials", False, f"credential lookup failed: {type(e).__name__}")
            return result
        if not add("credentials", creds is not None,
                   "AWS credentials resolved from the default provider chain" if creds else "No AWS credentials found",
                   "Attach an IAM role to the EC2 instance (or set AWS_* env vars locally)."):
            return result
        try:
            self.client.head_bucket(Bucket=self.bucket)
            add("bucket_access", True, "head_bucket succeeded")
        except Exception as e:
            code, _ = classify(e)
            hints = {"NoSuchBucket": "Create the bucket or fix S3_BUCKET_NAME.", "404": "Bucket not found: check S3_BUCKET_NAME and AWS_REGION.",
                     "403": "Access denied: attach s3:ListBucket/GetObject/PutObject/DeleteObject permissions.",
                     "AccessDenied": "Access denied: check the IAM role permissions."}
            add("bucket_access", False, f"{code}: {str(e)[:160]}", hints.get(code, "Check region, bucket name and IAM permissions."))
            return result
        result["ready"] = True
        if write_probe:
            key = f"{self.prefix}/_health/probe-{uuid.uuid4().hex[:12]}"
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=b"flowstore-probe")
                self.client.delete_object(Bucket=self.bucket, Key=key)
                add("write_probe", True, "controlled PUT + DELETE succeeded (write capability verified)")
                result["write_verified"] = True
            except Exception as e:
                code, _ = classify(e)
                add("write_probe", False, f"{code}: {str(e)[:160]}", "Grant s3:PutObject and s3:DeleteObject on the prefix.")
                result["ready"] = False
        return result

    # --- upload --------------------------------------------------------
    def upload_file(self, path: Path, key: str, *, cancel: threading.Event | None = None,
                    max_retries: int | None = None,
                    on_retry: Callable[[int, float, str], None] | None = None) -> UploadResult:
        """Upload with bounded retries, exponential backoff + jitter, retryable classification."""
        self.assert_managed_key(key)
        data = path.read_bytes()
        retries = self.settings.s3_max_retries if max_retries is None else max_retries
        t0 = time.perf_counter()
        attempt = 0
        while True:
            attempt += 1
            if cancel is not None and cancel.is_set():
                raise StorageError("upload cancelled", "Cancelled", False)
            try:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=data,
                                       ContentType="application/vnd.apache.parquet")
                dur = (time.perf_counter() - t0) * 1000
                with self.stats_lock:
                    self.last_success_at = time.time()
                return UploadResult(key, len(data), dur, attempt)
            except Exception as e:
                code, retryable = classify(e)
                with self.stats_lock:
                    self.last_error = f"{code}: {str(e)[:200]}"
                if not retryable or attempt > retries:
                    raise StorageError(f"{code}: {str(e)[:200]}", code, retryable) from e
                delay = min(8.0, 0.2 * (2 ** (attempt - 1))) * (0.5 + random.random() / 2)
                if on_retry:
                    on_retry(attempt, delay, code)
                if cancel is not None:
                    if cancel.wait(delay):
                        raise StorageError("upload cancelled", "Cancelled", False)
                else:
                    self._sleep(delay)

    # --- listing / inspection -----------------------------------------
    def list_keys(self, run_id: str = "", max_keys: int = 1000) -> list[dict[str, Any]]:
        prefix = self.runs_prefix + (f"{run_id}/" if run_id else "")
        out: list[dict[str, Any]] = []
        token = None
        while len(out) < max_keys:
            kw: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix, "MaxKeys": min(1000, max_keys - len(out))}
            if token:
                kw["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kw)
            out.extend({"key": o["Key"], "size": o["Size"], "last_modified": o["LastModified"].timestamp()}
                       for o in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp["NextContinuationToken"]
        return out

    def head(self, key: str) -> dict[str, Any]:
        self.assert_managed_key(key)
        r = self.client.head_object(Bucket=self.bucket, Key=key)
        return {"size": r["ContentLength"], "last_modified": r["LastModified"].timestamp(), "etag": r.get("ETag")}

    def delete(self, key: str) -> None:
        self.assert_managed_key(key)
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def download_stream(self, key: str):
        self.assert_managed_key(key)
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        return body.iter_chunks(chunk_size=256 * 1024)

    def open_ranged(self, key: str) -> "S3RangeFile":
        self.assert_managed_key(key)
        size = self.head(key)["size"]
        return S3RangeFile(self.client, self.bucket, key, size)

    def inspect(self, key: str, sample_rows: int = 10) -> dict[str, Any]:
        """Read Parquet metadata + a small sample via ranged GETs (never the whole file)."""
        rf = self.open_ranged(key)
        pf = pq.ParquetFile(pa.PythonFile(rf, mode="r"))
        md = pf.metadata
        schema = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in pf.schema_arrow]
        row_groups = []
        codec = None
        for i in range(md.num_row_groups):
            rg = md.row_group(i)
            if codec is None and rg.num_columns:
                codec = rg.column(0).compression
            row_groups.append({"index": i, "rows": rg.num_rows, "bytes": rg.total_byte_size})
        sample: list[dict[str, Any]] = []
        if md.num_row_groups and sample_rows:
            tbl = pf.read_row_group(0).slice(0, sample_rows)
            for r in tbl.to_pylist():
                sample.append({k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()})
        return {"num_rows": md.num_rows, "num_row_groups": md.num_row_groups, "columns": md.num_columns,
                "created_by": md.created_by, "compression": codec, "schema": schema,
                "row_groups": row_groups, "sample": sample, "bytes_read": rf.bytes_read}

    def parquet_num_rows(self, key: str) -> tuple[int, str | None]:
        rf = self.open_ranged(key)
        pf = pq.ParquetFile(pa.PythonFile(rf, mode="r"))
        codec = pf.metadata.row_group(0).column(0).compression if pf.metadata.num_row_groups else None
        return pf.metadata.num_rows, codec


class S3RangeFile(io.RawIOBase):
    """Seekable read-only file over S3 using ranged GETs (footer + needed row group only)."""

    def __init__(self, client: Any, bucket: str, key: str, size: int) -> None:
        self._c, self._b, self._k, self._size = client, bucket, key, size
        self._pos = 0
        self.bytes_read = 0

    def readable(self) -> bool: return True
    def seekable(self) -> bool: return True
    def tell(self) -> int: return self._pos

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0: self._pos = offset
        elif whence == 1: self._pos += offset
        else: self._pos = self._size + offset
        return self._pos

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0: n = self._size - self._pos
        if n <= 0 or self._pos >= self._size: return b""
        end = min(self._size - 1, self._pos + n - 1)
        data = self._c.get_object(Bucket=self._b, Key=self._k, Range=f"bytes={self._pos}-{end}")["Body"].read()
        self._pos += len(data)
        self.bytes_read += len(data)
        return data

    def readinto(self, b) -> int:
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)

    @property
    def size(self) -> int: return self._size
