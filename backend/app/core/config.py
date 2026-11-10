"""Environment-driven application settings. Nothing secret is stored here."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- AWS / S3 -----------------------------------------------------
    aws_region: str = "us-east-1"
    s3_bucket_name: str = ""
    s3_prefix: str = "flowstore"
    s3_max_storage_mb: int = 512
    s3_upload_concurrency: int = Field(default=2, ge=1, le=16)
    s3_upload_timeout_seconds: int = Field(default=30, ge=1, le=300)
    s3_max_retries: int = Field(default=4, ge=0, le=10)
    # Optional endpoint override. ONLY for local development against an S3-compatible
    # emulator (e.g. moto server). Leave empty on AWS.
    s3_endpoint_url: str = ""

    # --- Optional Lambda processor -------------------------------------
    # Name (or ARN) of the Lambda function that validates batches in "lambda" processor mode.
    # Empty = Lambda mode unavailable. Never taken from the browser.
    lambda_processor_name: str = ""
    lambda_timeout_seconds: int = Field(default=15, ge=1, le=60)
    lambda_max_retries: int = Field(default=3, ge=0, le=8)
    # DEVELOPMENT/TESTS ONLY: call the handler in-process instead of AWS. Labelled "emulated" in the UI.
    lambda_emulate_local: bool = False

    # --- CloudWatch application metrics (off by default) ----------------
    cloudwatch_enabled: bool = False
    cloudwatch_namespace: str = "FlowStore"
    cloudwatch_interval_seconds: float = Field(default=10.0, ge=1, le=300)

    # --- Application --------------------------------------------------
    data_dir: Path = Path("./data")
    cors_origins: str = "http://localhost:5173,http://127.0.0.1:5173"
    telemetry_hz: float = Field(default=5.0, ge=1, le=20)
    max_event_log_rows_per_run: int = 5000
    max_metric_rows_per_run: int = 3000
    max_rejected_rows_per_run: int = 500
    max_stored_runs: int = 200
    # Admin token protecting destructive storage operations. Empty = no extra token
    # (the UI still requires explicit confirmation).
    admin_token: str = ""

    # --- Run caps (cost control) -------------------------------------
    max_run_seconds: int = 300
    max_rate_per_second: int = 20_000
    max_records_per_run: int = 500_000
    max_pending_uploads_cap: int = 32

    # --- Cost estimate assumptions (configurable, estimates only) -----
    price_s3_put_per_1000: float = 0.005
    price_s3_gb_month: float = 0.023

    @property
    def using_s3_emulator(self) -> bool:
        return bool(self.s3_endpoint_url)

    @property
    def lambda_available(self) -> bool:
        """Lambda mode is offered only with a configured function (real AWS) or explicit local emulation."""
        if self.lambda_emulate_local:
            return True
        return bool(self.lambda_processor_name) and not self.using_s3_emulator

    @property
    def cloudwatch_active(self) -> bool:
        """CloudWatch is never used against the S3 emulator (there is no AWS to talk to)."""
        return self.cloudwatch_enabled and not self.using_s3_emulator

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def db_path(self) -> Path:
        return self.data_dir / "flowstore.sqlite"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"


@lru_cache
def get_settings() -> Settings:
    return Settings()
