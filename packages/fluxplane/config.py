"""Runtime configuration shared by API, scheduler, and workers."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLUXPLANE_", extra="ignore")

    database_url: str = "sqlite:////tmp/fluxplane-development.sqlite3"
    artifact_root: Path = Path("./runtime/artifacts")
    source_root: Path = Path("./runtime/sources")
    api_key: str = ""
    operator_api_key: str = ""
    viewer_api_key: str = ""
    analysis_base_url: str = ""
    analysis_model: str = ""
    analysis_api_key: str = Field(default="", repr=False)
    analysis_timeout_seconds: int = Field(default=60, ge=1, le=180)
    analysis_response_format: bool = False
    http_allowed_hosts: str = ""
    max_query_rows: int = Field(default=1000, ge=1, le=10000)
    worker_lease_seconds: int = Field(default=120, ge=10)
    query_workers: int = Field(default=1, ge=1)
    background_workers: int = Field(default=1, ge=1)
    background_borrow_grace_ms: int = Field(default=500, ge=0)
    resource_cpu_millicores: int = Field(default=2000, ge=500)
    resource_memory_mb: int = Field(default=2048, ge=512)
    remote_poll_seconds: int = Field(default=60, ge=1)
    materialization_max_bytes: int = Field(default=268435456, ge=0)
    materialization_stale_seconds: int = Field(default=86400, ge=0)
    postgres_full_reconcile_seconds: int = Field(default=86400, ge=60)
    http_full_reconcile_seconds: int = Field(default=86400, ge=60)

    @model_validator(mode="after")
    def validate_api_keys(self) -> Settings:
        keys = [key for key in (self.api_key, self.operator_api_key, self.viewer_api_key) if key]
        if len(keys) != len(set(keys)):
            raise ValueError("API keys for different roles must be distinct")
        if len(keys) > 0 and not self.api_key:
            raise ValueError("an administrator API key is required when roles are configured")
        return self

    def prepare_dirs(self) -> None:
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.source_root.mkdir(parents=True, exist_ok=True)


def settings() -> Settings:
    return Settings()
