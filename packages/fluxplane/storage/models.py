"""Persistent catalog, task, and publication records."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def now() -> datetime:
    return datetime.now(UTC)


def uid() -> str:
    return uuid4().hex


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    name: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class Dataset(Base):
    __tablename__ = "datasets"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    source_id: Mapped[str] = mapped_column(ForeignKey("sources.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    source_object: Mapped[str] = mapped_column(String(300), nullable=False)
    active_version_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (UniqueConstraint("dataset_id", "number"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    dataset_id: Mapped[str] = mapped_column(ForeignKey("datasets.id"), nullable=False)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    artifact_path: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_json: Mapped[list] = mapped_column(JSON, nullable=False)
    source_fingerprint: Mapped[str] = mapped_column(String(128), nullable=False)
    source_cursor: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DatasetProfile(Base):
    __tablename__ = "dataset_profiles"

    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), primary_key=True)
    sampled_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    fields: Mapped[list] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DatasetDelta(Base):
    __tablename__ = "dataset_deltas"

    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), primary_key=True)
    base_version_id: Mapped[str | None] = mapped_column(String(32))
    append_only: Mapped[bool] = mapped_column(Boolean, nullable=False)
    artifact_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(String(64))
    row_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Feature(Base):
    __tablename__ = "features"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    name: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    dependencies: Mapped[list] = mapped_column(JSON, nullable=False)
    entity_keys: Mapped[list] = mapped_column(JSON, nullable=False)
    refresh_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    active_snapshot_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class FeatureSnapshot(Base):
    __tablename__ = "feature_snapshots"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    feature_id: Mapped[str] = mapped_column(ForeignKey("features.id"), nullable=False)
    input_versions: Mapped[dict] = mapped_column(JSON, nullable=False)
    artifact_path: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    next_refresh_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class FeatureMaintenance(Base):
    __tablename__ = "feature_maintenance"

    feature_id: Mapped[str] = mapped_column(ForeignKey("features.id"), primary_key=True)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False)


class Release(Base):
    __tablename__ = "releases"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    parent_id: Mapped[str | None] = mapped_column(String(32))
    restored_from_id: Mapped[str | None] = mapped_column(String(32))
    dataset_versions: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    feature_snapshots: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    reason: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class CatalogState(Base):
    __tablename__ = "catalog_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    active_release_id: Mapped[str] = mapped_column(ForeignKey("releases.id"), nullable=False)


class Workflow(Base):
    __tablename__ = "workflows"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        Index("ix_jobs_queue", "status", "lane", "priority", "created_at"),
        Index("ix_jobs_workflow", "workflow_id"),
        UniqueConstraint("idempotency_key"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    lane: Mapped[str] = mapped_column(String(32), nullable=False)
    target_id: Mapped[str | None] = mapped_column(String(32))
    workflow_id: Mapped[str | None] = mapped_column(String(32))
    node_key: Mapped[str | None] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cpu_millicores: Mapped[int] = mapped_column(Integer, nullable=False, default=500)
    memory_mb: Mapped[int] = mapped_column(Integer, nullable=False, default=512)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    idempotency_key: Mapped[str | None] = mapped_column(String(200))
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_generation: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    worker_id: Mapped[str | None] = mapped_column(String(100))
    result: Mapped[dict | None] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, onupdate=now)


class JobDependency(Base):
    __tablename__ = "job_dependencies"

    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)
    depends_on_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), primary_key=True)


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (UniqueConstraint("job_id", "number"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error: Mapped[str | None] = mapped_column(Text)


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[str] = mapped_column(String(32), nullable=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class Observation(Base):
    __tablename__ = "observations"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    job_id: Mapped[str] = mapped_column(ForeignKey("jobs.id"), nullable=False)
    lane: Mapped[str] = mapped_column(String(32), nullable=False)
    duration_ms: Mapped[float] = mapped_column(nullable=False)
    cpu_ms: Mapped[float | None] = mapped_column()
    peak_rss_mb: Mapped[float | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)


class DatasetLayout(Base):
    __tablename__ = "dataset_layouts"
    __table_args__ = (UniqueConstraint("version_id", "partition_key"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    version_id: Mapped[str] = mapped_column(ForeignKey("dataset_versions.id"), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(160), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="building")
    artifact_dir: Mapped[str | None] = mapped_column(Text)
    row_count: Mapped[int | None] = mapped_column(Integer)
    partition_count: Mapped[int | None] = mapped_column(Integer)
    fragment_count: Mapped[int | None] = mapped_column(Integer)
    artifact_bytes: Mapped[int | None] = mapped_column(Integer)
    build_ms: Mapped[float | None] = mapped_column()
    baseline_ms: Mapped[float | None] = mapped_column()
    organized_ms: Mapped[float | None] = mapped_column()
    query_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, onupdate=now)


class Materialization(Base):
    __tablename__ = "materializations"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=uid)
    signature: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    sql: Mapped[str] = mapped_column(Text, nullable=False)
    input_versions: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, default="observing")
    observed_queries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    observed_total_ms: Mapped[float] = mapped_column(nullable=False, default=0.0)
    reuse_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reuse_total_ms: Mapped[float] = mapped_column(nullable=False, default=0.0)
    artifact_path: Mapped[str | None] = mapped_column(Text)
    sha256: Mapped[str | None] = mapped_column(String(64))
    preview: Mapped[dict | None] = mapped_column(JSON)
    row_count: Mapped[int | None] = mapped_column(Integer)
    build_duration_ms: Mapped[float | None] = mapped_column()
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, onupdate=now)

