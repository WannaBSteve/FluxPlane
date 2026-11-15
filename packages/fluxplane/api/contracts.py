"""Public response shapes shared with the generated web client."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class IdentityOut(ResponseModel):
    role: Literal["admin", "operator", "viewer"]


class SourceOut(ResponseModel):
    id: str
    name: str
    kind: str
    config: dict[str, Any]
    created_at: datetime


class DatasetOut(ResponseModel):
    id: str
    name: str
    source_id: str
    source_object: str
    active_version_id: str | None


class FeatureOut(ResponseModel):
    id: str
    name: str
    sql: str | None
    dependencies: list[str]
    entity_keys: list[str]
    active_snapshot_id: str | None
    input_versions: dict[str, str]
    refresh_interval_seconds: int | None
    stale: bool | None = None
    maintenance_mode: str | None = None
    maintenance: dict[str, Any] | None = None
    next_refresh_at: datetime | None = None


class RuntimeOut(ResponseModel):
    duration_ms: float
    cpu_ms: float | None
    peak_rss_mb: float | None


class JobOut(ResponseModel):
    id: str
    query_sql: str | None = None
    query_question: str | None = None
    kind: str
    lane: str
    target_id: str | None
    workflow_id: str | None
    node_key: str | None
    status: str
    priority: int
    cpu_millicores: int
    memory_mb: int
    attempt: int
    result: dict[str, Any] | None
    error: str | None
    runtime: RuntimeOut | None
    created_at: datetime
    updated_at: datetime


class WorkflowEdgeOut(ResponseModel):
    from_: str = Field(alias="from")
    to: str


class WorkflowOut(ResponseModel):
    id: str
    name: str
    status: str
    created_at: datetime
    jobs: list[JobOut]
    edges: list[WorkflowEdgeOut]


class ReleaseOut(ResponseModel):
    id: str
    parent_id: str | None
    restored_from_id: str | None
    dataset_versions: dict[str, str]
    feature_snapshots: dict[str, str]
    reason: str
    created_at: datetime
    active: bool


class PoolOut(ResponseModel):
    lane: str
    capacity: int
    max_capacity: int
    running: int
    pending: int


class WorkloadLaneOut(ResponseModel):
    lane: str
    completed: int
    resource_samples: int
    avg_duration_ms: float
    avg_cpu_ms: float | None
    avg_peak_rss_mb: float | None


class ResourceMetricsOut(ResponseModel):
    cpu_budget_millicores: int
    cpu_reserved_millicores: int
    memory_budget_mb: int
    memory_reserved_mb: int


class MetricsOut(ResponseModel):
    sources: int
    datasets: int
    features: int
    jobs: int
    running_jobs: int
    pools: list[PoolOut]
    workload: list[WorkloadLaneOut]
    resources: ResourceMetricsOut


class StorageSummaryOut(ResponseModel):
    artifact_count: int
    artifact_bytes: int
    orphan_count: int
    orphan_bytes: int
    running_jobs: int
    collected: bool


class DatasetVersionOut(ResponseModel):
    id: str
    number: int
    row_count: int
    schema_: list[dict[str, str]] = Field(alias="schema")
    created_at: datetime


class NumericDistributionOut(ResponseModel):
    kind: Literal["numeric"]
    min: float
    p25: float
    median: float
    p75: float
    max: float


class CategoryValueOut(ResponseModel):
    value: str
    count: int


class CategoricalDistributionOut(ResponseModel):
    kind: Literal["categorical"]
    top_values: list[CategoryValueOut]


class ProfileFieldOut(ResponseModel):
    name: str
    type: str
    null_count: int
    distinct_in_sample: int | None
    distribution: NumericDistributionOut | CategoricalDistributionOut | None = None


class DatasetProfileOut(ResponseModel):
    version_id: str | None
    row_count: int
    sampled_rows: int
    fields: list[ProfileFieldOut]


class FeatureSnapshotOut(ResponseModel):
    id: str
    row_count: int
    input_versions: dict[str, str]
    created_at: datetime
    next_refresh_at: datetime | None


class MaterializationOut(ResponseModel):
    id: str
    sql: str
    input_versions: dict[str, str]
    status: str
    current: bool
    observed_queries: int
    observed_total_ms: float
    reuse_count: int
    reuse_total_ms: float
    estimated_saved_ms: float | None
    estimated_net_ms: float | None
    artifact_bytes: int | None
    build_duration_ms: float | None
    row_count: int | None
    error: str | None
    created_at: datetime


class DatasetLayoutOut(ResponseModel):
    id: str
    dataset_id: str
    dataset_name: str
    version_id: str
    current: bool
    partition_key: str
    status: str
    row_count: int | None
    partition_count: int | None
    fragment_count: int | None
    artifact_bytes: int | None
    build_ms: float | None
    baseline_ms: float | None
    organized_ms: float | None
    query_count: int
    error: str | None
    created_at: datetime


class QueryRowsOut(ResponseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    offset: int
    limit: int
    has_more: bool


class EventCursorOut(ResponseModel):
    cursor: int
