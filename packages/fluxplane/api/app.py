"""Versioned HTTP API for sources, datasets, features, jobs, and queries."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from secrets import compare_digest
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import case, func, select
from sqlalchemy.exc import IntegrityError

from fluxplane.api.contracts import (
    DatasetLayoutOut,
    DatasetOut,
    DatasetProfileOut,
    DatasetVersionOut,
    EventCursorOut,
    FeatureOut,
    FeatureSnapshotOut,
    IdentityOut,
    JobOut,
    MaterializationOut,
    MetricsOut,
    QueryRowsOut,
    ReleaseOut,
    SourceOut,
    StorageSummaryOut,
    WorkflowOut,
)
from fluxplane.api.exploration import exploration_router
from fluxplane.application.service import Platform
from fluxplane.config import Settings, settings
from fluxplane.execution.query import table_preview
from fluxplane.features.operators import OPERATORS
from fluxplane.scheduling.jobs import cancel, retry
from fluxplane.storage.models import (
    CatalogState,
    Dataset,
    DatasetLayout,
    DatasetProfile,
    DatasetVersion,
    Event,
    Feature,
    FeatureMaintenance,
    FeatureSnapshot,
    Job,
    JobDependency,
    Materialization,
    Observation,
    Release,
    Source,
    Workflow,
)


class SourceIn(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    kind: Literal["file", "sqlite", "postgres", "http", "document"]
    config: dict = Field(default_factory=dict)


class DatasetIn(BaseModel):
    source_id: str
    name: str
    source_object: str


class FeatureIn(BaseModel):
    name: str
    sql: str | None = None
    dependencies: list[str]
    entity_keys: list[str]
    maintenance: dict | None = None
    refresh_interval_seconds: int | None = Field(default=None, ge=1)


class LayoutIn(BaseModel):
    partition_key: str


class QueryIn(BaseModel):
    sql: str
    expected_versions: dict[str, str] | None = None
    question: str | None = Field(default=None, max_length=4000)
    cpu_millicores: int | None = Field(default=None, ge=1)
    memory_mb: int | None = Field(default=None, ge=1)


class WorkflowStepIn(BaseModel):
    key: str
    kind: Literal["sync", "refresh", "query"]
    target_id: str | None = None
    sql: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    force_full: bool = False
    cpu_millicores: int = Field(default=500, ge=1)
    memory_mb: int = Field(default=512, ge=1)


class WorkflowIn(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    steps: list[WorkflowStepIn] = Field(min_length=1, max_length=100)


def source_dict(item: Source) -> dict:
    return {"id": item.id, "name": item.name, "kind": item.kind,
            "config": item.config, "created_at": item.created_at}


def dataset_dict(item: Dataset) -> dict:
    return {"id": item.id, "name": item.name, "source_id": item.source_id,
            "source_object": item.source_object, "active_version_id": item.active_version_id}


def feature_dict(item: Feature, input_versions: dict | None = None) -> dict:
    return {"id": item.id, "name": item.name, "sql": item.sql,
            "dependencies": item.dependencies, "entity_keys": item.entity_keys,
            "active_snapshot_id": item.active_snapshot_id, "input_versions": input_versions or {},
            "refresh_interval_seconds": item.refresh_interval_seconds}


def job_dict(item: Job, runtime: Observation | None = None) -> dict:
    result = ({key: value for key, value in item.result.items()
               if key not in {"artifact_path", "sha256"}}
              if isinstance(item.result, dict) else item.result)
    return {"id": item.id, "kind": item.kind, "lane": item.lane,
            "query_sql": item.payload.get("sql") if item.kind == "query" else None,
            "query_question": item.payload.get("question") if item.kind == "query" else None,
            "target_id": item.target_id, "status": item.status, "priority": item.priority,
            "workflow_id": item.workflow_id, "node_key": item.node_key,
            "cpu_millicores": item.cpu_millicores, "memory_mb": item.memory_mb,
            "attempt": item.attempt, "result": result, "error": item.error,
            "runtime": ({"duration_ms": runtime.duration_ms, "cpu_ms": runtime.cpu_ms,
                         "peak_rss_mb": runtime.peak_rss_mb} if runtime else None),
            "created_at": item.created_at, "updated_at": item.updated_at}


def workflow_dict(session, item: Workflow) -> dict:
    jobs = session.scalars(select(Job).where(Job.workflow_id == item.id)
                           .order_by(Job.created_at, Job.node_key)).all()
    keys = {job.id: job.node_key for job in jobs}
    edges = session.execute(select(JobDependency).where(
        JobDependency.job_id.in_(keys)
    )).scalars().all() if keys else []
    states = {job.status for job in jobs}
    status = ("failed" if "failed" in states else "cancelled" if "cancelled" in states else
              "succeeded" if states == {"succeeded"} else "running" if "running" in states else
              "pending")
    return {"id": item.id, "name": item.name, "status": status,
            "created_at": item.created_at,
            "jobs": [job_dict(job) for job in jobs],
            "edges": [{"from": keys[edge.depends_on_id], "to": keys[edge.job_id]}
                      for edge in edges]}


def release_dict(item: Release, active_id: str) -> dict:
    return {"id": item.id, "parent_id": item.parent_id,
            "restored_from_id": item.restored_from_id,
            "dataset_versions": item.dataset_versions,
            "feature_snapshots": item.feature_snapshots,
            "reason": item.reason, "created_at": item.created_at,
            "active": item.id == active_id}


def create_app(config: Settings | None = None) -> FastAPI:
    config = config or settings()
    platform = Platform(config)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        platform.initialize()
        yield

    app = FastAPI(title="FluxPlane API", version="0.1.0", lifespan=lifespan)

    def role_for_key(x_api_key: Annotated[str | None, Header()] = None) -> str:
        if not config.api_key:
            return "admin"
        for role, key in (("admin", config.api_key), ("operator", config.operator_api_key),
                          ("viewer", config.viewer_api_key)):
            if key and x_api_key and compare_digest(x_api_key, key):
                return role
        raise HTTPException(status_code=401, detail="invalid API key")

    def require_operator(role: str = Depends(role_for_key)) -> None:
        if role == "viewer":
            raise HTTPException(status_code=403, detail="operator role required")

    def require_admin(role: str = Depends(role_for_key)) -> None:
        if role != "admin":
            raise HTTPException(status_code=403, detail="administrator role required")

    read_guard = [Depends(role_for_key)]
    operator_guard = [Depends(require_operator)]
    admin_guard = [Depends(require_admin)]
    app.include_router(exploration_router(platform, read_guard, operator_guard))

    @app.get("/api/v1/auth/me", response_model=IdentityOut)
    def current_role(role: str = Depends(role_for_key)):
        return {"role": role}

    @app.exception_handler(ValueError)
    async def value_error_handler(_, exc: ValueError):
        return Response(json.dumps({"detail": str(exc)}), status_code=400,
                        media_type="application/json")

    @app.exception_handler(IntegrityError)
    async def integrity_error_handler(_, exc: IntegrityError):
        return Response(json.dumps({"detail": "name or idempotency key already exists"}),
                        status_code=409, media_type="application/json")

    @app.get("/health/live")
    def live():
        return {"status": "ok"}

    @app.get("/health/ready")
    def ready():
        try:
            with platform.db.session() as session:
                session.execute(select(1))
            if not config.artifact_root.is_dir() or not config.source_root.is_dir():
                raise RuntimeError("storage directories are unavailable")
        except Exception as exc:
            raise HTTPException(status_code=503, detail="service dependencies unavailable") from exc
        return {"status": "ready"}

    @app.get("/api/v1/system/metrics", dependencies=read_guard, response_model=MetricsOut)
    def metrics():
        return platform.status()

    @app.get("/api/v1/storage/summary", dependencies=read_guard,
             response_model=StorageSummaryOut)
    def storage_summary():
        return platform.storage_summary()

    @app.get("/api/v1/sources", dependencies=read_guard, response_model=list[SourceOut])
    def sources():
        with platform.db.session() as session:
            return jsonable_encoder([source_dict(x) for x in session.scalars(select(Source)).all()])

    @app.post("/api/v1/sources", dependencies=admin_guard, status_code=201)
    def create_source(body: SourceIn):
        return jsonable_encoder(source_dict(platform.add_source(body.name, body.kind, body.config)))

    @app.get("/api/v1/datasets", dependencies=read_guard, response_model=list[DatasetOut])
    def datasets():
        with platform.db.session() as session:
            return [dataset_dict(x) for x in session.scalars(select(Dataset)).all()]

    @app.post("/api/v1/datasets", dependencies=admin_guard, status_code=201)
    def create_dataset(body: DatasetIn):
        return dataset_dict(platform.add_dataset(body.source_id, body.name, body.source_object))

    @app.post("/api/v1/datasets/{dataset_id}/sync", dependencies=operator_guard, status_code=202)
    def sync_dataset(dataset_id: str, full: bool = False,
                     idempotency_key: Annotated[str | None, Header()] = None):
        return job_dict(platform.queue_sync(dataset_id, idempotency_key, force_full=full))

    @app.get("/api/v1/datasets/{dataset_id}/versions", dependencies=read_guard,
             response_model=list[DatasetVersionOut])
    def dataset_versions(dataset_id: str):
        with platform.db.session() as session:
            return jsonable_encoder([
                {"id": x.id, "number": x.number, "row_count": x.row_count,
                 "schema": x.schema_json, "created_at": x.created_at}
                for x in session.scalars(select(DatasetVersion)
                                         .where(DatasetVersion.dataset_id == dataset_id)
                                         .order_by(DatasetVersion.number.desc())).all()
            ])

    @app.get("/api/v1/datasets/{dataset_id}/profile", dependencies=read_guard,
             response_model=DatasetProfileOut)
    def dataset_profile(dataset_id: str):
        with platform.db.session() as session:
            dataset = session.get(Dataset, dataset_id)
            if dataset is None:
                raise HTTPException(status_code=404, detail="dataset not found")
            if not dataset.active_version_id:
                return {"version_id": None, "row_count": 0, "sampled_rows": 0, "fields": []}
            version = session.get(DatasetVersion, dataset.active_version_id)
            profile = session.get(DatasetProfile, version.id)
            return {"version_id": version.id, "row_count": version.row_count,
                    "sampled_rows": profile.sampled_rows if profile else 0,
                    "fields": profile.fields if profile else []}

    @app.get("/api/v1/features", dependencies=read_guard, response_model=list[FeatureOut])
    def features():
        with platform.db.session() as session:
            state = session.get(CatalogState, 1)
            release = session.get(Release, state.active_release_id)
            records = []
            for item in session.scalars(select(Feature)).all():
                active_id = release.feature_snapshots.get(item.name)
                active = session.get(FeatureSnapshot, active_id) if active_id else None
                record = feature_dict(item, active.input_versions if active else None)
                record["active_snapshot_id"] = active_id
                record["stale"] = bool(item.active_snapshot_id and not active_id) or platform.feature_expired(item, active)
                record["next_refresh_at"] = (platform._utc(active.next_refresh_at).isoformat()
                                             if active and active.next_refresh_at else None)
                rule = session.get(FeatureMaintenance, item.id)
                record["maintenance_mode"] = rule.mode if rule else "full"
                record["maintenance"] = rule.config if rule else None
                records.append(record)
            return records

    @app.get("/api/v1/feature-operators", dependencies=read_guard)
    def feature_operators():
        return OPERATORS

    @app.post("/api/v1/features", dependencies=admin_guard, status_code=201)
    def create_feature(body: FeatureIn):
        return feature_dict(platform.add_feature(
            body.name, body.sql, body.dependencies, body.entity_keys, body.maintenance,
            body.refresh_interval_seconds,
        ))

    @app.post("/api/v1/features/{feature_id}/refresh", dependencies=operator_guard, status_code=202)
    def refresh_feature(feature_id: str, idempotency_key: Annotated[str | None, Header()] = None):
        return job_dict(platform.queue_refresh(feature_id, idempotency_key))

    @app.get("/api/v1/layouts", dependencies=read_guard,
             response_model=list[DatasetLayoutOut])
    def layouts(limit: int = 50):
        with platform.db.session() as session:
            rows = session.execute(
                select(DatasetLayout, Dataset, DatasetVersion)
                .join(DatasetVersion, DatasetLayout.version_id == DatasetVersion.id)
                .join(Dataset, DatasetVersion.dataset_id == Dataset.id)
                .order_by(DatasetLayout.created_at.desc())
                .limit(min(max(limit, 1), 100))
            ).all()
            return [{"id": layout.id, "dataset_id": dataset.id,
                     "dataset_name": dataset.name, "version_id": version.id,
                     "current": dataset.active_version_id == version.id,
                     "partition_key": layout.partition_key, "status": layout.status,
                     "row_count": layout.row_count,
                     "partition_count": layout.partition_count,
                     "fragment_count": layout.fragment_count,
                     "artifact_bytes": layout.artifact_bytes,
                     "build_ms": layout.build_ms,
                     "baseline_ms": layout.baseline_ms,
                     "organized_ms": layout.organized_ms,
                     "query_count": layout.query_count,
                     "error": layout.error, "created_at": layout.created_at}
                    for layout, dataset, version in rows]

    @app.post("/api/v1/datasets/{dataset_id}/layouts", dependencies=admin_guard,
              status_code=202)
    def create_layout(dataset_id: str, body: LayoutIn):
        return job_dict(platform.queue_layout(dataset_id, body.partition_key))

    @app.get("/api/v1/features/{feature_id}/snapshots", dependencies=read_guard,
             response_model=list[FeatureSnapshotOut])
    def feature_snapshots(feature_id: str):
        with platform.db.session() as session:
            return jsonable_encoder([
                {"id": x.id, "row_count": x.row_count, "input_versions": x.input_versions,
                 "created_at": x.created_at,
                 "next_refresh_at": (platform._utc(x.next_refresh_at).isoformat()
                                     if x.next_refresh_at else None)}
                for x in session.scalars(select(FeatureSnapshot)
                                         .where(FeatureSnapshot.feature_id == feature_id)
                                         .order_by(FeatureSnapshot.created_at.desc())).all()
            ])

    @app.get("/api/v1/releases", dependencies=read_guard, response_model=list[ReleaseOut])
    def releases(limit: int = 30):
        with platform.db.session() as session:
            active_id = session.get(CatalogState, 1).active_release_id
            return jsonable_encoder([
                release_dict(item, active_id) for item in session.scalars(
                    select(Release).order_by(Release.created_at.desc(), Release.id.desc())
                    .limit(min(max(limit, 1), 100))
                ).all()
            ])

    @app.post("/api/v1/releases/{release_id}/restore", dependencies=admin_guard)
    def restore_release(release_id: str):
        restored = platform.restore_release(release_id)
        return jsonable_encoder(release_dict(restored, restored.id))

    @app.post("/api/v1/queries", dependencies=operator_guard, status_code=202)
    def create_query(body: QueryIn, idempotency_key: Annotated[str | None, Header()] = None):
        return job_dict(platform.queue_query(body.sql, idempotency_key,
                                            expected_versions=body.expected_versions,
                                            question=body.question,
                                             cpu_millicores=body.cpu_millicores,
                                             memory_mb=body.memory_mb))

    @app.post("/api/v1/workflows", dependencies=operator_guard, status_code=201)
    def create_workflow(body: WorkflowIn,
                        idempotency_key: Annotated[str | None, Header()] = None):
        workflow = platform.create_workflow(
            body.name, [step.model_dump(exclude_none=True) for step in body.steps],
            idempotency_key,
        )
        with platform.db.session() as session:
            return jsonable_encoder(workflow_dict(session, workflow))

    @app.get("/api/v1/workflows", dependencies=read_guard,
             response_model=list[WorkflowOut])
    def workflows(limit: int = 20):
        with platform.db.session() as session:
            items = session.scalars(select(Workflow).order_by(Workflow.created_at.desc())
                                    .limit(min(max(limit, 1), 100))).all()
            return jsonable_encoder([workflow_dict(session, item) for item in items])

    @app.get("/api/v1/workflows/{workflow_id}", dependencies=read_guard,
             response_model=WorkflowOut)
    def workflow(workflow_id: str):
        with platform.db.session() as session:
            item = session.get(Workflow, workflow_id)
            if item is None:
                raise HTTPException(status_code=404, detail="workflow not found")
            return jsonable_encoder(workflow_dict(session, item))

    @app.get("/api/v1/materializations", dependencies=read_guard,
             response_model=list[MaterializationOut])
    def materializations(limit: int = 30):
        with platform.db.session() as session:
            state = session.get(CatalogState, 1)
            release = session.get(Release, state.active_release_id)
            active = {**release.dataset_versions, **release.feature_snapshots}
            items = session.scalars(select(Materialization)
                                    .order_by(case(
                                        (Materialization.status == "active", 0),
                                        (Materialization.status == "building", 1),
                                        (Materialization.status == "observing", 2),
                                        (Materialization.status == "rejected", 3),
                                        else_=4,
                                    ), Materialization.updated_at.desc())
                                    .limit(min(max(limit, 1), 100))).all()
            return [{"id": item.id, "sql": item.sql,
                     "input_versions": item.input_versions, "status": item.status,
                     "current": all(active.get(name) == version for name, version
                                    in item.input_versions.items()),
                     "observed_queries": item.observed_queries,
                     "observed_total_ms": round(item.observed_total_ms, 1),
                     "reuse_count": item.reuse_count,
                     "reuse_total_ms": round(item.reuse_total_ms, 1),
                     "estimated_saved_ms": round(
                         (item.observed_total_ms / item.observed_queries) * item.reuse_count
                         - item.reuse_total_ms, 1
                     ) if item.observed_queries else None,
                     "estimated_net_ms": round(
                         (item.observed_total_ms / item.observed_queries) * item.reuse_count
                         - item.reuse_total_ms - item.build_duration_ms, 1
                     ) if item.observed_queries and item.build_duration_ms is not None else None,
                     "artifact_bytes": (Path(item.artifact_path).stat().st_size
                                        if item.artifact_path and
                                        platform.store.available(item.artifact_path) else None),
                     "build_duration_ms": item.build_duration_ms,
                     "row_count": item.row_count, "error": item.error,
                     "created_at": item.created_at}
                    for item in items]

    @app.get("/api/v1/runs", dependencies=read_guard, response_model=list[JobOut])
    def runs(limit: int = 50):
        with platform.db.session() as session:
            items = session.scalars(
                select(Job).order_by(Job.created_at.desc()).limit(min(max(limit, 1), 200))
            ).all()
            latest = {}
            if items:
                for observation in session.scalars(select(Observation).where(
                    Observation.job_id.in_([item.id for item in items])
                ).order_by(Observation.created_at, Observation.id)):
                    latest[observation.job_id] = observation
            return jsonable_encoder([job_dict(item, latest.get(item.id)) for item in items])

    @app.get("/api/v1/runs/{job_id}", dependencies=read_guard, response_model=JobOut)
    def run(job_id: str):
        with platform.db.session() as session:
            item = session.get(Job, job_id)
            if item is None:
                raise HTTPException(status_code=404, detail="run not found")
            observation = session.scalar(select(Observation).where(
                Observation.job_id == item.id
            ).order_by(Observation.created_at.desc(), Observation.id.desc()).limit(1))
            return jsonable_encoder(job_dict(item, observation))

    @app.get("/api/v1/runs/{job_id}/rows", dependencies=read_guard,
             response_model=QueryRowsOut)
    def query_rows(job_id: str, offset: int = Query(default=0, ge=0),
                   limit: int = Query(default=25, ge=1, le=500)):
        with platform.db.session() as session:
            item = session.get(Job, job_id)
            if item is None:
                raise HTTPException(status_code=404, detail="run not found")
            if item.kind != "query":
                raise HTTPException(status_code=400, detail="run is not a query")
            if item.status != "succeeded":
                raise HTTPException(status_code=409, detail="query result is not ready")
            path = item.result.get("artifact_path") if isinstance(item.result, dict) else None
        if not path:
            raise HTTPException(status_code=410, detail="query result is no longer available")
        try:
            table, total = platform.store.read_page(path, offset, limit)
        except (OSError, ValueError) as exc:
            raise HTTPException(status_code=410, detail="query result is no longer available") from exc
        preview = table_preview(table, limit)
        return {**preview, "row_count": total, "offset": offset, "limit": limit,
                "has_more": offset + table.num_rows < total}

    @app.post("/api/v1/runs/{job_id}/cancel", dependencies=operator_guard)
    def cancel_run(job_id: str):
        with platform.db.session() as session:
            item = cancel(session, job_id)
            if item is None:
                raise HTTPException(status_code=404, detail="run not found")
            return jsonable_encoder(job_dict(item))

    @app.post("/api/v1/runs/{job_id}/retry", dependencies=operator_guard)
    def retry_run(job_id: str):
        with platform.db.session() as session:
            item = retry(session, job_id)
            if item is None:
                raise HTTPException(status_code=404, detail="run not found")
            return jsonable_encoder(job_dict(item))

    @app.get("/api/v1/events/cursor", dependencies=read_guard,
             response_model=EventCursorOut)
    def event_cursor():
        with platform.db.session() as session:
            return {"cursor": session.scalar(select(func.max(Event.id))) or 0}

    @app.get("/api/v1/events", dependencies=read_guard)
    async def events(after: int = 0):
        async def stream():
            cursor = max(0, after)
            idle_seconds = 0
            yield ": connected\n\n"
            while True:
                with platform.db.session() as session:
                    rows = session.scalars(
                        select(Event).where(Event.id > cursor).order_by(Event.id).limit(50)
                    ).all()
                    batch = [(e.id, e.kind, e.subject_id, e.data) for e in rows]
                for event_id, kind, subject, data in batch:
                    cursor = event_id
                    payload = json.dumps({"id": event_id, "kind": kind, "subject_id": subject,
                                          "data": data}, default=str)
                    yield f"id: {event_id}\ndata: {payload}\n\n"
                idle_seconds = 0 if batch else idle_seconds + 1
                if idle_seconds >= 5:
                    yield ": heartbeat\n\n"
                    idle_seconds = 0
                await asyncio.sleep(1)

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache"})

    return app


app = create_app()

