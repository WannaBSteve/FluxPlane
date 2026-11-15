"""Catalog operations, pinned inputs, and fenced publication."""

from __future__ import annotations

import shutil
import statistics
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from fluxplane.config import Settings
from fluxplane.connectors.snapshot import (
    read_http_changes,
    read_postgres_changes,
    read_snapshot,
    stable_file_state,
)
from fluxplane.execution.query import (
    IDENTIFIER,
    equality_filters_by_source,
    equivalent_results,
    materialization_signature,
    run_sql,
    table_preview,
    validate_query,
)
from fluxplane.features.aggregate import aggregate_sql, merge_sql
from fluxplane.features.operators import run_operator, validate_operator
from fluxplane.features.window import (
    rolling_next_due_sql,
    rolling_sum_sql,
    validate_window_query,
    validate_window_rules,
    window_filter_sql,
    window_next_due_sql,
)
from fluxplane.profiling.data import profile_table
from fluxplane.profiling.runtime import RuntimeSampler
from fluxplane.scheduling.jobs import complete, submit
from fluxplane.storage.artifacts import ArtifactStore
from fluxplane.storage.database import Database
from fluxplane.storage.models import (
    CatalogState,
    Dataset,
    DatasetDelta,
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
    now,
)


class ConflictError(RuntimeError):
    pass


def _measure_layout_probe(probe: str, source_path: str, layout_path: str,
                          store: ArtifactStore) -> tuple[float, float]:
    baseline_samples = []
    organized_samples = []
    for attempt in range(3):
        order = ("baseline", "organized") if attempt != 1 else ("organized", "baseline")
        results = {}
        for variant in order:
            input_source = source_path if variant == "baseline" else store.open_layout(layout_path)
            probe_started = time.monotonic()
            results[variant] = run_sql(probe, {"source": input_source}, store)
            elapsed = (time.monotonic() - probe_started) * 1000
            (baseline_samples if variant == "baseline" else organized_samples).append(elapsed)
        if not results["baseline"].equals(results["organized"]):
            raise ValueError("organized layout changes the filtered result")
    return statistics.median(baseline_samples), statistics.median(organized_samples)


class Platform:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings)
        self.store = ArtifactStore(settings.artifact_root)

    def initialize(self) -> None:
        self.settings.prepare_dirs()
        self.db.create_schema()
        try:
            with self.db.session() as session:
                if session.get(CatalogState, 1) is not None:
                    return
                datasets = {item.name: item.active_version_id for item in
                            session.scalars(select(Dataset)).all() if item.active_version_id}
                features = {}
                for item in session.scalars(select(Feature)).all():
                    snapshot = (session.get(FeatureSnapshot, item.active_snapshot_id)
                                if item.active_snapshot_id else None)
                    if snapshot and all(datasets.get(name) == version_id
                                        for name, version_id in snapshot.input_versions.items()):
                        features[item.name] = snapshot.id
                initial = Release(dataset_versions=datasets, feature_snapshots=features,
                                  reason="initial")
                session.add(initial)
                session.flush()
                session.add(CatalogState(id=1, active_release_id=initial.id))
        except IntegrityError:
            # A second process may have initialized the same shared catalog.
            pass

    @staticmethod
    def _locked_release(session: Session) -> tuple[CatalogState, Release]:
        state = session.get(CatalogState, 1, with_for_update=True)
        if state is None:
            raise RuntimeError("catalog is not initialized")
        return state, session.get(Release, state.active_release_id)

    @staticmethod
    def _create_release(session: Session, state: CatalogState, parent: Release,
                        datasets: dict, features: dict, reason: str,
                        restored_from_id: str | None = None) -> Release:
        release = Release(parent_id=parent.id, dataset_versions=datasets,
                          feature_snapshots=features, reason=reason,
                          restored_from_id=restored_from_id)
        session.add(release)
        session.flush()
        state.active_release_id = release.id
        session.add(Event(kind="release.published", subject_id=release.id,
                          data={"reason": reason, "parent_id": parent.id}))
        return release

    def restore_release(self, release_id: str) -> Release:
        with self.db.session() as session:
            state, current = self._locked_release(session)
            target = session.get(Release, release_id)
            if target is None:
                raise ValueError("release does not exist")
            datasets = session.scalars(select(Dataset).order_by(Dataset.id).with_for_update()).all()
            features = session.scalars(select(Feature).order_by(Feature.id).with_for_update()).all()
            by_dataset = {item.name: item for item in datasets}
            by_feature = {item.name: item for item in features}
            for name, version_id in target.dataset_versions.items():
                item = by_dataset.get(name)
                version = session.get(DatasetVersion, version_id)
                if (item is None or version is None or version.dataset_id != item.id or
                        not self.store.available(version.artifact_path)):
                    raise ValueError(f"dataset version for {name} is unavailable")
            for name, snapshot_id in target.feature_snapshots.items():
                item = by_feature.get(name)
                snapshot = session.get(FeatureSnapshot, snapshot_id)
                if (item is None or snapshot is None or snapshot.feature_id != item.id or
                        not self.store.available(snapshot.artifact_path) or
                        any(target.dataset_versions.get(dependency) != version_id
                            for dependency, version_id in snapshot.input_versions.items())):
                    raise ValueError(f"feature snapshot for {name} is incompatible or unavailable")
            for item in datasets:
                item.active_version_id = target.dataset_versions.get(item.name)
            for item in features:
                item.active_snapshot_id = target.feature_snapshots.get(item.name)
            restored = self._create_release(
                session, state, current, dict(target.dataset_versions),
                dict(target.feature_snapshots), "restore", restored_from_id=target.id,
            )
            return restored

    def storage_summary(self, *, collect: bool = False, min_age_seconds: int = 3600) -> dict:
        if min_age_seconds < 0:
            raise ValueError("minimum artifact age cannot be negative")
        with self.db.session() as session:
            session.get(CatalogState, 1, with_for_update=True)
            active_jobs = session.scalar(select(func.count(Job.id)).where(
                Job.status.in_(("running", "cancelling")))) or 0
            if collect and active_jobs:
                raise ValueError("cannot collect artifacts while tasks are running")
            referenced = {
                path for (path,) in session.execute(select(DatasetVersion.artifact_path))
            }
            referenced.update(path for (path,) in session.execute(
                select(FeatureSnapshot.artifact_path)))
            referenced.update(path for (path,) in session.execute(
                select(DatasetDelta.artifact_path).where(DatasetDelta.artifact_path.is_not(None))))
            referenced.update(path for (path,) in session.execute(
                select(Materialization.artifact_path).where(Materialization.artifact_path.is_not(None))))
            referenced_layouts = {path for (path,) in session.execute(
                select(DatasetLayout.artifact_dir).where(DatasetLayout.artifact_dir.is_not(None)))}
            for (result,) in session.execute(select(Job.result).where(Job.result.is_not(None))):
                if isinstance(result, dict) and result.get("artifact_path"):
                    referenced.add(result["artifact_path"])
            cutoff = time.time() - min_age_seconds
            total_count = total_bytes = orphan_count = orphan_bytes = 0
            for folder in ("datasets", "deltas", "features", "queries", "materializations"):
                root = self.store.root / folder
                if not root.exists():
                    continue
                for path in root.iterdir():
                    if (not path.is_file() or path.is_symlink() or
                            path.suffix not in {".parquet", ".partial"} or
                            not path.resolve().is_relative_to(self.store.root)):
                        continue
                    stat = path.stat()
                    total_count += 1
                    total_bytes += stat.st_size
                    if str(path) in referenced or stat.st_mtime > cutoff:
                        continue
                    orphan_count += 1
                    orphan_bytes += stat.st_size
                    if collect:
                        path.unlink()
            layout_root = self.store.root / "layouts"
            if layout_root.exists():
                for path in layout_root.iterdir():
                    if (not path.is_dir() or path.is_symlink() or
                            not path.resolve().is_relative_to(layout_root)):
                        continue
                    size = sum(file.stat().st_size for file in path.rglob("*") if file.is_file())
                    total_count += 1
                    total_bytes += size
                    if str(path) in referenced_layouts or path.stat().st_mtime > cutoff:
                        continue
                    orphan_count += 1
                    orphan_bytes += size
                    if collect:
                        shutil.rmtree(path)
            return {"artifact_count": total_count - (orphan_count if collect else 0),
                    "artifact_bytes": total_bytes - (orphan_bytes if collect else 0),
                    "orphan_count": orphan_count, "orphan_bytes": orphan_bytes,
                    "running_jobs": active_jobs, "collected": collect}

    def verify_storage(self) -> dict:
        """Check referenced immutable files after a backup or restore."""
        references: dict[str, str] = {}
        layouts: list[str] = []
        problems: list[str] = []

        def add(path: str | None, digest: str | None, label: str) -> None:
            if path is None:
                return
            if not digest:
                problems.append(f"{label}: missing checksum")
            elif path in references and references[path] != digest:
                problems.append(f"{label}: conflicting checksums for {path}")
            else:
                references[path] = digest

        with self.db.session() as session:
            for item in session.scalars(select(DatasetVersion)).all():
                add(item.artifact_path, item.sha256, f"dataset version {item.id}")
            for item in session.scalars(select(DatasetDelta)).all():
                add(item.artifact_path, item.sha256, f"dataset delta {item.version_id}")
            for item in session.scalars(select(FeatureSnapshot)).all():
                add(item.artifact_path, item.sha256, f"feature snapshot {item.id}")
            for item in session.scalars(select(Materialization)).all():
                add(item.artifact_path, item.sha256, f"materialization {item.id}")
            for item in session.scalars(select(DatasetLayout)).all():
                if item.artifact_dir:
                    layouts.append(item.artifact_dir)
            for (result,) in session.execute(select(Job.result).where(Job.result.is_not(None))):
                if isinstance(result, dict):
                    add(result.get("artifact_path"), result.get("sha256"), "job result")

        for path, digest in references.items():
            if not self.store.verify_artifact(path, digest):
                problems.append(f"artifact unavailable or checksum mismatch: {path}")
        for path in set(layouts):
            if not self.store.verify_layout(path):
                problems.append(f"layout unavailable or checksum mismatch: {path}")
        return {"ok": not problems, "artifacts_checked": len(references),
                "layouts_checked": len(set(layouts)), "problems": problems}

    def maintain_materializations(self, *, max_bytes: int | None = None,
                                  stale_seconds: int | None = None) -> dict:
        """Retire stale or over-budget cache entries between running tasks."""
        budget = self.settings.materialization_max_bytes if max_bytes is None else max_bytes
        stale_age = (self.settings.materialization_stale_seconds if stale_seconds is None
                     else stale_seconds)
        if budget < 0 or stale_age < 0:
            raise ValueError("materialization budget and retention must be nonnegative")
        retired_paths: list[str] = []
        with self.db.session() as session:
            session.get(CatalogState, 1, with_for_update=True)
            running = session.scalar(select(func.count(Job.id)).where(
                Job.status.in_(("running", "cancelling")))) or 0
            if running:
                return {"retired": 0, "reclaimed_bytes": 0,
                        "deferred_running_jobs": running, "unlink_failures": 0}
            state = session.get(CatalogState, 1)
            release = session.get(Release, state.active_release_id)
            active = {**release.dataset_versions, **release.feature_snapshots}
            items = session.scalars(select(Materialization).where(
                Materialization.status == "active").with_for_update()).all()
            sizes = {item.id: (Path(item.artifact_path).stat().st_size
                               if item.artifact_path and
                               self.store.available(item.artifact_path) else 0)
                     for item in items}
            cutoff = now() - timedelta(seconds=stale_age)
            retired = {item.id for item in items if (
                (any(active.get(name) != version for name, version in
                     item.input_versions.items()) and self._utc(item.updated_at) <= cutoff)
                or sizes[item.id] == 0
            )}
            total = sum(sizes[item.id] for item in items if item.id not in retired)
            for item in sorted((item for item in items if item.id not in retired),
                               key=lambda entry: (
                                   ((entry.observed_total_ms / entry.observed_queries)
                                    * entry.reuse_count - entry.reuse_total_ms
                                    - (entry.build_duration_ms or 0)) /
                                   max(sizes[entry.id], 1),
                                   self._utc(entry.updated_at),
                               )):
                if total <= budget:
                    break
                retired.add(item.id)
                total -= sizes[item.id]
            if retired:
                for item in items:
                    if item.id not in retired:
                        continue
                    if item.artifact_path:
                        retired_paths.append(item.artifact_path)
                    item.status = "retired"
                    item.artifact_path = None
                    item.sha256 = None
                    session.add(Event(kind="materialization.retired", subject_id=item.id,
                                      data={"artifact_bytes": sizes[item.id]}))
                for job in session.scalars(select(Job).where(
                    Job.kind == "query", Job.result.is_not(None)
                )).all():
                    if isinstance(job.result, dict) and job.result.get("materialization_id") in retired:
                        job.result = {key: value for key, value in job.result.items()
                                      if key not in {"artifact_path", "sha256"}}
        reclaimed = 0
        unlink_failures = 0
        for path in retired_paths:
            try:
                if self.store.available(path):
                    target = Path(path)
                    size = target.stat().st_size
                    target.unlink()
                    reclaimed += size
            except OSError:
                # Metadata is already retired; the orphan collector can retry.
                unlink_failures += 1
        return {"retired": len(retired), "reclaimed_bytes": reclaimed,
                "deferred_running_jobs": 0, "unlink_failures": unlink_failures}

    def add_source(self, name: str, kind: str, config: dict) -> Source:
        if kind not in {"file", "sqlite", "postgres", "http", "document"}:
            raise ValueError("unsupported source kind")
        allowed_keys = {
            "file": set(), "document": set(), "sqlite": {"path"},
            "postgres": {"database_url_env", "primary_key", "updated_at", "deleted_column"},
            "http": {"url", "primary_key", "updated_at", "deleted_column", "cursor_param",
                     "field_map", "records_path", "next_path"},
        }
        if set(config) - allowed_keys[kind]:
            raise ValueError("source configuration contains unsupported fields")
        if kind == "sqlite" and "path" not in config:
            raise ValueError("SQLite source needs a relative path")
        if kind == "postgres" and "database_url_env" not in config:
            raise ValueError("PostgreSQL source needs a credential reference")
        if kind == "postgres":
            cursor_keys = {"primary_key", "updated_at"}
            if any(key in config for key in cursor_keys | {"deleted_column"}):
                if not cursor_keys.issubset(config):
                    raise ValueError("PostgreSQL cursor needs a primary key and update timestamp")
                if any(not IDENTIFIER.fullmatch(str(config[key]))
                       for key in cursor_keys | ({"deleted_column"} & set(config))):
                    raise ValueError("invalid PostgreSQL cursor column")
        if kind == "http":
            parsed = urlparse(str(config.get("url", "")))
            allowed = [part.strip() for part in self.settings.http_allowed_hosts.split(",") if part.strip()]
            if parsed.scheme not in {"http", "https"} or parsed.hostname not in allowed:
                raise ValueError("HTTP source host is not enabled on this instance")
            field_map = config.get("field_map", {})
            if (not isinstance(field_map, dict) or
                    any(not isinstance(source_field, str) or not source_field or
                        not isinstance(catalog_field, str) or
                        not IDENTIFIER.fullmatch(catalog_field)
                        for source_field, catalog_field in field_map.items()) or
                    len(set(field_map.values())) != len(field_map)):
                raise ValueError("HTTP field mapping needs unique SQL field names")
            if any(not isinstance(config[key], str) or len(config[key]) > 200 or
                   any(not part for part in config[key].split("."))
                   for key in ("records_path", "next_path") if key in config):
                raise ValueError("HTTP response paths must be nonempty field paths")
            cursor_keys = {"primary_key", "updated_at"}
            if any(key in config for key in cursor_keys | {"deleted_column", "cursor_param"}):
                if not cursor_keys.issubset(config):
                    raise ValueError("HTTP cursor needs a primary key and update timestamp")
                if any(not IDENTIFIER.fullmatch(str(config[key]))
                       for key in cursor_keys | ({"deleted_column", "cursor_param"} & set(config))):
                    raise ValueError("invalid HTTP cursor field or parameter")
        with self.db.session() as session:
            source = Source(name=name, kind=kind, config=config)
            session.add(source)
            session.flush()
            return source

    def add_dataset(self, source_id: str, name: str, source_object: str) -> Dataset:
        if not IDENTIFIER.fullmatch(name) or name.lower().startswith("pg_"):
            raise ValueError("dataset name must be a plain SQL identifier")
        with self.db.session() as session:
            if session.get(Source, source_id) is None:
                raise ValueError("source does not exist")
            dataset = Dataset(source_id=source_id, name=name, source_object=source_object)
            session.add(dataset)
            session.flush()
            return dataset

    def queue_sync(self, dataset_id: str, idempotency_key: str | None = None,
                   force_full: bool = False) -> Job:
        with self.db.session() as session:
            if session.get(Dataset, dataset_id) is None:
                raise ValueError("dataset does not exist")
            return submit(
                session, "sync", "background", target_id=dataset_id,
                payload={"force_full": force_full},
                idempotency_key=idempotency_key, priority=10,
            )

    def queue_layout(self, dataset_id: str, partition_key: str,
                     *, automatic: bool = False) -> Job:
        """Build a verified physical layout for the current logical dataset version."""
        if not IDENTIFIER.fullmatch(partition_key):
            raise ValueError("partition key must be a plain column name")
        with self.db.session() as session:
            session.get(CatalogState, 1, with_for_update=True)
            dataset = session.get(Dataset, dataset_id)
            if dataset is None or dataset.active_version_id is None:
                raise ValueError("dataset has no published version")
            version = session.get(DatasetVersion, dataset.active_version_id)
            if partition_key not in {field["name"] for field in version.schema_json}:
                raise ValueError("partition key is not in the published schema")
            layout = session.scalar(select(DatasetLayout).where(
                DatasetLayout.version_id == version.id,
                DatasetLayout.partition_key == partition_key,
            ).with_for_update())
            if layout is not None and layout.status in {"active", "building"}:
                raise ValueError("layout is already active or building")
            if layout is not None and automatic:
                raise ValueError("rejected layout needs an explicit retry")
            if layout is None:
                layout = DatasetLayout(version_id=version.id, partition_key=partition_key,
                                       status="building", query_count=0)
                session.add(layout)
                session.flush()
            else:
                layout.status = "building"
                layout.error = None
            return submit(session, "organize", "background", target_id=layout.id,
                          priority=1, cpu_millicores=500, memory_mb=512)

    def poll_layout_candidates(self) -> list[str]:
        """Plan one layout when a large current dataset sees repeated equality filters."""
        with self.db.session() as session:
            state = session.get(CatalogState, 1)
            release = session.get(Release, state.active_release_id)
            active = dict(release.dataset_versions)
            versions = {version.id: version for version in session.scalars(
                select(DatasetVersion).where(DatasetVersion.id.in_(list(active.values())))
            ).all()}
            datasets = {item.id: item for item in session.scalars(select(Dataset)).all()}
            counts: dict[tuple[str, str], int] = {}
            jobs = session.scalars(select(Job).where(
                Job.kind == "query", Job.status == "succeeded",
                Job.created_at >= now() - timedelta(hours=1),
            ).order_by(Job.created_at.desc()).limit(300)).all()
            for job in jobs:
                payload = job.payload
                if not isinstance(payload, dict) or not payload.get("versions") or (
                    isinstance(job.result, dict) and job.result.get("cache_hit")
                ):
                    continue
                filters = equality_filters_by_source(payload["sql"], set(active))
                if not filters:
                    continue
                for name, version_id in active.items():
                    if payload["versions"].get(name) != version_id:
                        continue
                    version = versions[version_id]
                    if version.row_count < 10000:
                        continue
                    fields = {field["name"] for field in version.schema_json}
                    for key in filters.get(name, set()) & fields:
                        counts[(version.dataset_id, key)] = (
                            counts.get((version.dataset_id, key), 0) + 1
                        )
        for (dataset_id, key), count in sorted(counts.items(), key=lambda item: -item[1]):
            if count < 20 or dataset_id not in datasets:
                continue
            try:
                return [self.queue_layout(dataset_id, key, automatic=True).id]
            except ValueError:
                continue
        return []

    def poll_file_sources(
        self, seen: dict[str, tuple[int, int, str]]
    ) -> tuple[dict[str, tuple[int, int, str]], list[str]]:
        current: dict[str, tuple[int, int, str]] = {}
        scheduled: list[str] = []
        with self.db.session() as session:
            records = session.execute(
                select(Dataset, Source).join(Source, Dataset.source_id == Source.id)
                .where(Source.kind.in_(("file", "document")))
            ).all()
            for dataset, _source in records:
                try:
                    state = stable_file_state(self.settings.source_root, dataset.source_object)
                except (OSError, ValueError):
                    continue
                if state is None:
                    continue
                current[dataset.id] = state
                if seen.get(dataset.id) != state:
                    continue
                version = (session.get(DatasetVersion, dataset.active_version_id)
                           if dataset.active_version_id else None)
                if version and version.source_fingerprint == state[2]:
                    continue
                key = f"file-sync:{dataset.id}:{dataset.active_version_id or 'none'}:{state[2]}"
                existing = session.scalar(select(Job.id).where(Job.idempotency_key == key))
                if existing:
                    continue
                job = submit(session, "sync", "background", target_id=dataset.id,
                             idempotency_key=key, priority=10)
                scheduled.append(job.id)
        return current, scheduled

    def poll_remote_sources(self) -> list[str]:
        """Submit due database and HTTP synchronization without duplicate queued work."""
        scheduled: list[str] = []
        with self.db.session() as session:
            session.get(CatalogState, 1, with_for_update=True)
            records = session.execute(
                select(Dataset, Source).join(Source, Dataset.source_id == Source.id)
                .where(Source.kind.in_(("postgres", "sqlite", "http")))
            ).all()
            current = now()
            for dataset, source in records:
                active = session.scalar(select(Job.id).where(
                    Job.kind == "sync", Job.target_id == dataset.id,
                    Job.status.in_(("pending", "running", "cancelling")),
                ).limit(1))
                if active:
                    continue
                recent = session.scalar(select(Job.updated_at).where(
                    Job.kind == "sync", Job.target_id == dataset.id,
                ).order_by(Job.updated_at.desc()).limit(1))
                if recent and (current - self._utc(recent)).total_seconds() < (
                    self.settings.remote_poll_seconds
                ):
                    continue
                full = False
                if source.kind in {"postgres", "http"} and source.config.get("updated_at"):
                    last_full = session.scalar(select(Job.updated_at).where(
                        Job.kind == "sync", Job.target_id == dataset.id,
                        Job.status == "succeeded",
                        Job.result["source_read"].as_string() == "snapshot",
                    ).order_by(Job.updated_at.desc()).limit(1))
                    full = (last_full is None or
                            (current - self._utc(last_full)).total_seconds() >=
                            (self.settings.postgres_full_reconcile_seconds if source.kind == "postgres"
                             else self.settings.http_full_reconcile_seconds))
                job = submit(session, "sync", "background", target_id=dataset.id,
                             payload={"force_full": full}, priority=10)
                scheduled.append(job.id)
        return scheduled

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    @classmethod
    def feature_expired(cls, feature: Feature, snapshot: FeatureSnapshot | None,
                        at: datetime | None = None) -> bool:
        if snapshot is None:
            return False
        if snapshot.next_refresh_at is not None:
            return cls._utc(snapshot.next_refresh_at) <= (at or now())
        if feature.refresh_interval_seconds is None:
            return False
        return cls._utc(snapshot.created_at) + timedelta(
            seconds=feature.refresh_interval_seconds
        ) <= (at or now())

    def poll_time_features(self) -> list[str]:
        """Refresh time-dependent features even when their input versions are unchanged."""
        scheduled: list[str] = []
        with self.db.session() as session:
            state = session.get(CatalogState, 1, with_for_update=True)
            release = session.get(Release, state.active_release_id)
            current = now()
            for feature in session.scalars(
                select(Feature).outerjoin(FeatureMaintenance).where(or_(
                    Feature.refresh_interval_seconds.is_not(None),
                    FeatureMaintenance.mode == "rolling_sum",
                    FeatureMaintenance.mode == "window_sql",
                ))
            ).all():
                snapshot_id = release.feature_snapshots.get(feature.name)
                snapshot = session.get(FeatureSnapshot, snapshot_id) if snapshot_id else None
                if snapshot and not self.feature_expired(feature, snapshot, current):
                    continue
                if not all(name in release.dataset_versions for name in feature.dependencies):
                    continue
                active = session.scalar(select(Job.id).where(
                    Job.kind == "refresh", Job.target_id == feature.id,
                    Job.status.in_(("pending", "running", "cancelling")),
                ).limit(1))
                if active:
                    continue
                latest = session.scalar(select(Job).where(
                    Job.kind == "refresh", Job.target_id == feature.id,
                ).order_by(Job.created_at.desc()).limit(1))
                if (latest and latest.status == "failed" and
                        (current - self._utc(latest.updated_at)).total_seconds() < 30):
                    continue
                job = submit(session, "refresh", "background", target_id=feature.id,
                             priority=5)
                scheduled.append(job.id)
        return scheduled

    def add_feature(self, name: str, sql: str | None, dependencies: list[str],
                    entity_keys: list[str], maintenance: dict | None = None,
                    refresh_interval_seconds: int | None = None) -> Feature:
        if not IDENTIFIER.fullmatch(name) or not dependencies or not entity_keys:
            raise ValueError("feature needs an identifier, dependencies, and entity keys")
        if refresh_interval_seconds is not None and refresh_interval_seconds < 1:
            raise ValueError("refresh interval must be positive")
        if maintenance and refresh_interval_seconds is not None:
            raise ValueError("choose either a declared window or a refresh interval")
        if maintenance:
            if maintenance.get("mode") == "append_sum_count":
                if (set(maintenance) != {"mode", "measure", "sum_alias", "count_alias"} or
                        len(dependencies) != 1):
                    raise ValueError("invalid append aggregate feature definition")
                sql = aggregate_sql(dependencies[0], entity_keys, maintenance["measure"],
                                    maintenance["sum_alias"], maintenance["count_alias"])
            elif maintenance.get("mode") == "rolling_sum":
                if (set(maintenance) != {"mode", "measure", "event_time", "window_seconds",
                                         "sum_alias", "count_alias"} or len(dependencies) != 1):
                    raise ValueError("invalid rolling feature definition")
                sql = rolling_sum_sql(dependencies[0], entity_keys, maintenance["measure"],
                                      maintenance["event_time"], maintenance["window_seconds"],
                                      maintenance["sum_alias"], maintenance["count_alias"])
            elif maintenance.get("mode") == "python":
                if len(dependencies) != 1 or sql is not None:
                    raise ValueError("Python operator needs one input dataset and no SQL")
                validate_operator({key: value for key, value in maintenance.items()
                                   if key != "mode"}, entity_keys)
            elif maintenance.get("mode") == "window_sql":
                if set(maintenance) != {"mode", "windows"} or not sql:
                    raise ValueError("window SQL needs a query and window rules")
                validate_window_rules(maintenance["windows"], dependencies)
                validate_window_query(sql)
            else:
                raise ValueError("unsupported feature maintenance mode")
        elif not sql:
            raise ValueError("feature SQL is required")
        with self.db.session() as session:
            datasets = session.scalars(select(Dataset).where(Dataset.name.in_(dependencies))).all()
            if {d.name for d in datasets} != set(dependencies):
                raise ValueError("unknown feature dependency")
            if maintenance and maintenance["mode"] == "rolling_sum" and datasets[0].active_version_id:
                version = session.get(DatasetVersion, datasets[0].active_version_id)
                fields = {field["name"] for field in version.schema_json}
                required = {*entity_keys, maintenance["measure"], maintenance["event_time"]}
                if missing := required - fields:
                    raise ValueError(f"rolling feature columns are missing: {', '.join(sorted(missing))}")
            if maintenance and maintenance["mode"] == "python" and datasets[0].active_version_id:
                version = session.get(DatasetVersion, datasets[0].active_version_id)
                fields = {field["name"]: field["type"] for field in version.schema_json}
                rule = {key: value for key, value in maintenance.items() if key != "mode"}
                validate_operator(rule, entity_keys, set(fields))
                if fields[rule["text_column"]] not in {"string", "large_string"}:
                    raise ValueError("Python operator text column must contain strings")
            if maintenance and maintenance["mode"] == "window_sql":
                fields = {}
                for dataset in datasets:
                    if dataset.active_version_id:
                        version = session.get(DatasetVersion, dataset.active_version_id)
                        fields[dataset.name] = {field["name"] for field in version.schema_json}
                validate_window_rules(maintenance["windows"], dependencies, fields)
            if session.scalar(select(Dataset).where(Dataset.name == name)):
                raise ValueError("feature name conflicts with dataset")
            if not maintenance or maintenance["mode"] != "python":
                validate_query(sql, set(dependencies))
            feature = Feature(name=name, sql=sql, dependencies=dependencies,
                              entity_keys=entity_keys,
                              refresh_interval_seconds=refresh_interval_seconds)
            session.add(feature)
            session.flush()
            if maintenance:
                session.add(FeatureMaintenance(feature_id=feature.id, mode=maintenance["mode"],
                                               config={key: value for key, value in
                                                       maintenance.items() if key != "mode"}))
            return feature

    def queue_refresh(self, feature_id: str, idempotency_key: str | None = None) -> Job:
        with self.db.session() as session:
            feature = session.get(Feature, feature_id)
            if feature is None:
                raise ValueError("feature does not exist")
            rule = session.get(FeatureMaintenance, feature_id)
            return submit(
                session, "refresh", "background", target_id=feature_id,
                idempotency_key=idempotency_key, priority=5,
                payload={"force": feature.refresh_interval_seconds is not None or
                         (rule is not None and rule.mode in {"rolling_sum", "window_sql"})},
            )

    def queue_query(self, sql: str, idempotency_key: str | None = None,
                    *, cpu_millicores: int | None = None,
                    memory_mb: int | None = None,
                    expected_versions: dict[str, str] | None = None,
                    question: str | None = None) -> Job:
        cpu_millicores = 500 if cpu_millicores is None else cpu_millicores
        memory_mb = 512 if memory_mb is None else memory_mb
        if (cpu_millicores < 1 or memory_mb < 1 or
                cpu_millicores > self.settings.resource_cpu_millicores or
                memory_mb > self.settings.resource_memory_mb):
            raise ValueError("query resource reservation is outside the instance budget")
        with self.db.session() as session:
            release, versions = self._query_release(session, sql)
            if expected_versions is not None:
                referenced = validate_query(sql, set(versions))
                if any(expected_versions.get(name) != versions[name] for name in referenced):
                    raise ValueError("分析所用数据已更新，请重新生成计划后执行")
            return submit(
                session, "query", "query",
                payload={"sql": sql, "versions": versions, "release_id": release.id,
                         "question": question},
                idempotency_key=idempotency_key, priority=20,
                cpu_millicores=cpu_millicores, memory_mb=memory_mb,
            )

    def _query_release(self, session: Session, sql: str) -> tuple[Release, dict]:
        datasets = session.scalars(select(Dataset)).all()
        features = session.scalars(select(Feature)).all()
        available = {item.name for item in datasets + features}
        referenced = validate_query(sql, available)
        state = session.get(CatalogState, 1)
        release = session.get(Release, state.active_release_id)
        versions = {**release.dataset_versions, **release.feature_snapshots}
        missing = referenced - set(versions)
        if missing:
            stale = missing & {item.name for item in features}
            if stale:
                raise ValueError(f"feature {', '.join(sorted(stale))} is stale; refresh is pending")
            raise ValueError(f"dataset {', '.join(sorted(missing))} is not published")
        for feature in features:
            if feature.name in referenced:
                snapshot = session.get(FeatureSnapshot, versions[feature.name])
                if self.feature_expired(feature, snapshot):
                    raise ValueError(f"feature {feature.name} has expired; refresh required")
        return release, versions

    def create_workflow(self, name: str, steps: list[dict],
                        idempotency_key: str | None = None) -> Workflow:
        if not name.strip() or len(name) > 160 or not 1 <= len(steps) <= 100:
            raise ValueError("workflow needs a name and 1 to 100 steps")
        steps = [dict(step) for step in steps]
        keys = [step.get("key") for step in steps]
        if (any(not isinstance(key, str) or not IDENTIFIER.fullmatch(key) or len(key) > 80
                for key in keys) or len(set(keys)) != len(keys)):
            raise ValueError("workflow step keys must be unique SQL-style identifiers")
        with self.db.session() as session:
            session.get(CatalogState, 1, with_for_update=True)
            if idempotency_key:
                previous = session.scalar(select(Workflow).where(
                    Workflow.idempotency_key == idempotency_key
                ))
                if previous:
                    return previous
            datasets = {item.id: item for item in session.scalars(select(Dataset)).all()}
            features = {item.id: item for item in session.scalars(select(Feature)).all()}
            maintenance_modes = {item.feature_id: item.mode for item in
                                 session.scalars(select(FeatureMaintenance)).all()}
            available = {item.name for item in datasets.values()} | {
                item.name for item in features.values()
            }
            sync_steps: dict[str, str] = {}
            query_references: set[str] = set()
            for step in steps:
                kind, target = step.get("kind"), step.get("target_id")
                if kind == "sync":
                    if target not in datasets:
                        raise ValueError("workflow sync target is unknown")
                    sync_steps[step["key"]] = datasets[target].name
                elif kind == "refresh":
                    if target not in features:
                        raise ValueError("workflow refresh target is unknown")
                elif kind == "query":
                    if target is not None or not isinstance(step.get("sql"), str):
                        raise ValueError("workflow query needs SQL and no target")
                    query_references.update(validate_query(step["sql"], available))
                else:
                    raise ValueError("unsupported workflow step kind")
            release = session.get(Release, session.get(CatalogState, 1).active_release_id)
            selected_refreshes = {step["target_id"] for step in steps if step["kind"] == "refresh"}
            synchronized = set(sync_steps.values())
            for feature in features.values():
                required_by_query = (feature.name in query_references and
                                     (feature.name not in release.feature_snapshots or
                                      self.feature_expired(feature, session.get(
                                          FeatureSnapshot, release.feature_snapshots[feature.name]
                                      ))))
                if (feature.id not in selected_refreshes and
                        (set(feature.dependencies) & synchronized or required_by_query)):
                    key = f"refresh_{feature.id[:12]}"
                    if key in keys:
                        raise ValueError("workflow step key conflicts with an automatic refresh")
                    steps.append({"key": key, "kind": "refresh", "target_id": feature.id})
                    keys.append(key)
            if len(steps) > 100:
                raise ValueError("workflow exceeds 100 steps after planning dependent refreshes")
            refresh_targets = [step["target_id"] for step in steps
                               if step["kind"] == "refresh"]
            if (len(set(sync_steps.values())) != len(sync_steps) or
                    len(set(refresh_targets)) != len(refresh_targets)):
                raise ValueError("workflow cannot update the same dataset or feature twice")
            available_datasets = set(release.dataset_versions) | synchronized
            for target in refresh_targets:
                missing_inputs = set(features[target].dependencies) - available_datasets
                if missing_inputs:
                    raise ValueError(f"feature inputs are not published or synchronized: "
                                     f"{', '.join(sorted(missing_inputs))}")
            missing_query_inputs = query_references - (
                available_datasets | set(release.feature_snapshots) |
                {features[target].name for target in refresh_targets}
            )
            if missing_query_inputs:
                raise ValueError(f"query inputs are not published or synchronized: "
                                 f"{', '.join(sorted(missing_query_inputs))}")
            dependencies: dict[str, set[str]] = {}
            all_keys = set(keys)
            for step in steps:
                key = step["key"]
                required = set(step.get("depends_on") or [])
                if not required <= all_keys or key in required:
                    raise ValueError("workflow has an unknown or self dependency")
                if step["kind"] == "refresh":
                    inputs = set(features[step["target_id"]].dependencies)
                    required.update(node for node, dataset in sync_steps.items() if dataset in inputs)
                elif step["kind"] == "query":
                    required.update(node["key"] for node in steps if node["kind"] != "query")
                dependencies[key] = required
            remaining = {key: set(required) for key, required in dependencies.items()}
            while remaining:
                ready = {key for key, required in remaining.items() if not required}
                if not ready:
                    raise ValueError("workflow dependencies contain a cycle")
                remaining = {key: required - ready for key, required in remaining.items()
                             if key not in ready}
            workflow = Workflow(name=name.strip(), idempotency_key=idempotency_key)
            session.add(workflow)
            session.flush()
            jobs: dict[str, Job] = {}
            for step in steps:
                kind = step["kind"]
                cpu = int(step.get("cpu_millicores", 500))
                memory = int(step.get("memory_mb", 512))
                if cpu > self.settings.resource_cpu_millicores or memory > self.settings.resource_memory_mb:
                    raise ValueError("workflow step reservation exceeds instance budget")
                payload = ({"sql": step["sql"]} if kind == "query" else
                           {"force_full": bool(step.get("force_full", False))} if kind == "sync" else
                           {"force": (features[step["target_id"]].refresh_interval_seconds is not None or
                                      maintenance_modes.get(step["target_id"]) == "rolling_sum")})
                jobs[step["key"]] = submit(
                    session, kind, "query" if kind == "query" else "background",
                    target_id=step.get("target_id"), payload=payload,
                    priority=20 if kind == "query" else 10 if kind == "sync" else 5,
                    cpu_millicores=cpu, memory_mb=memory,
                    workflow_id=workflow.id, node_key=step["key"],
                )
            for key, required in dependencies.items():
                for parent in required:
                    session.add(JobDependency(job_id=jobs[key].id,
                                              depends_on_id=jobs[parent].id))
            session.add(Event(kind="workflow.submitted", subject_id=workflow.id,
                              data={"steps": list(jobs)}))
            return workflow

    def _pinned_artifacts(self, versions: dict[str, str]) -> dict[str, str]:
        with self.db.session() as session:
            paths: dict[str, str] = {}
            for name, version_id in versions.items():
                version = session.get(DatasetVersion, version_id)
                if version is None:
                    version = session.get(FeatureSnapshot, version_id)
                if version is None:
                    raise ValueError(f"pinned version for {name} is unavailable")
                paths[name] = version.artifact_path
            return paths

    def _query_sources(self, sql: str, versions: dict[str, str]) -> tuple[dict, list[str]]:
        sources: dict = self._pinned_artifacts(versions)
        filters = equality_filters_by_source(sql, set(versions))
        if not filters:
            return sources, []
        keys = set().union(*filters.values())
        selected = []
        with self.db.session() as session:
            layouts = session.scalars(select(DatasetLayout).where(
                DatasetLayout.version_id.in_(list(versions.values())),
                DatasetLayout.partition_key.in_(keys),
                DatasetLayout.status == "active",
            )).all()
            by_version = {(item.version_id, item.partition_key): item for item in layouts
                          if item.artifact_dir and self.store.available_layout(item.artifact_dir)}
        for name, version_id in versions.items():
            layout = next((by_version[(version_id, key)] for key in sorted(filters.get(name, set()))
                           if (version_id, key) in by_version), None)
            if layout is not None:
                sources[name] = self.store.open_layout(layout.artifact_dir)
                selected.append(layout.id)
        return sources, selected

    def _sync(self, job: Job, generation: int) -> dict:
        with self.db.session() as session:
            dataset = session.get(Dataset, job.target_id)
            if dataset is None:
                raise ValueError("dataset no longer exists")
            source = session.get(Source, dataset.source_id)
            if source is None:
                raise ValueError("source no longer exists")
            before_id = dataset.active_version_id
            previous_version = session.get(DatasetVersion, before_id) if before_id else None
            previous_schema = previous_version.schema_json if previous_version else []
            source_kind, source_config, source_object = source.kind, source.config, dataset.source_object
        previous_table = (self.store.read_table(previous_version.artifact_path)
                          if previous_version else None)
        if (source_kind == "postgres" and not job.payload.get("force_full") and
                previous_table is not None and
                source_config.get("updated_at") and previous_version.source_cursor):
            snapshot = read_postgres_changes(source_config, source_object,
                                             previous_table, previous_version.source_cursor)
        elif (source_kind == "http" and not job.payload.get("force_full") and
              previous_table is not None and source_config.get("updated_at") and
              previous_version.source_cursor):
            allowed = [value.strip() for value in self.settings.http_allowed_hosts.split(",")
                       if value.strip()]
            snapshot = read_http_changes(source_config, previous_table,
                                         previous_version.source_cursor, allowed)
        else:
            snapshot = read_snapshot(source_kind, source_config, source_object, self.settings)
        if (previous_table is not None and snapshot.table.num_rows == 0 and
                snapshot.table.column_names == ["_empty"]):
            snapshot = replace(snapshot, table=previous_table.slice(0, 0))
        current_schema = {field.name: str(field.type) for field in snapshot.table.schema}
        previous_fields = ([] if previous_version and previous_version.row_count == 0 and
                           len(previous_schema) == 1 and previous_schema[0]["name"] == "_empty"
                           else previous_schema)
        changed_fields = [field["name"] for field in previous_fields
                          if current_schema.get(field["name"]) != field["type"]]
        if changed_fields:
            raise ValueError(f"incompatible schema change: {', '.join(changed_fields)}")
        schema_unchanged = previous_schema == [
            {"name": field.name, "type": str(field.type)} for field in snapshot.table.schema
        ]
        if (previous_version and previous_version.source_fingerprint == snapshot.fingerprint and
                previous_version.source_cursor == snapshot.source_cursor and schema_unchanged):
            with self.db.session() as session:
                _state, parent_release = self._locked_release(session)
                current_job = session.get(Job, job.id, with_for_update=True)
                dataset = session.get(Dataset, job.target_id, with_for_update=True)
                if current_job.status != "running" or current_job.lease_generation != generation:
                    raise ConflictError("job lease changed before completion")
                if (dataset.active_version_id != before_id or
                        parent_release.dataset_versions.get(dataset.name) != before_id):
                    raise ConflictError("dataset changed while synchronization was running")
                existing_profile = session.get(DatasetProfile, before_id)
                if (existing_profile is None or
                        any("distribution" not in field for field in existing_profile.fields)):
                    refreshed_profile = profile_table(snapshot.table)
                    if existing_profile is None:
                        session.add(DatasetProfile(version_id=before_id, **refreshed_profile))
                    else:
                        existing_profile.sampled_rows = refreshed_profile["sampled_rows"]
                        existing_profile.fields = refreshed_profile["fields"]
                result = {"version_id": before_id, "changed": False,
                          "row_count": previous_version.row_count, "source_read": snapshot.mode}
                if not complete(session, job, generation, result=result):
                    raise ConflictError("job lease changed before completion")
                return result
        profile = profile_table(snapshot.table)
        delta_table: pa.Table | None = None
        if (previous_table is not None and
                snapshot.table.num_rows >= previous_table.num_rows and
                snapshot.table.slice(0, previous_table.num_rows).equals(previous_table)):
            delta_table = snapshot.table.slice(previous_table.num_rows)
        delta_path, delta_sha = (self.store.write_table("deltas", delta_table)
                                 if delta_table is not None and delta_table.num_rows else
                                 (None, None))
        path, digest = self.store.write_table("datasets", snapshot.table)
        with self.db.session() as session:
            state, parent_release = self._locked_release(session)
            current_job = session.get(Job, job.id, with_for_update=True)
            dataset = session.get(Dataset, job.target_id, with_for_update=True)
            if current_job.status != "running" or current_job.lease_generation != generation:
                raise ConflictError("job lease changed before publication")
            if (dataset.active_version_id != before_id or
                    parent_release.dataset_versions.get(dataset.name) != before_id):
                raise ConflictError("dataset changed while synchronization was running")
            number = (session.scalar(select(func.max(DatasetVersion.number))
                                     .where(DatasetVersion.dataset_id == dataset.id)) or 0) + 1
            version = DatasetVersion(
                dataset_id=dataset.id,
                number=number,
                artifact_path=path,
                sha256=digest,
                row_count=snapshot.table.num_rows,
                schema_json=[{"name": f.name, "type": str(f.type)} for f in snapshot.table.schema],
                source_fingerprint=snapshot.fingerprint,
                source_cursor=snapshot.source_cursor,
            )
            session.add(version)
            session.flush()
            session.add(DatasetProfile(version_id=version.id, **profile))
            session.add(DatasetDelta(version_id=version.id, base_version_id=before_id,
                                     append_only=delta_table is not None,
                                     artifact_path=delta_path, sha256=delta_sha,
                                     row_count=delta_table.num_rows if delta_table is not None else 0))
            dataset.active_version_id = version.id
            session.add(Event(
                kind="dataset.published", subject_id=dataset.id,
                data={"version_id": version.id, "row_count": version.row_count},
            ))
            release_datasets = {**parent_release.dataset_versions, dataset.name: version.id}
            release_features = {}
            for name, snapshot_id in parent_release.feature_snapshots.items():
                active = session.get(FeatureSnapshot, snapshot_id)
                if all(release_datasets.get(dependency) == dependency_version
                       for dependency, dependency_version in active.input_versions.items()):
                    release_features[name] = snapshot_id
            self._create_release(session, state, parent_release, release_datasets,
                                 release_features, f"dataset:{dataset.name}")
            # Dependent definitions become stale; refresh requests coalesce per version.
            for feature in session.scalars(select(Feature)).all():
                if dataset.name in feature.dependencies:
                    key = f"feature:{feature.id}:dataset:{dataset.id}:version:{version.id}"
                    submit(session, "refresh", "background", target_id=feature.id,
                           idempotency_key=key, priority=5)
            result = {"version_id": version.id, "changed": True,
                      "row_count": version.row_count, "source_read": snapshot.mode}
            if not complete(session, job, generation, result=result):
                raise ConflictError("job lease changed before completion")
            if current_job.status != "succeeded":
                raise ConflictError("job was cancelled before publication")
            return result

    def _refresh(self, job: Job, generation: int) -> dict:
        with self.db.session() as session:
            feature = session.get(Feature, job.target_id)
            if feature is None:
                raise ValueError("feature no longer exists")
            datasets = session.scalars(
                select(Dataset).where(Dataset.name.in_(feature.dependencies))
            ).all()
            if len(datasets) != len(feature.dependencies) or any(not d.active_version_id for d in datasets):
                raise ValueError("feature inputs are not published")
            versions = {d.name: d.active_version_id for d in datasets}
            before_id = feature.active_snapshot_id
            prior = None
            if before_id:
                prior = session.get(FeatureSnapshot, before_id)
                if (prior.input_versions == versions and
                        not job.payload.get("force") and
                        not self.feature_expired(feature, prior)):
                    result = {"snapshot_id": prior.id, "row_count": prior.row_count,
                              "changed": False}
                    if not complete(session, job, generation, result=result):
                        raise ConflictError("job lease changed before completion")
                    return result
            sql, keys = feature.sql, list(feature.entity_keys)
            maintenance = session.get(FeatureMaintenance, feature.id)
            mode = maintenance.mode if maintenance else "full"
            rule = dict(maintenance.config) if maintenance else None
            delta = (session.get(DatasetDelta, versions[feature.dependencies[0]])
                     if mode == "append_sum_count" and before_id else None)
            source_version = (session.get(DatasetVersion, versions[feature.dependencies[0]])
                              if delta else None)
            incremental = bool(
                mode == "append_sum_count" and prior and delta and delta.append_only and
                delta.artifact_path and
                delta.row_count * 4 <= source_version.row_count and
                prior.input_versions.get(feature.dependencies[0]) == delta.base_version_id
            )
            prior_path = prior.artifact_path if incremental else None
            delta_path = delta.artifact_path if incremental else None
        next_due_at = None
        if mode == "rolling_sum":
            source = feature.dependencies[0]
            anchor = now()
            source_table = self.store.read_table(self._pinned_artifacts(versions)[source])
            table = run_sql(
                rolling_sum_sql(source, keys, rule["measure"], rule["event_time"],
                                rule["window_seconds"], rule["sum_alias"],
                                rule["count_alias"], anchor),
                {source: source_table}, self.store,
            )
            due = run_sql(
                rolling_next_due_sql(source, keys, rule["measure"], rule["event_time"],
                                     rule["window_seconds"], rule["sum_alias"],
                                     rule["count_alias"], anchor),
                {source: source_table}, self.store,
            ).column("next_due")[0].as_py()
            next_due_at = self._utc(due) if due is not None else None
        elif mode == "window_sql":
            anchor = now()
            paths = self._pinned_artifacts(versions)
            inputs: dict[str, str | pa.Table] = dict(paths)
            due_times = []
            for source, spec in rule["windows"].items():
                source_table = self.store.read_table(paths[source])
                inputs[source] = run_sql(
                    window_filter_sql(source, spec["event_time"],
                                      spec["window_seconds"], anchor),
                    {source: source_table}, self.store,
                )
                due = run_sql(
                    window_next_due_sql(source, spec["event_time"],
                                        spec["window_seconds"], anchor),
                    {source: source_table}, self.store,
                ).column("next_due")[0].as_py()
                if due is not None:
                    due_times.append(self._utc(due))
            table = run_sql(sql, inputs, self.store)
            next_due_at = min(due_times) if due_times else None
        elif incremental:
            delta_sql = aggregate_sql("delta_source", keys, rule["measure"],
                                      rule["sum_alias"], rule["count_alias"])
            table = run_sql(
                merge_sql(keys, rule["sum_alias"], rule["count_alias"], delta_sql),
                {"prior": prior_path, "delta_source": delta_path}, self.store,
            )
        elif mode == "python":
            source = feature.dependencies[0]
            input_table = self.store.read_table(self._pinned_artifacts(versions)[source])
            table = run_operator(rule, keys, input_table)
        else:
            paths = self._pinned_artifacts(versions)
            table = run_sql(sql, paths, self.store)
        if any(key not in table.column_names for key in keys):
            raise ValueError("feature result is missing an entity key")
        if any(table.column(key).null_count for key in keys):
            raise ValueError("feature entity keys cannot be null")
        key_tuples = list(zip(*(table.column(key).to_pylist() for key in keys)))
        if len(set(key_tuples)) != table.num_rows:
            raise ValueError("feature entity keys are not unique")
        path, digest = self.store.write_table("features", table)
        with self.db.session() as session:
            state, parent_release = self._locked_release(session)
            current_job = session.get(Job, job.id, with_for_update=True)
            datasets = session.scalars(
                select(Dataset).where(Dataset.name.in_(versions)).with_for_update()
            ).all()
            feature = session.get(Feature, job.target_id, with_for_update=True)
            if current_job.status != "running" or current_job.lease_generation != generation:
                raise ConflictError("job lease changed before publication")
            if feature.active_snapshot_id != before_id:
                raise ConflictError("feature changed while refreshing")
            for dataset in datasets:
                if (dataset.active_version_id != versions[dataset.name] or
                        parent_release.dataset_versions.get(dataset.name) != versions[dataset.name]):
                    raise ConflictError("feature input changed while refreshing")
            snapshot = FeatureSnapshot(
                feature_id=feature.id,
                input_versions=versions,
                artifact_path=path,
                sha256=digest,
                row_count=table.num_rows,
                next_refresh_at=next_due_at,
            )
            session.add(snapshot)
            session.flush()
            feature.active_snapshot_id = snapshot.id
            session.add(Event(kind="feature.published", subject_id=feature.id,
                              data={"snapshot_id": snapshot.id, "row_count": table.num_rows}))
            self._create_release(session, state, parent_release,
                                 dict(parent_release.dataset_versions),
                                 {**parent_release.feature_snapshots, feature.name: snapshot.id},
                                 f"feature:{feature.name}")
            result = {"snapshot_id": snapshot.id, "row_count": table.num_rows,
                      "maintenance": "window_sql" if mode == "window_sql" else
                      "python" if mode == "python" else
                      "rolling_window" if mode == "rolling_sum" else
                      "incremental" if incremental else "full",
                      "next_refresh_at": next_due_at.isoformat() if next_due_at else None}
            if not complete(session, job, generation, result=result):
                raise ConflictError("job lease changed before completion")
            if current_job.status != "succeeded":
                raise ConflictError("job was cancelled before publication")
            return result

    def _query(self, job: Job, generation: int) -> dict:
        payload = job.payload
        if "versions" not in payload:
            with self.db.session() as session:
                current = session.get(Job, job.id, with_for_update=True)
                if (current.status != "running" or current.lease_generation != generation):
                    raise ConflictError("query lease changed before input binding")
                if "versions" not in current.payload:
                    release, versions = self._query_release(session, current.payload["sql"])
                    current.payload = {**current.payload, "versions": versions,
                                       "release_id": release.id}
                payload = dict(current.payload)
        key = materialization_signature(payload["sql"], payload["versions"])
        if key is not None:
            with self.db.session() as session:
                cached = session.scalar(select(Materialization).where(
                    Materialization.signature == key[0], Materialization.status == "active"
                ))
                if (cached is not None and cached.preview is not None and
                        cached.artifact_path and self.store.available(cached.artifact_path)):
                    return {**cached.preview, "artifact_path": cached.artifact_path,
                            "sha256": cached.sha256, "versions": payload["versions"],
                            "cache_hit": True, "materialization_id": cached.id}
        sources, layouts = self._query_sources(payload["sql"], payload["versions"])
        table = run_sql(payload["sql"], sources, self.store)
        preview = table_preview(table, self.settings.max_query_rows)
        path, digest = self.store.write_table("queries", table)
        return {**preview, "artifact_path": path, "sha256": digest,
                "versions": payload["versions"], "cache_hit": False,
                "layout_ids": layouts}

    def _organize(self, job: Job, generation: int) -> dict:
        with self.db.session() as session:
            layout = session.get(DatasetLayout, job.target_id)
            if layout is None or layout.status != "building":
                raise ValueError("layout candidate is unavailable")
            version = session.get(DatasetVersion, layout.version_id)
            dataset = session.get(Dataset, version.dataset_id)
            if dataset.active_version_id != version.id:
                raise ValueError("dataset version changed before layout build")
            key = layout.partition_key
            source_path = version.artifact_path
        started = time.monotonic()
        source = self.store.read_table(source_path)
        path, manifest = self.store.write_layout(source, key)
        scanner = self.store.open_layout(path)
        rebuilt = scanner.to_table()
        if not equivalent_results("SELECT * FROM source", source, rebuilt, self.store):
            raise ValueError("organized layout does not match the source version")
        values = [value for value in source.column(key).to_pylist() if value is not None]
        if not values or manifest["partition_count"] < 2:
            raise ValueError("layout needs at least two nonempty partitions")
        sample = str(values[0]).replace("'", "''")
        probe = f'SELECT COUNT(*) AS n FROM source WHERE "{key}" = \'{sample}\''
        baseline_ms, organized_ms = _measure_layout_probe(probe, source_path, path, self.store)
        source_bytes = Path(source_path).stat().st_size
        beneficial = (organized_ms < baseline_ms * 0.95 and
                      manifest["bytes"] <= source_bytes * 2)
        build_ms = round((time.monotonic() - started) * 1000, 1)
        with self.db.session() as session:
            _state, release = self._locked_release(session)
            current = session.get(Job, job.id, with_for_update=True)
            layout = session.get(DatasetLayout, job.target_id, with_for_update=True)
            dataset = session.get(Dataset, version.dataset_id, with_for_update=True)
            if current.status != "running" or current.lease_generation != generation:
                raise ConflictError("layout lease changed before publication")
            if (layout.status != "building" or dataset.active_version_id != version.id or
                    release.dataset_versions.get(dataset.name) != version.id):
                raise ConflictError("dataset version changed before layout publication")
            layout.status = "active" if beneficial else "rejected"
            layout.artifact_dir = path if beneficial else None
            layout.row_count = manifest["row_count"]
            layout.partition_count = manifest["partition_count"]
            layout.fragment_count = manifest["fragment_count"]
            layout.artifact_bytes = manifest["bytes"]
            layout.build_ms = build_ms
            layout.baseline_ms = round(baseline_ms, 1)
            layout.organized_ms = round(organized_ms, 1)
            layout.error = None if beneficial else "measured probe did not justify the layout"
            session.add(Event(kind="layout.published" if beneficial else "layout.rejected",
                              subject_id=layout.id,
                              data={"partition_key": key, "artifact_bytes": manifest["bytes"]}))
            result = {"layout_id": layout.id, "published": beneficial,
                      "partition_count": manifest["partition_count"],
                      "fragment_count": manifest["fragment_count"],
                      "file_count": manifest["file_count"],
                      "build_ms": build_ms,
                      "baseline_ms": round(baseline_ms, 1),
                      "organized_ms": round(organized_ms, 1)}
            if not complete(session, job, generation, result=result):
                raise ConflictError("layout lease changed before completion")
            return result

    def _materialize(self, job: Job, generation: int) -> dict:
        with self.db.session() as session:
            candidate = session.get(Materialization, job.target_id)
            if candidate is None or candidate.status != "building":
                raise ValueError("materialization candidate is unavailable")
            sql, versions = candidate.sql, dict(candidate.input_versions)
            state = session.get(CatalogState, 1)
            release = session.get(Release, state.active_release_id)
            active = {**release.dataset_versions, **release.feature_snapshots}
            if any(active.get(name) != version for name, version in versions.items()):
                raise ValueError("materialization inputs changed before build")
        started = time.monotonic()
        paths = self._pinned_artifacts(versions)
        first = run_sql(sql, paths, self.store)
        second = run_sql(sql, paths, self.store)
        if not equivalent_results(sql, first, second, self.store):
            raise ValueError("materialization result changed during validation")
        path, digest = self.store.write_table("materializations", first)
        preview = table_preview(first, self.settings.max_query_rows)
        build_ms = round((time.monotonic() - started) * 1000, 1)
        with self.db.session() as session:
            state, release = self._locked_release(session)
            current = session.get(Job, job.id, with_for_update=True)
            candidate = session.get(Materialization, job.target_id, with_for_update=True)
            active = {**release.dataset_versions, **release.feature_snapshots}
            if current.status != "running" or current.lease_generation != generation:
                raise ConflictError("materialization lease changed before publication")
            if candidate.status != "building" or any(
                active.get(name) != version for name, version in versions.items()
            ):
                raise ConflictError("materialization inputs changed before publication")
            candidate.status = "active"
            candidate.artifact_path = path
            candidate.sha256 = digest
            candidate.preview = preview
            candidate.row_count = first.num_rows
            candidate.build_duration_ms = build_ms
            session.add(Event(kind="materialization.published", subject_id=candidate.id,
                              data={"row_count": first.num_rows, "build_duration_ms": build_ms}))
            result = {"materialization_id": candidate.id, "row_count": first.num_rows,
                      "build_duration_ms": build_ms}
            if not complete(session, job, generation, result=result):
                raise ConflictError("materialization lease changed before completion")
            return result

    @staticmethod
    def _record_materialization_use(session: Session, job: Job, result: dict,
                                    duration_ms: float) -> None:
        if result.get("cache_hit"):
            candidate = session.get(Materialization, result["materialization_id"],
                                    with_for_update=True)
            if candidate is not None:
                candidate.reuse_count += 1
                candidate.reuse_total_ms += duration_ms
            return
        key = materialization_signature(job.payload["sql"], job.payload["versions"])
        if key is None:
            return
        session.get(CatalogState, 1, with_for_update=True)
        candidate = session.scalar(select(Materialization).where(
            Materialization.signature == key[0]
        ))
        if candidate is None:
            candidate = Materialization(signature=key[0], sql=job.payload["sql"].strip(),
                                        input_versions=key[1], status="observing",
                                        observed_queries=0, observed_total_ms=0.0,
                                        reuse_count=0)
            session.add(candidate)
            session.flush()
        elif candidate.status == "retired":
            candidate.status = "observing"
            candidate.observed_queries = 0
            candidate.observed_total_ms = 0.0
            candidate.reuse_count = 0
            candidate.reuse_total_ms = 0.0
            candidate.build_duration_ms = None
            candidate.error = None
        candidate.observed_queries += 1
        candidate.observed_total_ms += duration_ms
        if (candidate.status == "observing" and candidate.observed_queries >= 3 and
                candidate.observed_total_ms >= 30):
            candidate.status = "building"
            submit(session, "materialize", "background", target_id=candidate.id,
                   priority=1, cpu_millicores=500, memory_mb=512)
            session.add(Event(kind="materialization.planned", subject_id=candidate.id,
                              data={"observed_queries": candidate.observed_queries}))

    def process(self, job_id: str, generation: int) -> dict:
        with self.db.session() as session:
            job = session.get(Job, job_id)
            if job is None or job.status not in {"running", "cancelling"} or job.lease_generation != generation:
                raise ConflictError("job lease is no longer current")
            already_cancelled = job.status == "cancelling"
        started = now()
        error: str | None = None
        result: dict | None = None
        with RuntimeSampler() as usage:
            try:
                if already_cancelled:
                    pass
                elif job.kind == "sync":
                    result = self._sync(job, generation)
                elif job.kind == "refresh":
                    result = self._refresh(job, generation)
                elif job.kind == "query":
                    result = self._query(job, generation)
                elif job.kind == "materialize":
                    result = self._materialize(job, generation)
                elif job.kind == "organize":
                    result = self._organize(job, generation)
                else:
                    raise ValueError("unknown job kind")
            except Exception as exc:  # noqa: BLE001 - persist worker failures as task results
                error = f"{type(exc).__name__}: {exc}"
        duration_ms = max(0.0, (now() - started).total_seconds() * 1000)
        with self.db.session() as session:
            current = session.get(Job, job.id)
            published = (job.kind in {"sync", "refresh", "materialize", "organize"} and
                         error is None and
                         current.lease_generation == generation and
                         current.status in {"succeeded", "cancelled"})
            accepted = published or complete(session, job, generation, result=result, error=error)
            if accepted:
                session.add(Observation(
                    job_id=job.id, lane=job.lane, duration_ms=duration_ms,
                    cpu_ms=round(usage.cpu_ms, 1),
                    peak_rss_mb=(round(usage.peak_rss_mb, 1)
                                 if usage.peak_rss_mb is not None else None),
                ))
                final_status = session.get(Job, job.id).status
                if job.kind == "query" and final_status == "succeeded" and result is not None:
                    self._record_materialization_use(session, current, result, duration_ms)
                    for layout_id in result.get("layout_ids", []):
                        layout = session.get(DatasetLayout, layout_id, with_for_update=True)
                        if layout is not None:
                            layout.query_count += 1
                if job.kind == "materialize" and final_status in {"failed", "cancelled"}:
                    candidate = session.get(Materialization, job.target_id,
                                            with_for_update=True)
                    if candidate is not None and candidate.status == "building":
                        candidate.status = "rejected"
                        candidate.error = error or "materialization build was cancelled"
                if job.kind == "organize" and final_status in {"failed", "cancelled"}:
                    candidate = session.get(DatasetLayout, job.target_id,
                                            with_for_update=True)
                    if candidate is not None and candidate.status == "building":
                        candidate.status = "rejected"
                        candidate.error = error or "layout build was cancelled"
        if not accepted:
            raise ConflictError("job lease changed while processing")
        return {"status": final_status, "result": result if final_status == "succeeded" else None,
                "error": error if final_status == "failed" else None}

    def status(self) -> dict:
        with self.db.session() as session:
            reserved_cpu, reserved_memory = session.execute(
                select(func.coalesce(func.sum(Job.cpu_millicores), 0),
                       func.coalesce(func.sum(Job.memory_mb), 0))
                .where(Job.status.in_(("running", "cancelling")))
            ).one()
            return {
                "sources": session.scalar(select(func.count(Source.id))) or 0,
                "datasets": session.scalar(select(func.count(Dataset.id))) or 0,
                "features": session.scalar(select(func.count(Feature.id))) or 0,
                "jobs": session.scalar(select(func.count(Job.id))) or 0,
                "running_jobs": session.scalar(select(func.count(Job.id)).where(Job.status == "running")) or 0,
                "resources": {
                    "cpu_budget_millicores": self.settings.resource_cpu_millicores,
                    "cpu_reserved_millicores": reserved_cpu,
                    "memory_budget_mb": self.settings.resource_memory_mb,
                    "memory_reserved_mb": reserved_memory,
                },
                "pools": [
                    {"lane": lane, "capacity": capacity,
                     "max_capacity": (self.settings.query_workers +
                                      self.settings.background_workers if lane == "query"
                                      else capacity),
                     "running": session.scalar(select(func.count(Job.id)).where(
                         Job.lane == lane, Job.status.in_(("running", "cancelling")))) or 0,
                     "pending": session.scalar(select(func.count(Job.id)).where(
                         Job.lane == lane, Job.status == "pending")) or 0}
                    for lane, capacity in (("query", self.settings.query_workers),
                                           ("background", self.settings.background_workers))
                ],
                "workload": [
                    {"lane": lane, "completed": count, "resource_samples": sampled,
                     "avg_duration_ms": round(avg_duration or 0, 1),
                     "avg_cpu_ms": round(avg_cpu, 1) if avg_cpu is not None else None,
                     "avg_peak_rss_mb": round(avg_rss, 1) if avg_rss is not None else None}
                    for lane, count, sampled, avg_duration, avg_cpu, avg_rss in session.execute(
                        select(Observation.lane, func.count(Observation.id),
                               func.count(Observation.cpu_ms),
                               func.avg(Observation.duration_ms),
                               func.avg(Observation.cpu_ms),
                               func.avg(Observation.peak_rss_mb))
                        .where(Observation.created_at >= now() - timedelta(hours=1))
                        .group_by(Observation.lane)
                    )
                ],
            }

