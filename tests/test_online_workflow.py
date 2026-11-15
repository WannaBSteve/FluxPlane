"""End-to-end behavior across source synchronization, features, and pinned queries."""

import json
import os
import sqlite3
import sys
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import fitz
import fluxplane.application.service as service_module
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient
from fluxplane.api.app import create_app
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.connectors.snapshot import SourceSnapshot
from fluxplane.execution.query import equivalent_results
from fluxplane.scheduling.jobs import claim, claim_for_worker, complete, submit
from fluxplane.scheduling.policy import order_ready
from fluxplane.storage.models import (
    CatalogState,
    Dataset,
    DatasetDelta,
    DatasetLayout,
    DatasetProfile,
    DatasetVersion,
    Feature,
    FeatureSnapshot,
    Job,
    JobAttempt,
    JobDependency,
    Materialization,
    Release,
    Workflow,
    now,
)
from sqlalchemy import select


@pytest.fixture
def platform(tmp_path: Path) -> Platform:
    source_root = tmp_path / "sources"
    source_root.mkdir()
    (source_root / "orders.csv").write_text(
        "customer_id,product_id,amount\n1,10,5\n1,11,8\n2,10,3\n", encoding="utf-8"
    )
    con = sqlite3.connect(source_root / "products.sqlite3")
    con.execute("CREATE TABLE products(product_id INTEGER, category TEXT)")
    con.executemany("INSERT INTO products VALUES (?, ?)", [(10, "A"), (11, "B")])
    con.commit()
    con.close()
    config = Settings(
        database_url=f"sqlite:///{tmp_path / 'metadata.sqlite3'}",
        artifact_root=tmp_path / "artifacts",
        source_root=source_root,
    )
    result = Platform(config)
    result.initialize()
    return result


def work(platform: Platform, lane: str) -> dict:
    with platform.db.session() as session:
        item = claim(session, lane, "test-worker", 120)
        assert item is not None
        job_id, generation = item.id, item.lease_generation
    return platform.process(job_id, generation)


def prepare_sources(platform: Platform) -> tuple[Dataset, Dataset]:
    orders_source = platform.add_source("order-file", "file", {})
    products_source = platform.add_source("product-database", "sqlite", {"path": "products.sqlite3"})
    orders = platform.add_dataset(orders_source.id, "orders", "orders.csv")
    products = platform.add_dataset(products_source.id, "products", "products")
    for item in [orders, products]:
        platform.queue_sync(item.id)
        assert work(platform, "background")["status"] == "succeeded"
    return orders, products


def test_cross_source_query_feature_update_and_pinned_read(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    feature = platform.add_feature(
        "spend_by_category",
        "SELECT p.category, SUM(o.amount) AS total FROM orders o "
        "JOIN products p ON o.product_id=p.product_id GROUP BY p.category",
        ["orders", "products"], ["category"],
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["status"] == "succeeded"
    old_query = platform.queue_query("SELECT * FROM spend_by_category ORDER BY category")

    source_path = platform.settings.source_root / "orders.csv"
    source_path.write_text(source_path.read_text() + "2,10,7\n", encoding="utf-8")
    platform.queue_sync(orders.id)
    assert work(platform, "background")["status"] == "succeeded"

    with pytest.raises(ValueError, match="stale"):
        platform.queue_query("SELECT * FROM spend_by_category")
    with TestClient(create_app(platform.settings)) as client:
        record = next(item for item in client.get("/api/v1/features").json()
                      if item["name"] == "spend_by_category")
        assert record["stale"] is True
        assert record["active_snapshot_id"] is None

    with platform.db.session() as session:
        scheduled = session.scalars(
            select(Job).where(Job.kind == "refresh", Job.status == "pending")
        ).all()
        assert len(scheduled) == 1
    assert work(platform, "background")["status"] == "succeeded"
    current_query = platform.queue_query("SELECT * FROM spend_by_category ORDER BY category")

    with platform.db.session() as session:
        old_versions = session.get(Job, old_query.id).payload["versions"]
        new_versions = session.get(Job, current_query.id).payload["versions"]
        assert old_versions["orders"] != new_versions["orders"]
        assert old_versions["spend_by_category"] != new_versions["spend_by_category"]

    old_result = work(platform, "query")
    new_result = work(platform, "query")
    assert old_result["result"]["rows"] == [
        {"category": "A", "total": "8"}, {"category": "B", "total": "8"}
    ]
    assert new_result["result"]["rows"] == [
        {"category": "A", "total": "15"}, {"category": "B", "total": "8"}
    ]


def test_workflow_plans_dependencies_and_binds_query_after_refresh(platform: Platform) -> None:
    orders, products = prepare_sources(platform)
    feature = platform.add_feature(
        "category_spend",
        "SELECT p.category, SUM(o.amount) AS total FROM orders o "
        "JOIN products p ON o.product_id=p.product_id GROUP BY p.category",
        ["orders", "products"], ["category"],
    )
    (platform.settings.source_root / "orders.csv").write_text(
        "customer_id,product_id,amount\n1,10,5\n1,11,8\n2,10,3\n2,10,7\n",
        encoding="utf-8",
    )
    steps = [
        {"key": "orders_sync", "kind": "sync", "target_id": orders.id},
        {"key": "products_sync", "kind": "sync", "target_id": products.id},
        {"key": "spend_refresh", "kind": "refresh", "target_id": feature.id},
        {"key": "report", "kind": "query",
         "sql": "SELECT category, total FROM category_spend ORDER BY category"},
    ]
    workflow = platform.create_workflow("Daily category report", steps, "daily-report-1")
    assert platform.create_workflow("Daily category report", steps, "daily-report-1").id == workflow.id
    with platform.db.session() as session:
        jobs = {job.node_key: job for job in session.scalars(
            select(Job).where(Job.workflow_id == workflow.id)
        ).all()}
        assert len(jobs) == 4
        assert "versions" not in jobs["report"].payload
        edges = {(session.get(Job, edge.depends_on_id).node_key,
                  session.get(Job, edge.job_id).node_key)
                 for edge in session.scalars(select(JobDependency)).all()}
        assert {("orders_sync", "spend_refresh"),
                ("products_sync", "spend_refresh"),
                ("spend_refresh", "report")}.issubset(edges)
        assert claim(session, "query", "early-query", 120) is None
    completed: list[str] = []
    for _ in range(8):
        with platform.db.session() as session:
            job = claim(session, "background", "workflow-worker", 120)
            work_item = (job.id, job.lease_generation, job.node_key) if job else None
        if work_item is None:
            break
        job_id, generation, key = work_item
        assert platform.process(job_id, generation)["status"] == "succeeded"
        if key:
            completed.append(key)
    assert {"orders_sync", "products_sync", "spend_refresh"} <= set(completed)
    assert completed.index("spend_refresh") > completed.index("orders_sync")
    assert completed.index("spend_refresh") > completed.index("products_sync")
    with platform.db.session() as session:
        query = claim(session, "query", "workflow-query", 120)
        assert query.id == jobs["report"].id
        query_id, generation = query.id, query.lease_generation
    result = platform.process(query_id, generation)
    assert result["result"]["rows"] == [
        {"category": "A", "total": "15"}, {"category": "B", "total": "8"},
    ]
    with platform.db.session() as session:
        assert "versions" in session.get(Job, query_id).payload
        assert session.scalar(select(Workflow).where(Workflow.id == workflow.id)) is not None
    with TestClient(create_app(platform.settings)) as client:
        record = client.get(f"/api/v1/workflows/{workflow.id}").json()
        assert record["status"] == "succeeded"
        assert len(record["edges"]) == 5
        created = client.post("/api/v1/workflows", json={
            "name": "Follow-up report",
            "steps": [{"key": "report", "kind": "query",
                       "sql": "SELECT COUNT(*) AS n FROM orders"}],
        }, headers={"Idempotency-Key": "follow-up-report"})
        assert created.status_code == 201
        repeated = client.post("/api/v1/workflows", json={
            "name": "Follow-up report",
            "steps": [{"key": "report", "kind": "query",
                       "sql": "SELECT COUNT(*) AS n FROM orders"}],
        }, headers={"Idempotency-Key": "follow-up-report"})
        assert repeated.json()["id"] == created.json()["id"]


def test_workflow_failure_propagates_and_cycles_are_rejected(platform: Platform) -> None:
    source = platform.add_source("missing-file", "file", {})
    dataset = platform.add_dataset(source.id, "missing_orders", "missing.csv")
    steps = [
        {"key": "sync", "kind": "sync", "target_id": dataset.id},
        {"key": "report", "kind": "query", "sql": "SELECT * FROM missing_orders"},
    ]
    with pytest.raises(ValueError, match="cycle"):
        platform.create_workflow("Cyclic", [
            {**steps[0], "depends_on": ["report"]}, steps[1],
        ])
    workflow = platform.create_workflow("Missing input", steps)
    assert work(platform, "background")["status"] == "failed"
    with platform.db.session() as session:
        assert claim(session, "query", "query-worker", 120) is None
        report = session.scalar(select(Job).where(
            Job.workflow_id == workflow.id, Job.node_key == "report"
        ))
        assert report.status == "failed"
        assert "dependency_failed" in report.error


def test_workflow_adds_refresh_for_changed_feature_inputs(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    feature = platform.add_feature(
        "order_totals", "SELECT customer_id, SUM(amount) AS total "
        "FROM orders GROUP BY customer_id", ["orders"], ["customer_id"],
    )
    workflow = platform.create_workflow("Refresh and read", [
        {"key": "sync", "kind": "sync", "target_id": orders.id},
        {"key": "report", "kind": "query", "sql": "SELECT * FROM order_totals"},
    ])
    with platform.db.session() as session:
        jobs = session.scalars(select(Job).where(Job.workflow_id == workflow.id)).all()
        planned = next(job for job in jobs if job.kind == "refresh")
        assert planned.target_id == feature.id
        assert len(jobs) == 3
    for _ in range(4):
        with platform.db.session() as session:
            item = claim(session, "background", "worker", 120)
            work_item = (item.id, item.lease_generation) if item else None
        if work_item is None:
            break
        assert platform.process(*work_item)["status"] == "succeeded"
    with platform.db.session() as session:
        item = claim(session, "query", "reader", 120)
        assert item is not None
        work_item = item.id, item.lease_generation
    assert platform.process(*work_item)["result"]["row_count"] == 2


def test_workflow_rejects_duplicate_updates_and_unavailable_inputs(platform: Platform) -> None:
    source = platform.add_source("pending-source", "file", {})
    dataset = platform.add_dataset(source.id, "pending_orders", "orders.csv")
    feature = platform.add_feature(
        "pending_totals", "SELECT customer_id, SUM(amount) AS total "
        "FROM pending_orders GROUP BY customer_id", ["pending_orders"], ["customer_id"],
    )
    with pytest.raises(ValueError, match="same dataset"):
        platform.create_workflow("Duplicate sync", [
            {"key": "first", "kind": "sync", "target_id": dataset.id},
            {"key": "second", "kind": "sync", "target_id": dataset.id},
        ])
    with pytest.raises(ValueError, match="not published or synchronized"):
        platform.create_workflow("Missing input", [
            {"key": "refresh", "kind": "refresh", "target_id": feature.id},
        ])
    with pytest.raises(ValueError, match="not published or synchronized"):
        platform.create_workflow("Missing query input", [
            {"key": "report", "kind": "query", "sql": "SELECT * FROM pending_orders"},
        ])


def test_ready_task_is_not_hidden_behind_many_waiting_dependencies(platform: Platform) -> None:
    with platform.db.session() as session:
        parent = submit(session, "sync", "background")
        parent.status = "running"
        for _ in range(105):
            child = submit(session, "refresh", "background")
            session.add(JobDependency(job_id=child.id, depends_on_id=parent.id))
        ready = submit(session, "sync", "background")
    with platform.db.session() as session:
        selected = claim(session, "background", "worker", 120, max_running=2)
        assert selected.id == ready.id


def test_time_dependent_feature_refreshes_without_source_change(platform: Platform) -> None:
    prepare_sources(platform)
    feature = platform.add_feature(
        "recent_orders",
        "SELECT customer_id, MAX(epoch_ms(CURRENT_TIMESTAMP)) AS computed_at "
        "FROM orders GROUP BY customer_id",
        ["orders"], ["customer_id"], refresh_interval_seconds=2,
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        current = session.get(Feature, feature.id)
        old_id = current.active_snapshot_id
        snapshot = session.get(FeatureSnapshot, old_id)
        snapshot.created_at = now() - timedelta(seconds=3)
    with pytest.raises(ValueError, match="expired"):
        platform.queue_query("SELECT * FROM recent_orders")
    with TestClient(create_app(platform.settings)) as client:
        listed = client.get("/api/v1/features").json()
        assert next(item for item in listed if item["name"] == "recent_orders")["stale"] is True
    scheduled = platform.poll_time_features()
    assert len(scheduled) == 1
    assert platform.poll_time_features() == []
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        assert session.get(Feature, feature.id).active_snapshot_id != old_id
    assert platform.queue_query("SELECT * FROM recent_orders").status == "pending"


def test_rolling_feature_refreshes_at_event_entry_and_exit(
    platform: Platform, monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"time": anchor}
    monkeypatch.setattr(service_module, "now", lambda: clock["time"])
    (platform.settings.source_root / "events.csv").write_text(
        "customer_id,amount,occurred_at\n"
        f"1,5,{(anchor - timedelta(seconds=5)).isoformat()}\n"
        f"1,7,{(anchor + timedelta(seconds=4)).isoformat()}\n",
        encoding="utf-8",
    )
    source = platform.add_source("event-file", "file", {})
    dataset = platform.add_dataset(source.id, "events", "events.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    feature = platform.add_feature("recent_amount", None, ["events"], ["customer_id"], {
        "mode": "rolling_sum", "measure": "amount", "event_time": "occurred_at",
        "window_seconds": 10, "sum_alias": "total", "count_alias": "event_count",
    })
    platform.queue_refresh(feature.id)
    first = work(platform, "background")
    assert first["status"] == "succeeded"
    assert first["result"]["next_refresh_at"] == (anchor + timedelta(seconds=4)).isoformat()
    with TestClient(create_app(platform.settings)) as client:
        record = next(item for item in client.get("/api/v1/features").json()
                      if item["name"] == "recent_amount")
        assert record["maintenance_mode"] == "rolling_sum"
        assert record["maintenance"]["window_seconds"] == 10
        assert datetime.fromisoformat(record["next_refresh_at"]) == anchor + timedelta(seconds=4)
    assert work_query_rows(platform, "SELECT total, event_count FROM recent_amount") == [
        {"total": "5", "event_count": 1},
    ]
    clock["time"] = anchor + timedelta(seconds=3)
    assert platform.poll_time_features() == []
    clock["time"] = anchor + timedelta(seconds=4)
    with pytest.raises(ValueError, match="expired"):
        platform.queue_query("SELECT * FROM recent_amount")
    assert len(platform.poll_time_features()) == 1
    assert platform.poll_time_features() == []
    entered = work(platform, "background")
    assert entered["status"] == "succeeded"
    assert entered["result"]["next_refresh_at"] == (anchor + timedelta(seconds=5)).isoformat()
    assert work_query_rows(platform, "SELECT total, event_count FROM recent_amount") == [
        {"total": "12", "event_count": 2},
    ]
    clock["time"] = anchor + timedelta(seconds=6)
    assert len(platform.poll_time_features()) == 1
    assert work(platform, "background")["status"] == "succeeded"
    assert work_query_rows(platform, "SELECT total, event_count FROM recent_amount") == [
        {"total": "7", "event_count": 1},
    ]


def test_rolling_feature_checks_published_columns_and_window(platform: Platform) -> None:
    prepare_sources(platform)
    rule = {"mode": "rolling_sum", "measure": "amount", "event_time": "occurred_at",
            "window_seconds": 10, "sum_alias": "total", "count_alias": "row_count"}
    with pytest.raises(ValueError, match="columns are missing: occurred_at"):
        platform.add_feature("missing_event_time", None, ["orders"], ["customer_id"], rule)
    with pytest.raises(ValueError, match="positive window"):
        platform.add_feature("bad_window", None, ["orders"], ["customer_id"],
                             {**rule, "window_seconds": 0})


def test_cross_source_window_sql_uses_one_event_time_anchor(
    platform: Platform, monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    clock = {"time": anchor}
    monkeypatch.setattr(service_module, "now", lambda: clock["time"])
    root = platform.settings.source_root
    (root / "events_a.csv").write_text(
        "customer_id,amount,occurred_at\n"
        f"1,5,{(anchor - timedelta(seconds=5)).isoformat()}\n"
        f"1,7,{(anchor + timedelta(seconds=4)).isoformat()}\n", encoding="utf-8",
    )
    (root / "events_b.csv").write_text(
        "customer_id,amount,occurred_at\n"
        f"1,3,{(anchor - timedelta(seconds=2)).isoformat()}\n", encoding="utf-8",
    )
    (root / "customers.csv").write_text("customer_id\n1\n", encoding="utf-8")
    source = platform.add_source("window-files", "file", {})
    for name in ("events_a", "events_b", "customers"):
        dataset = platform.add_dataset(source.id, name, f"{name}.csv")
        platform.queue_sync(dataset.id)
        assert work(platform, "background")["status"] == "succeeded"
    sql = (
        "SELECT c.customer_id, COALESCE(a.amount, 0) + COALESCE(b.amount, 0) AS total "
        "FROM customers c "
        "LEFT JOIN (SELECT customer_id, SUM(amount) AS amount FROM events_a "
        "GROUP BY customer_id) a USING (customer_id) "
        "LEFT JOIN (SELECT customer_id, SUM(amount) AS amount FROM events_b "
        "GROUP BY customer_id) b USING (customer_id)"
    )
    rule = {"mode": "window_sql", "windows": {
        "events_a": {"event_time": "occurred_at", "window_seconds": 10},
        "events_b": {"event_time": "occurred_at", "window_seconds": 10},
    }}
    feature = platform.add_feature("recent_total", sql,
                                   ["events_a", "events_b", "customers"],
                                   ["customer_id"], rule)
    platform.queue_refresh(feature.id)
    first = work(platform, "background")
    assert first["result"]["next_refresh_at"] == (anchor + timedelta(seconds=4)).isoformat()
    assert work_query_rows(platform, "SELECT total FROM recent_total") == [{"total": "8"}]
    with TestClient(create_app(platform.settings)) as client:
        entry = client.get("/api/v1/features").json()[0]
        assert entry["maintenance_mode"] == "window_sql"
        assert set(entry["maintenance"]["windows"]) == {"events_a", "events_b"}

    clock["time"] = anchor + timedelta(seconds=4)
    with pytest.raises(ValueError, match="expired"):
        platform.queue_query("SELECT * FROM recent_total")
    assert len(platform.poll_time_features()) == 1
    entered = work(platform, "background")
    assert entered["result"]["next_refresh_at"] == (anchor + timedelta(seconds=5)).isoformat()
    assert work_query_rows(platform, "SELECT total FROM recent_total") == [{"total": "15"}]

    clock["time"] = anchor + timedelta(seconds=6)
    assert len(platform.poll_time_features()) == 1
    assert work(platform, "background")["status"] == "succeeded"
    assert work_query_rows(platform, "SELECT total FROM recent_total") == [{"total": "10"}]

    clock["time"] = anchor + timedelta(seconds=9)
    assert len(platform.poll_time_features()) == 1
    assert work(platform, "background")["status"] == "succeeded"
    assert work_query_rows(platform, "SELECT total FROM recent_total") == [{"total": "7"}]
    with pytest.raises(ValueError, match="missing from events_a"):
        platform.add_feature("invalid_window", sql,
                             ["events_a", "events_b", "customers"], ["customer_id"],
                             {"mode": "window_sql", "windows": {
                                 "events_a": {"event_time": "unknown", "window_seconds": 10},
                             }})
    with pytest.raises(ValueError, match="declared input windows"):
        platform.add_feature("implicit_clock", "SELECT customer_id, CURRENT_TIMESTAMP AS clock "
                             "FROM customers", ["events_a", "customers"], ["customer_id"],
                             {"mode": "window_sql", "windows": {
                                 "events_a": {"event_time": "occurred_at", "window_seconds": 10},
                             }})


def test_repeated_query_materializes_and_invalidates_on_source_change(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id ORDER BY customer_id"
    candidate = None
    for _ in range(10):
        platform.queue_query(sql)
        result = work(platform, "query")
        assert result["status"] == "succeeded"
        assert result["result"]["cache_hit"] is False
        with platform.db.session() as session:
            candidate = session.scalar(select(Materialization))
            if candidate is not None and candidate.status == "building":
                break
    assert candidate is not None and candidate.status == "building"
    built = work(platform, "background")
    assert built["status"] == "succeeded"
    with platform.db.session() as session:
        cached = session.get(Materialization, candidate.id)
        assert cached.status == "active"
        assert cached.build_duration_ms > 0
        assert cached.row_count == 2
        assert platform.store.available(cached.artifact_path)
    hit_job = platform.queue_query(sql)
    hit = work(platform, "query")
    assert hit["result"]["cache_hit"] is True
    assert hit["result"]["rows"] == result["result"]["rows"]
    with platform.db.session() as session:
        assert session.get(Materialization, candidate.id).reuse_count == 1
        for index in range(35):
            session.add(Materialization(
                signature=f"one-off-{index}", sql=f"SELECT {index}",
                input_versions={}, status="observing",
                observed_queries=1, observed_total_ms=1,
            ))
    with TestClient(create_app(platform.settings)) as client:
        listed = client.get("/api/v1/materializations").json()
        assert listed[0]["id"] == candidate.id
        assert listed[0]["current"] is True
        assert listed[0]["reuse_count"] == 1
        assert listed[0]["reuse_total_ms"] >= 0
        assert listed[0]["estimated_net_ms"] is not None
        assert listed[0]["artifact_bytes"] > 0
    platform.storage_summary(collect=True, min_age_seconds=0)
    with platform.db.session() as session:
        assert platform.store.available(session.get(Materialization, candidate.id).artifact_path)
        old_release_id = session.get(CatalogState, 1).active_release_id

    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text(encoding="utf-8") + "2,10,7\n", encoding="utf-8")
    platform.queue_sync(orders.id)
    assert work(platform, "background")["status"] == "succeeded"
    platform.queue_query(sql)
    changed = work(platform, "query")
    assert changed["result"]["cache_hit"] is False
    assert changed["result"]["rows"] != hit["result"]["rows"]
    retired = platform.maintain_materializations(max_bytes=10**9, stale_seconds=0)
    assert retired["retired"] == 1
    assert retired["reclaimed_bytes"] > 0
    with platform.db.session() as session:
        old = session.get(Materialization, candidate.id)
        assert old.status == "retired"
        assert old.artifact_path is None
        assert "artifact_path" not in session.get(Job, hit_job.id).result
    platform.restore_release(old_release_id)
    platform.queue_query(sql)
    restored = work(platform, "query")
    assert restored["result"]["cache_hit"] is False
    with platform.db.session() as session:
        assert session.get(Materialization, candidate.id).status == "observing"


def test_materialization_eviction_waits_for_running_query(platform: Platform) -> None:
    prepare_sources(platform)
    sql = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    for _ in range(10):
        platform.queue_query(sql)
        work(platform, "query")
        with platform.db.session() as session:
            candidate = session.scalar(select(Materialization))
            if candidate is not None and candidate.status == "building":
                break
    assert work(platform, "background")["status"] == "succeeded"
    queued = platform.queue_query(sql)
    with platform.db.session() as session:
        running = claim(session, "query", "busy-worker", 120)
        assert running.id == queued.id
        generation = running.lease_generation
    assert platform.maintain_materializations(max_bytes=0)["deferred_running_jobs"] == 1
    assert platform.process(queued.id, generation)["result"]["cache_hit"] is True
    assert platform.maintain_materializations(max_bytes=0)["retired"] == 1
    with platform.db.session() as session:
        assert session.get(Materialization, candidate.id).status == "retired"
        assert "artifact_path" not in session.get(Job, queued.id).result


def test_materialization_validation_respects_sql_result_order(platform: Platform) -> None:
    first = pa.table({"key": [1, 2, 1], "amount": [3, 4, 3]})
    reordered = pa.table({"key": [1, 1, 2], "amount": [3, 3, 4]})
    changed = pa.table({"key": [1, 1, 2], "amount": [3, 4, 4]})
    sql = "SELECT key, amount FROM orders"
    assert equivalent_results(sql, first, reordered, platform.store)
    assert not equivalent_results(sql, first, changed, platform.store)
    assert not equivalent_results(sql + " ORDER BY key", first, reordered, platform.store)


def test_partition_layout_is_published_and_used_only_for_matching_version(
    platform: Platform, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(service_module, "_measure_layout_probe",
                        lambda *_args: (10.0, 5.0))
    root = platform.settings.source_root
    path = root / "regional.parquet"
    rows = 120000
    pq.write_table(pa.table({
        "row_id": list(range(rows)),
        "region": ["east", "west", "north"] * (rows // 3),
        "amount": [1] * rows,
        "payload": ["v" * 80] * rows,
    }), path)
    source = platform.add_source("regional-source", "file", {})
    dataset = platform.add_dataset(source.id, "regional", "regional.parquet")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with TestClient(create_app(platform.settings)) as client:
        submitted = client.post(f"/api/v1/datasets/{dataset.id}/layouts",
                                json={"partition_key": "region"})
        assert submitted.status_code == 202
    built = work(platform, "background")
    assert built["status"] == "succeeded", built["error"]
    assert built["result"]["published"] is True
    assert built["result"]["fragment_count"] > built["result"]["file_count"] == 3
    with platform.db.session() as session:
        layout = session.scalar(select(DatasetLayout))
        assert layout.status == "active"
        assert platform.store.available_layout(layout.artifact_dir)
        layout_id = layout.id
    with TestClient(create_app(platform.settings)) as client:
        listed = client.get("/api/v1/layouts").json()
        assert listed[0]["id"] == layout_id
        assert listed[0]["current"] is True
    sql = "SELECT SUM(amount) AS total FROM regional WHERE region = 'east'"
    platform.queue_query(sql)
    used = work(platform, "query")
    assert used["result"]["rows"] == [{"total": "40000"}]
    assert used["result"]["layout_ids"] == [layout_id]
    with platform.db.session() as session:
        assert session.get(DatasetLayout, layout_id).query_count == 1

    pq.write_table(pa.table({"row_id": [1], "region": ["east"],
                             "amount": [7], "payload": ["new"]}), path)
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    platform.queue_query(sql)
    updated = work(platform, "query")
    assert updated["result"]["rows"] == [{"total": "7"}]
    assert updated["result"]["layout_ids"] == []


def test_layout_build_rejects_version_change_and_can_retry_on_new_version(
    platform: Platform,
) -> None:
    path = platform.settings.source_root / "regional.csv"
    path.write_text("region,amount\neast,1\nwest,2\n", encoding="utf-8")
    source = platform.add_source("regions", "file", {})
    dataset = platform.add_dataset(source.id, "regions", "regional.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    candidate_job = platform.queue_layout(dataset.id, "region")
    path.write_text("region,amount\neast,3\nwest,4\n", encoding="utf-8")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    rejected = work(platform, "background")
    assert rejected["status"] == "failed"
    assert "version changed" in rejected["error"]
    with platform.db.session() as session:
        old = session.get(DatasetLayout, candidate_job.target_id)
        assert old.status == "rejected"
        assert old.artifact_dir is None
    new_job = platform.queue_layout(dataset.id, "region")
    assert new_job.target_id != candidate_job.target_id


def test_repeated_filtered_queries_schedule_one_layout_candidate(platform: Platform) -> None:
    path = platform.settings.source_root / "regional.parquet"
    pq.write_table(pa.table({
        "region": ["east", "west"] * 6000,
        "amount": [1] * 12000,
    }), path)
    source = platform.add_source("regional-source", "file", {})
    dataset = platform.add_dataset(source.id, "regional", "regional.parquet")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    for _ in range(20):
        platform.queue_query("SELECT COUNT(*) FROM regional WHERE region = 'east'")
        assert work(platform, "query")["status"] == "succeeded"
    scheduled = platform.poll_layout_candidates()
    assert len(scheduled) == 1
    assert platform.poll_layout_candidates() == []
    with platform.db.session() as session:
        candidate = session.scalar(select(DatasetLayout))
        assert candidate.version_id == session.get(Dataset, dataset.id).active_version_id
        assert candidate.partition_key == "region"
        candidate.status = "rejected"
    assert platform.poll_layout_candidates() == []


def test_volatile_query_does_not_materialize(platform: Platform) -> None:
    prepare_sources(platform)
    for _ in range(4):
        platform.queue_query("SELECT CURRENT_TIMESTAMP AS observed_at FROM orders LIMIT 1")
        assert work(platform, "query")["status"] == "succeeded"
    with platform.db.session() as session:
        assert session.scalar(select(Materialization)) is None


def test_materialization_rejects_changed_inputs_before_build(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    sql = "SELECT COUNT(*) AS n FROM orders"
    for _ in range(10):
        platform.queue_query(sql)
        assert work(platform, "query")["status"] == "succeeded"
        with platform.db.session() as session:
            candidate = session.scalar(select(Materialization))
            if candidate is not None and candidate.status == "building":
                break
    else:
        pytest.fail("repeated query did not schedule materialization")
    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text(encoding="utf-8") + "2,10,7\n", encoding="utf-8")
    platform.queue_sync(orders.id)
    assert work(platform, "background")["status"] == "succeeded"
    stale = work(platform, "background")
    assert stale["status"] == "failed"
    assert "inputs changed" in stale["error"]
    with platform.db.session() as session:
        record = session.get(Materialization, candidate.id)
        assert record.status == "rejected"
        assert record.artifact_path is None


def test_cancelled_materialization_does_not_remain_building(platform: Platform) -> None:
    from fluxplane.scheduling.jobs import cancel

    prepare_sources(platform)
    for _ in range(10):
        platform.queue_query("SELECT COUNT(*) AS n FROM orders")
        assert work(platform, "query")["status"] == "succeeded"
        with platform.db.session() as session:
            candidate = session.scalar(select(Materialization))
            if candidate is not None and candidate.status == "building":
                break
    else:
        pytest.fail("repeated query did not schedule materialization")
    with platform.db.session() as session:
        job = session.scalar(select(Job).where(Job.kind == "materialize"))
        claimed = claim(session, "background", "test-worker", 120)
        assert claimed.id == job.id
        cancel(session, job.id)
        generation = claimed.lease_generation
    assert platform.process(job.id, generation)["status"] == "cancelled"
    with platform.db.session() as session:
        record = session.get(Materialization, candidate.id)
        assert record.status == "rejected"
        assert record.artifact_path is None


def test_materialization_lease_exhaustion_and_pending_cancel_settle_candidate(
    platform: Platform,
) -> None:
    from fluxplane.scheduling.jobs import cancel, retry

    with platform.db.session() as session:
        candidate = Materialization(signature="f" * 64, sql="SELECT 1 FROM orders",
                                    input_versions={"orders": "old-version"}, status="building",
                                    observed_queries=3, observed_total_ms=100, reuse_count=0)
        session.add(candidate)
        session.flush()
        job = submit(session, "materialize", "background", target_id=candidate.id)
        candidate_id, job_id = candidate.id, job.id
    for _ in range(3):
        with platform.db.session() as session:
            claimed = claim(session, "background", "lost-worker", 10)
            assert claimed.id == job_id
            claimed.lease_until = now() - timedelta(seconds=1)
    with platform.db.session() as session:
        assert claim(session, "background", "replacement", 10) is None
        assert session.get(Job, job_id).status == "failed"
        assert session.get(Materialization, candidate_id).status == "rejected"
        retry(session, job_id)
        assert session.get(Materialization, candidate_id).status == "building"
        cancel(session, job_id)
        assert session.get(Materialization, candidate_id).status == "rejected"


def work_query_rows(platform: Platform, sql: str) -> list[dict]:
    platform.queue_query(sql)
    return work(platform, "query")["result"]["rows"]


def test_idempotent_submission_and_failed_external_query(platform: Platform) -> None:
    prepare_sources(platform)
    first = platform.queue_query("SELECT COUNT(*) AS n FROM orders", "same-request")
    repeated = platform.queue_query("SELECT COUNT(*) AS n FROM orders", "same-request")
    assert first.id == repeated.id
    assert work(platform, "query")["result"]["rows"] == [{"n": 3}]
    with pytest.raises(ValueError, match="external access"):
        platform.queue_query("SELECT read_text('/etc/passwd')")
    with pytest.raises(ValueError, match="only one SELECT"):
        platform.queue_query("DELETE FROM orders")


def test_http_api_matches_catalog_and_job_state(platform: Platform) -> None:
    app = create_app(platform.settings)
    with TestClient(app) as client:
        assert client.get("/health/ready").json()["status"] == "ready"
        before_event = client.get("/api/v1/events/cursor").json()["cursor"]
        source = client.post("/api/v1/sources", json={"name": "files", "kind": "file", "config": {}})
        assert source.status_code == 201
        dataset = client.post("/api/v1/datasets", json={
            "name": "orders", "source_id": source.json()["id"], "source_object": "orders.csv"
        })
        assert dataset.status_code == 201
        submitted = client.post(f"/api/v1/datasets/{dataset.json()['id']}/sync")
        assert submitted.status_code == 202
        assert client.get("/api/v1/events/cursor").json()["cursor"] > before_event
        assert work(platform, "background")["status"] == "succeeded"
        assert client.get(f"/api/v1/runs/{submitted.json()['id']}").json()["status"] == "succeeded"
        assert client.get("/api/v1/system/metrics").json()["datasets"] == 1
        refused = client.post("/api/v1/queries", json={
            "sql": "SELECT COUNT(*) AS n FROM orders", "cpu_millicores": 3000,
        })
        assert refused.status_code == 400
        admitted = client.post("/api/v1/queries", json={
            "sql": "SELECT COUNT(*) AS n FROM orders", "cpu_millicores": 750,
            "memory_mb": 768,
        })
        assert admitted.status_code == 202
        assert (admitted.json()["cpu_millicores"], admitted.json()["memory_mb"]) == (750, 768)
        assert client.get("/api/v1/system/metrics").json()["resources"] == {
            "cpu_budget_millicores": 2000, "cpu_reserved_millicores": 0,
            "memory_budget_mb": 2048, "memory_reserved_mb": 0,
        }
        profile = client.get(f"/api/v1/datasets/{dataset.json()['id']}/profile").json()
        assert profile["row_count"] == 3
        amount = next(field for field in profile["fields"] if field["name"] == "amount")
        assert amount["null_count"] == 0
        assert amount["distinct_in_sample"] == 3
        assert client.get("/api/v1/system/metrics").json()["workload"][0]["completed"] == 1
        assert work(platform, "query")["status"] == "succeeded"
        observed = client.get(f"/api/v1/runs/{admitted.json()['id']}").json()["runtime"]
        assert observed["duration_ms"] >= 0
        assert observed["cpu_ms"] >= 0
        if sys.platform == "linux":
            assert observed["peak_rss_mb"] > 0
        workload = client.get("/api/v1/system/metrics").json()["workload"]
        assert any(item["lane"] == "query" and item["avg_cpu_ms"] is not None
                   for item in workload)
        feature = client.post("/api/v1/features", json={
            "name": "customer_totals", "dependencies": ["orders"],
            "entity_keys": ["customer_id"],
            "maintenance": {"mode": "append_sum_count", "measure": "amount",
                            "sum_alias": "total_amount", "count_alias": "order_count"},
        })
        assert feature.status_code == 201
        assert "SUM" in feature.json()["sql"]
        client.post(f"/api/v1/features/{feature.json()['id']}/refresh")
        assert work(platform, "background")["status"] == "succeeded"
        assert client.get("/api/v1/features").json()[0]["maintenance_mode"] == "append_sum_count"
        for path in (
            "/auth/me", "/sources", "/datasets", "/features", "/releases",
            "/workflows", "/materializations", "/layouts", "/runs",
            "/storage/summary", f"/datasets/{dataset.json()['id']}/versions",
            f"/features/{feature.json()['id']}/snapshots",
        ):
            assert client.get("/api/v1" + path).status_code == 200


def test_query_result_pages_reach_rows_beyond_the_inline_preview(platform: Platform) -> None:
    prepare_sources(platform)
    platform.settings.max_query_rows = 2
    with TestClient(create_app(platform.settings)) as client:
        submitted = client.post("/api/v1/queries", json={
            "sql": "SELECT customer_id, amount FROM orders ORDER BY amount"
        })
        job_id = submitted.json()["id"]
        assert client.get(f"/api/v1/runs/{job_id}/rows").status_code == 409
        assert work(platform, "query")["status"] == "succeeded"
        inline = client.get(f"/api/v1/runs/{job_id}").json()["result"]
        assert len(inline["rows"]) == 2
        assert "artifact_path" not in inline and "sha256" not in inline
        first = client.get(f"/api/v1/runs/{job_id}/rows?offset=0&limit=2")
        assert first.status_code == 200
        assert first.json()["row_count"] == 3
        assert first.json()["has_more"] is True
        second = client.get(f"/api/v1/runs/{job_id}/rows?offset=2&limit=2")
        assert second.status_code == 200
        assert second.json()["rows"] == [{"customer_id": 1, "amount": 8}]
        assert second.json()["has_more"] is False
        assert client.get(f"/api/v1/runs/{job_id}/rows?limit=501").status_code == 422


def test_cancel_and_retry_keep_publication_fenced(platform: Platform) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    app = create_app(platform.settings)
    with TestClient(app) as client:
        pending = platform.queue_sync(dataset.id)
        response = client.post(f"/api/v1/runs/{pending.id}/cancel")
        assert response.json()["status"] == "cancelled"
        with platform.db.session() as session:
            assert claim(session, "background", "worker", 120) is None

        running = platform.queue_sync(dataset.id)
        with platform.db.session() as session:
            job = claim(session, "background", "worker", 120)
            job_id, generation = job.id, job.lease_generation
        assert running.id == job_id
        assert client.post(f"/api/v1/runs/{running.id}/cancel").json()["status"] == "cancelling"
        assert platform.process(job_id, generation)["status"] == "cancelled"
        with platform.db.session() as session:
            assert session.get(Dataset, dataset.id).active_version_id is None

        bad_source = platform.add_source("missing", "file", {})
        bad_dataset = platform.add_dataset(bad_source.id, "missing_data", "absent.csv")
        failed = platform.queue_sync(bad_dataset.id)
        assert work(platform, "background")["status"] == "failed"
        assert client.post(f"/api/v1/runs/{failed.id}/retry").json()["status"] == "pending"


def test_expired_worker_is_fenced_and_work_is_recovered(platform: Platform) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    submitted = platform.queue_sync(dataset.id)
    with platform.db.session() as session:
        first = claim(session, "background", "lost-worker", 10)
        old_generation = first.lease_generation
        first.lease_until = now() - timedelta(seconds=1)
    with platform.db.session() as session:
        recovered = claim(session, "background", "new-worker", 10)
        assert recovered.id == submitted.id
        assert recovered.attempt == 2
        assert recovered.lease_generation > old_generation
        assert session.query(JobAttempt).filter_by(job_id=submitted.id).count() == 2
    with pytest.raises(RuntimeError, match="lease is no longer current"):
        platform.process(submitted.id, old_generation)
    assert platform.process(submitted.id, recovered.lease_generation)["status"] == "succeeded"


def test_http_source_requires_server_allowlist(platform: Platform) -> None:
    with pytest.raises(ValueError, match="not enabled"):
        platform.add_source("external", "http", {"url": "https://example.org/items"})
    allowed = Platform(platform.settings.model_copy(update={"http_allowed_hosts": "example.org"}))
    source = allowed.add_source("external", "http", {"url": "https://example.org/items"})
    assert source.kind == "http"


def test_cancellation_during_build_cannot_publish(platform: Platform, monkeypatch) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    submitted = platform.queue_sync(dataset.id)
    with platform.db.session() as session:
        running = claim(session, "background", "worker", 120)
        generation = running.lease_generation
    write_table = platform.store.write_table

    def cancel_before_publish(*args, **kwargs):
        result = write_table(*args, **kwargs)
        with platform.db.session() as session:
            from fluxplane.scheduling.jobs import cancel

            cancel(session, submitted.id)
        return result

    monkeypatch.setattr(platform.store, "write_table", cancel_before_publish)
    assert platform.process(submitted.id, generation)["status"] == "cancelled"
    with platform.db.session() as session:
        assert session.get(Dataset, dataset.id).active_version_id is None


def test_sync_failure_after_file_write_leaves_only_collectible_artifact(
    platform: Platform, monkeypatch,
) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    original = platform.store.write_table

    def fail_after_write(kind, table):
        result = original(kind, table)
        if kind == "datasets":
            raise RuntimeError("simulated failure after artifact write")
        return result

    monkeypatch.setattr(platform.store, "write_table", fail_after_write)
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "failed"
    with platform.db.session() as session:
        assert session.get(Dataset, dataset.id).active_version_id is None
        assert session.scalar(select(DatasetVersion)) is None
    assert platform.storage_summary(min_age_seconds=0)["orphan_count"] == 1
    assert platform.storage_summary(collect=True, min_age_seconds=0)["orphan_count"] == 1


def test_lost_worker_reply_after_commit_cannot_duplicate_publication(
    platform: Platform, monkeypatch,
) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    submitted = platform.queue_sync(dataset.id)
    with platform.db.session() as session:
        running = claim(session, "background", "worker", 120)
        generation = running.lease_generation
    original = platform._sync

    def lose_reply(job, lease_generation):
        original(job, lease_generation)
        raise RuntimeError("simulated lost worker reply")

    monkeypatch.setattr(platform, "_sync", lose_reply)
    with pytest.raises(service_module.ConflictError, match="lease changed while processing"):
        platform.process(submitted.id, generation)
    with platform.db.session() as session:
        assert session.get(Job, submitted.id).status == "succeeded"
        assert session.scalar(select(DatasetVersion)) is not None
        assert len(session.scalars(select(DatasetVersion)).all()) == 1
        assert session.get(Dataset, dataset.id).active_version_id is not None


def test_scheduling_ages_waiting_work_and_uses_observed_duration() -> None:
    moment = now()
    old = Job(id="old", kind="refresh", lane="background", priority=5,
              created_at=moment - timedelta(seconds=210))
    recent = Job(id="recent", kind="sync", lane="background", priority=10,
                 created_at=moment)
    assert order_ready([recent, old], {}, moment)[0].id == "old"

    short = Job(id="short", kind="refresh", lane="background", priority=5,
                created_at=moment)
    long = Job(id="long", kind="sync", lane="background", priority=5,
               created_at=moment)
    assert order_ready([long, short], {"sync": 800, "refresh": 40}, moment)[0].id == "short"


def test_resource_admission_across_execution_pools(platform: Platform) -> None:
    with platform.db.session() as session:
        large = submit(session, "query", "query", cpu_millicores=1500, memory_mb=1536)
        background = submit(session, "sync", "background", cpu_millicores=1000,
                            memory_mb=1024)
    with platform.db.session() as session:
        running = claim(session, "query", "query-worker", 120, max_running=2,
                        cpu_budget=2000, memory_budget=2048)
        assert running.id == large.id
    with platform.db.session() as session:
        assert claim(session, "background", "background-worker", 120,
                     cpu_budget=2000, memory_budget=2048) is None
    with platform.db.session() as session:
        assert complete(session, running, running.lease_generation, result={})
    with platform.db.session() as session:
        admitted = claim(session, "background", "background-worker", 120,
                         cpu_budget=2000, memory_budget=2048)
        assert admitted.id == background.id
    with platform.db.session() as session:
        assert complete(session, admitted, admitted.lease_generation, result={})
        oversized = submit(session, "sync", "background", cpu_millicores=3000)
    with platform.db.session() as session:
        assert claim(session, "background", "background-worker", 120,
                     cpu_budget=2000, memory_budget=2048) is None
    with platform.db.session() as session:
        failed = session.get(Job, oversized.id)
        assert failed.status == "failed"
        assert "resource_unschedulable" in failed.error


def test_idle_background_worker_borrows_query_capacity(platform: Platform) -> None:
    with platform.db.session() as session:
        first_query = submit(session, "query", "query")
        second_query = submit(session, "query", "query")
        next_query = submit(session, "query", "query")
        background = submit(session, "sync", "background")
    options = {"query_workers": 1, "background_workers": 1}
    with platform.db.session() as session:
        home = claim_for_worker(session, "background", "background-worker", 120, **options)
        assert home.id == background.id
    with platform.db.session() as session:
        native = claim_for_worker(session, "query", "query-worker", 120, **options)
        assert native.id == first_query.id
    with platform.db.session() as session:
        assert complete(session, home, home.lease_generation, result={})
    with platform.db.session() as session:
        assert claim_for_worker(session, "background", "background-worker", 120,
                                allow_borrow=False, **options) is None
        borrowed = claim_for_worker(session, "background", "background-worker", 120,
                                    **options)
        assert borrowed.id == second_query.id
        assert borrowed.lane == "query"
        assert claim_for_worker(session, "query", "other-query-worker", 120,
                                **options) is None
    with platform.db.session() as session:
        another_background = submit(session, "refresh", "background")
        assert session.get(Job, borrowed.id).status == "running"
        assert session.get(Job, another_background.id).status == "pending"
    with platform.db.session() as session:
        assert complete(session, borrowed, borrowed.lease_generation, result={})
    with platform.db.session() as session:
        chosen = claim_for_worker(session, "background", "background-worker", 120,
                                  **options)
        assert chosen.id == another_background.id
        assert session.get(Job, next_query.id).status == "pending"
    query_pool = next(pool for pool in platform.status()["pools"] if pool["lane"] == "query")
    assert query_pool["capacity"] == 1
    assert query_pool["max_capacity"] == 2


def test_resource_admission_can_run_smaller_ready_task(platform: Platform) -> None:
    with platform.db.session() as session:
        running = submit(session, "query", "query", cpu_millicores=1500, memory_mb=1536)
        blocked = submit(session, "sync", "background", cpu_millicores=1000,
                         memory_mb=1024, priority=10)
        fitting = submit(session, "refresh", "background", cpu_millicores=500,
                         memory_mb=512, priority=5)
    with platform.db.session() as session:
        assert claim(session, "query", "reader", 120, cpu_budget=2000,
                     memory_budget=2048).id == running.id
    with platform.db.session() as session:
        selected = claim(session, "background", "worker", 120, cpu_budget=2000,
                         memory_budget=2048)
        assert selected.id == fitting.id
        assert session.get(Job, blocked.id).status == "pending"


def test_http_json_and_jsonl_sources_join(platform: Platform) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = b'{"data":[{"product_id":10,"stock":4},{"product_id":11,"stock":7}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        (platform.settings.source_root / "catalog.jsonl").write_text(
            '{"product_id":10,"price":20}\n{"product_id":11,"price":30}\n',
            encoding="utf-8",
        )
        allowed = Platform(platform.settings.model_copy(update={"http_allowed_hosts": "127.0.0.1"}))
        catalog = allowed.add_source("catalog-file", "file", {})
        inventory = allowed.add_source("inventory-api", "http", {
            "url": f"http://127.0.0.1:{server.server_port}/inventory",
        })
        for source, name, source_object in (
            (catalog, "catalog", "catalog.jsonl"),
            (inventory, "inventory", "inventory"),
        ):
            dataset = allowed.add_dataset(source.id, name, source_object)
            allowed.queue_sync(dataset.id)
            assert work(allowed, "background")["status"] == "succeeded"
        allowed.queue_query(
            "SELECT c.product_id, c.price, i.stock FROM catalog c "
            "JOIN inventory i ON c.product_id=i.product_id ORDER BY c.product_id"
        )
        assert work(allowed, "query")["result"]["rows"] == [
            {"product_id": 10, "price": 20, "stock": 4},
            {"product_id": 11, "price": 30, "stock": 7},
        ]
        with pytest.raises(ValueError, match="unique SQL field names"):
            allowed.add_source("bad-map", "http", {
                "url": f"http://127.0.0.1:{server.server_port}/inventory",
                "field_map": {"product_id": "key", "stock": "key"},
            })
        collision = allowed.add_source("colliding-map", "http", {
            "url": f"http://127.0.0.1:{server.server_port}/inventory",
            "field_map": {"product_id": "stock"},
        })
        bad_dataset = allowed.add_dataset(collision.id, "bad_inventory", "inventory")
        allowed.queue_sync(bad_dataset.id)
        assert "mapping collides" in work(allowed, "background")["error"]
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_http_source_follows_same_origin_pages(platform: Platform) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            payload = (b'{"data":[{"id":1}],"next":"/records?page=2"}'
                       if self.path == "/records" else b'{"data":[{"id":2}],"next":null}')
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        allowed = Platform(platform.settings.model_copy(update={"http_allowed_hosts": "127.0.0.1"}))
        source = allowed.add_source("paginated-api", "http", {
            "url": f"http://127.0.0.1:{server.server_port}/records",
        })
        dataset = allowed.add_dataset(source.id, "records", "records")
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["row_count"] == 2
        allowed.queue_query("SELECT SUM(id) AS total FROM records")
        assert work(allowed, "query")["result"]["rows"] == [{"total": "3"}]
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_http_source_uses_configured_record_and_next_paths(platform: Platform) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            page = 1 if self.path == "/records" else 2
            payload = json.dumps({
                "payload": {"records": [{"externalId": page}]},
                "paging": {"next": "/records?page=2" if page == 1 else None},
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        allowed = Platform(platform.settings.model_copy(update={"http_allowed_hosts": "127.0.0.1"}))
        config = {"url": f"http://127.0.0.1:{server.server_port}/records",
                  "records_path": "payload.records", "next_path": "paging.next",
                  "field_map": {"externalId": "external_id"}}
        source = allowed.add_source("configured-api", "http", config)
        dataset = allowed.add_dataset(source.id, "configured_records", "records")
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["row_count"] == 2
        allowed.queue_query("SELECT SUM(external_id) AS total FROM configured_records")
        assert work(allowed, "query")["result"]["rows"] == [{"total": "3"}]
        with pytest.raises(ValueError, match="nonempty field paths"):
            allowed.add_source("bad-path", "http", {**config, "records_path": "payload..records"})
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_http_timestamp_cursor_merges_changes_and_reconciles_deletions(platform: Platform) -> None:
    old = datetime(2026, 1, 1, tzinfo=UTC)
    new = old + timedelta(hours=1)

    class Handler(BaseHTTPRequestHandler):
        rows: ClassVar[list[dict]] = [
            {"id": 1, "amount": 10, "updated_at": old.isoformat(), "is_deleted": False},
            {"id": 2, "amount": 20, "updated_at": old.isoformat(), "is_deleted": False},
        ]
        requested_cursors: ClassVar[list[str]] = []

        def do_GET(self):
            params = parse_qs(urlparse(self.path).query)
            since = params.get("updated_after", [None])[0]
            if since:
                self.requested_cursors.append(since)
            rows = [row for row in self.rows if not since or
                    datetime.fromisoformat(row["updated_at"]) >= datetime.fromisoformat(since)]
            payload = json.dumps({"data": rows}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        allowed = Platform(platform.settings.model_copy(update={"http_allowed_hosts": "127.0.0.1"}))
        source = allowed.add_source("cursor-api", "http", {
            "url": f"http://127.0.0.1:{server.server_port}/orders",
            "primary_key": "order_id", "updated_at": "source_updated_at",
            "deleted_column": "removed",
            "field_map": {"id": "order_id", "amount": "amount_cents",
                          "updated_at": "source_updated_at", "is_deleted": "removed"},
        })
        dataset = allowed.add_dataset(source.id, "remote_orders", "orders")
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["source_read"] == "snapshot"
        Handler.rows = [
            {"id": 1, "amount": 15, "updated_at": new.isoformat(), "is_deleted": False},
            {"id": 2, "amount": 20, "updated_at": new.isoformat(), "is_deleted": True},
            {"id": 3, "amount": 30, "updated_at": new.isoformat(), "is_deleted": False},
        ]
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["source_read"] == "cursor"
        assert Handler.requested_cursors
        allowed.queue_query("SELECT order_id, amount_cents FROM remote_orders ORDER BY order_id")
        assert work(allowed, "query")["result"]["rows"] == [
            {"order_id": 1, "amount_cents": 15},
            {"order_id": 3, "amount_cents": 30},
        ]
        Handler.rows.append({
            "id": 4, "amount": 0, "updated_at": (new + timedelta(hours=1)).isoformat(),
            "is_deleted": True,
        })
        allowed.queue_sync(dataset.id)
        advanced = work(allowed, "background")["result"]
        assert advanced["changed"] is True
        assert advanced["row_count"] == 2
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["changed"] is False
        Handler.rows = Handler.rows[:2]
        allowed.queue_sync(dataset.id, force_full=True)
        assert work(allowed, "background")["result"]["source_read"] == "snapshot"
        allowed.queue_query("SELECT order_id FROM remote_orders ORDER BY order_id")
        assert work(allowed, "query")["result"]["rows"] == [{"order_id": 1}]
        with allowed.db.session() as session:
            for job in session.scalars(select(Job).where(Job.kind == "sync",
                                                     Job.target_id == dataset.id)).all():
                job.updated_at = now() - timedelta(days=2)
        scheduled = allowed.poll_remote_sources()
        assert len(scheduled) == 1
        with allowed.db.session() as session:
            assert session.get(Job, scheduled[0]).payload["force_full"] is True
        assert work(allowed, "background")["status"] == "succeeded"
        Handler.rows = []
        allowed.queue_sync(dataset.id, force_full=True)
        assert work(allowed, "background")["result"]["row_count"] == 0
        Handler.rows = [{"id": 5, "amount": 50, "updated_at": new.isoformat(),
                         "is_deleted": False}]
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["row_count"] == 1
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_remote_poll_schedules_changes_without_duplicate_work(platform: Platform) -> None:
    class Handler(BaseHTTPRequestHandler):
        amount = 5

        def do_GET(self):
            payload = f'[{{"id":1,"amount":{self.amount}}}]'.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        allowed = Platform(platform.settings.model_copy(update={
            "http_allowed_hosts": "127.0.0.1", "remote_poll_seconds": 1,
        }))
        source = allowed.add_source("changing-api", "http", {
            "url": f"http://127.0.0.1:{server.server_port}/records",
        })
        dataset = allowed.add_dataset(source.id, "remote_records", "remote_records")
        scheduled = allowed.poll_remote_sources()
        assert len(scheduled) == 1
        assert allowed.poll_remote_sources() == []
        assert work(allowed, "background")["result"]["row_count"] == 1
        assert allowed.poll_remote_sources() == []
        before_files = allowed.storage_summary()["artifact_count"]
        allowed.queue_sync(dataset.id)
        assert work(allowed, "background")["result"]["changed"] is False
        assert allowed.storage_summary()["artifact_count"] == before_files

        Handler.amount = 9
        with allowed.db.session() as session:
            for job in session.scalars(select(Job).where(
                Job.kind == "sync", Job.target_id == dataset.id,
            )).all():
                job.updated_at = now() - timedelta(seconds=2)
        assert len(allowed.poll_remote_sources()) == 1
        assert work(allowed, "background")["result"]["changed"] is True
        allowed.queue_query("SELECT amount FROM remote_records")
        assert work(allowed, "query")["result"]["rows"] == [{"amount": 9}]
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


def test_incompatible_schema_keeps_the_published_version(platform: Platform) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        first_version = session.get(Dataset, dataset.id).active_version_id

    (platform.settings.source_root / "orders.csv").write_text(
        "customer_id,product_id\n1,10\n", encoding="utf-8"
    )
    platform.queue_sync(dataset.id)
    outcome = work(platform, "background")
    assert outcome["status"] == "failed"
    assert "incompatible schema change" in outcome["error"]
    with platform.db.session() as session:
        assert session.get(Dataset, dataset.id).active_version_id == first_version
    platform.queue_query("SELECT SUM(amount) AS total FROM orders")
    assert work(platform, "query")["result"]["rows"] == [{"total": "16"}]


def test_additive_connector_schema_is_published_even_when_source_is_unchanged(
    platform: Platform, monkeypatch,
) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    platform.queue_sync(dataset.id)
    first = work(platform, "background")["result"]["version_id"]
    original = service_module.read_snapshot

    def with_location(kind, config, source_object, settings):
        snapshot = original(kind, config, source_object, settings)
        table = snapshot.table.append_column(
            "source_location", pa.array(["orders.csv"] * snapshot.table.num_rows)
        )
        return SourceSnapshot(table, snapshot.fingerprint, snapshot.source_cursor, snapshot.mode)

    monkeypatch.setattr(service_module, "read_snapshot", with_location)
    platform.queue_sync(dataset.id)
    second = work(platform, "background")["result"]
    assert second["changed"] is True
    assert second["version_id"] != first
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["result"]["changed"] is False
    platform.queue_query("SELECT DISTINCT source_location FROM orders")
    assert work(platform, "query")["result"]["rows"] == [
        {"source_location": "orders.csv"}
    ]


def test_unchanged_source_updates_an_old_field_profile_in_place(platform: Platform) -> None:
    source = platform.add_source("files", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    platform.queue_sync(dataset.id)
    first = work(platform, "background")["result"]["version_id"]
    with platform.db.session() as session:
        profile = session.get(DatasetProfile, first)
        profile.fields = [{key: value for key, value in field.items()
                           if key != "distribution"} for field in profile.fields]
    platform.queue_sync(dataset.id)
    unchanged = work(platform, "background")["result"]
    assert unchanged["changed"] is False and unchanged["version_id"] == first
    with platform.db.session() as session:
        profile = session.get(DatasetProfile, first)
        assert all("distribution" in field for field in profile.fields)
        assert next(field for field in profile.fields if field["name"] == "amount")[
            "distribution"]["median"] == 5
        assert session.scalar(select(DatasetVersion).where(
            DatasetVersion.dataset_id == dataset.id).order_by(DatasetVersion.number.desc()
        )).id == first


def test_release_restore_is_atomic_and_queries_keep_their_input_vector(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    feature = platform.add_feature(
        "spend", "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id",
        ["orders"], ["customer_id"],
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        old_release_id = session.get(CatalogState, 1).active_release_id

    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text() + "2,10,7\n", encoding="utf-8")
    platform.queue_sync(orders.id)
    assert work(platform, "background")["status"] == "succeeded"
    with pytest.raises(ValueError, match="stale"):
        platform.queue_query("SELECT * FROM spend")
    assert work(platform, "background")["status"] == "succeeded"
    new_query = platform.queue_query("SELECT * FROM spend ORDER BY customer_id")

    with TestClient(create_app(platform.settings)) as client:
        assert any(item["id"] == old_release_id for item in client.get("/api/v1/releases").json())
        response = client.post(f"/api/v1/releases/{old_release_id}/restore")
        assert response.status_code == 200
        assert response.json()["restored_from_id"] == old_release_id
        restored_id = response.json()["id"]
    with platform.db.session() as session:
        restored = session.get(Release, restored_id)
    assert restored.parent_id != old_release_id
    assert restored.restored_from_id == old_release_id
    old_query = platform.queue_query("SELECT * FROM spend ORDER BY customer_id")
    assert work(platform, "query")["result"]["rows"] == [
        {"customer_id": 1, "total": "13"}, {"customer_id": 2, "total": "10"},
    ]
    assert work(platform, "query")["result"]["rows"] == [
        {"customer_id": 1, "total": "13"}, {"customer_id": 2, "total": "3"},
    ]
    with platform.db.session() as session:
        assert session.get(Job, new_query.id).payload["release_id"] != restored.id
        assert session.get(Job, old_query.id).payload["release_id"] == restored.id
        assert session.get(Release, restored.id).dataset_versions["orders"] == (
            session.get(Release, old_release_id).dataset_versions["orders"]
        )
    platform.queue_sync(orders.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        newest = session.get(Dataset, orders.id).active_version_id
        assert session.get(DatasetVersion, newest).number == 3


def test_pool_quota_keeps_second_background_job_waiting(platform: Platform) -> None:
    source = platform.add_source("files", "file", {})
    first = platform.add_dataset(source.id, "orders", "orders.csv")
    second = platform.add_dataset(source.id, "copy_orders", "orders.csv")
    platform.queue_sync(first.id)
    platform.queue_sync(second.id)
    with platform.db.session() as session:
        running = claim(session, "background", "worker-a", 120, max_running=1)
        assert running is not None
        first_id, generation = running.id, running.lease_generation
    with platform.db.session() as session:
        assert claim(session, "background", "worker-b", 120, max_running=1) is None
        waiting = session.scalar(select(Job).where(Job.status == "pending"))
        assert waiting is not None
    assert platform.process(first_id, generation)["status"] == "succeeded"
    with platform.db.session() as session:
        next_job = claim(session, "background", "worker-b", 120, max_running=1)
        assert next_job is not None
        assert next_job.id == waiting.id


def test_orphan_collection_preserves_versions_and_running_reads(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    with platform.db.session() as session:
        version = session.get(DatasetVersion, session.get(Dataset, orders.id).active_version_id)
        retained = Path(version.artifact_path)
    orphan_path, _ = platform.store.write_table("queries", pa.table({"value": [1]}))
    old_time = time.time() - 7200
    os.utime(orphan_path, (old_time, old_time))
    assert platform.storage_summary(min_age_seconds=3600)["orphan_count"] == 1

    platform.queue_query("SELECT COUNT(*) AS n FROM orders")
    with platform.db.session() as session:
        running = claim(session, "query", "reader", 120)
        job_id, generation = running.id, running.lease_generation
    with pytest.raises(ValueError, match="tasks are running"):
        platform.storage_summary(collect=True, min_age_seconds=3600)
    assert platform.process(job_id, generation)["status"] == "succeeded"

    result = platform.storage_summary(collect=True, min_age_seconds=3600)
    assert result["orphan_count"] == 1
    assert not Path(orphan_path).exists()
    assert retained.exists()


def test_storage_verification_detects_corrupted_published_artifact(platform: Platform) -> None:
    orders, _ = prepare_sources(platform)
    assert platform.verify_storage()["ok"] is True
    with platform.db.session() as session:
        version = session.get(DatasetVersion, session.get(Dataset, orders.id).active_version_id)
        artifact = Path(version.artifact_path)
    with artifact.open("ab") as handle:
        handle.write(b"corrupted")
    report = platform.verify_storage()
    assert report["ok"] is False
    assert any(str(artifact) in problem for problem in report["problems"])


def test_append_aggregate_uses_delta_then_falls_back_on_rewrite(platform: Platform) -> None:
    source = platform.add_source("orders-file", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    feature = platform.add_feature(
        "customer_totals", None, ["orders"], ["customer_id"],
        {"mode": "append_sum_count", "measure": "amount",
         "sum_alias": "total_amount", "count_alias": "order_count"},
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["result"]["maintenance"] == "full"

    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text() + "2,10,7\n", encoding="utf-8")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        current = session.get(Dataset, dataset.id)
        delta = session.get(DatasetDelta, current.active_version_id)
        assert delta.append_only is True
        assert delta.row_count == 1
    assert work(platform, "background")["result"]["maintenance"] == "incremental"

    platform.queue_query("SELECT * FROM customer_totals ORDER BY customer_id")
    platform.queue_query(
        "SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS order_count "
        "FROM orders GROUP BY customer_id ORDER BY customer_id"
    )
    aggregate_rows = work(platform, "query")["result"]["rows"]
    reference_rows = work(platform, "query")["result"]["rows"]
    assert aggregate_rows == reference_rows == [
        {"customer_id": 1, "total_amount": "13", "order_count": 2},
        {"customer_id": 2, "total_amount": "10", "order_count": 2},
    ]

    path.write_text(path.read_text().replace("1,10,5", "1,10,50"), encoding="utf-8")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        current = session.get(Dataset, dataset.id)
        assert session.get(DatasetDelta, current.active_version_id).append_only is False
    assert work(platform, "background")["result"]["maintenance"] == "full"
    platform.queue_query("SELECT * FROM customer_totals ORDER BY customer_id")
    assert work(platform, "query")["result"]["rows"] == [
        {"customer_id": 1, "total_amount": "58", "order_count": 2},
        {"customer_id": 2, "total_amount": "10", "order_count": 2},
    ]


def test_large_append_recomputes_feature_instead_of_merging_delta(platform: Platform) -> None:
    source = platform.add_source("orders-file", "file", {})
    dataset = platform.add_dataset(source.id, "orders", "orders.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    feature = platform.add_feature(
        "customer_totals", None, ["orders"], ["customer_id"],
        {"mode": "append_sum_count", "measure": "amount",
         "sum_alias": "total_amount", "count_alias": "order_count"},
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["status"] == "succeeded"
    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text() + "2,10,7\n2,10,7\n2,10,7\n", encoding="utf-8")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with platform.db.session() as session:
        current = session.get(Dataset, dataset.id)
        assert session.get(DatasetDelta, current.active_version_id).append_only is True
    assert work(platform, "background")["result"]["maintenance"] == "full"
    platform.queue_query("SELECT * FROM customer_totals ORDER BY customer_id")
    platform.queue_query(
        "SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS order_count "
        "FROM orders GROUP BY customer_id ORDER BY customer_id"
    )
    assert work(platform, "query")["result"]["rows"] == work(platform, "query")["result"]["rows"]


def test_parquet_and_pdf_sources_are_queryable(platform: Platform) -> None:
    root = platform.settings.source_root
    pq.write_table(pa.table({"product_id": [10, 11], "price": [20, 30]}), root / "prices.parquet")
    document = fitz.open()
    page = document.new_page()
    page.insert_text((72, 72), "Prices are refreshed daily.")
    page = document.new_page()
    page.insert_text((72, 72), "Check the second page for exceptions.")
    document.save(root / "guide.pdf")
    document.close()

    file_source = platform.add_source("prices-file", "file", {})
    pdf_source = platform.add_source("price-guide", "document", {})
    prices = platform.add_dataset(file_source.id, "prices", "prices.parquet")
    guide = platform.add_dataset(pdf_source.id, "guide", "guide.pdf")
    for dataset in (prices, guide):
        platform.queue_sync(dataset.id)
        assert work(platform, "background")["status"] == "succeeded"
    platform.queue_query("SELECT SUM(price) AS total FROM prices")
    assert work(platform, "query")["result"]["rows"] == [{"total": "50"}]
    platform.queue_query(
        "SELECT chunk_index, page_number, char_start, char_end, text "
        "FROM guide ORDER BY chunk_index"
    )
    rows = work(platform, "query")["result"]["rows"]
    assert [row["page_number"] for row in rows] == [1, 2]
    assert [row["chunk_index"] for row in rows] == [0, 1]
    assert all(row["char_start"] == 0 and row["char_end"] == len(row["text"])
               for row in rows)
    assert "refreshed daily" in rows[0]["text"]
    assert "second page" in rows[1]["text"]


def test_text_document_chunks_keep_character_positions(platform: Platform) -> None:
    content = "a" * 2000 + "important clause"
    (platform.settings.source_root / "policy.md").write_text(content, encoding="utf-8")
    source = platform.add_source("policy-doc", "document", {})
    dataset = platform.add_dataset(source.id, "policy", "policy.md")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    platform.queue_query(
        "SELECT chunk_index, page_number, char_start, char_end, text "
        "FROM policy ORDER BY chunk_index"
    )
    rows = work(platform, "query")["result"]["rows"]
    assert [(row["chunk_index"], row["page_number"], row["char_start"],
             row["char_end"]) for row in rows] == [(0, None, 0, 2000),
                                                   (1, None, 2000, len(content))]
    assert rows[1]["text"] == "important clause"


def test_registered_python_feature_tracks_document_versions(platform: Platform) -> None:
    document = platform.settings.source_root / "policy.md"
    document.write_text("Returns and returns are accepted.", encoding="utf-8")
    source = platform.add_source("policy-source", "document", {})
    dataset = platform.add_dataset(source.id, "policy", "policy.md")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"

    rule = {"mode": "python", "operator": "text_match_count", "version": "1",
            "text_column": "text", "terms": ["return"], "output_column": "matches"}
    feature = platform.add_feature("policy_matches", None, ["policy"],
                                   ["source", "chunk_index"], rule)
    with TestClient(create_app(platform.settings)) as client:
        assert client.get("/api/v1/feature-operators").json()[0]["name"] == "text_match_count"
        assert client.get("/api/v1/features").json()[0]["maintenance"]["version"] == "1"
    platform.queue_refresh(feature.id)
    first = work(platform, "background")
    assert first["status"] == "succeeded"
    assert first["result"]["maintenance"] == "python"
    platform.queue_query("SELECT matches FROM policy_matches")
    assert work(platform, "query")["result"]["rows"] == [{"matches": 2}]

    document.write_text("No matching text remains.", encoding="utf-8")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    assert work(platform, "background")["status"] == "succeeded"
    platform.queue_query("SELECT matches FROM policy_matches")
    assert work(platform, "query")["result"]["rows"] == [{"matches": 0}]
    with platform.db.session() as session:
        from fluxplane.storage.models import FeatureSnapshot

        prior = session.get(FeatureSnapshot, first["result"]["snapshot_id"])
        assert platform.store.read_table(prior.artifact_path).column("matches")[0].as_py() == 2

    with pytest.raises(ValueError, match="unknown Python operator"):
        platform.add_feature("bad_operator", None, ["policy"], ["source", "chunk_index"],
                             {**rule, "version": "2"})
    with pytest.raises(ValueError, match="input columns are missing"):
        platform.add_feature("bad_column", None, ["policy"], ["source", "chunk_index"],
                             {**rule, "text_column": "unknown"})


def test_stable_file_change_schedules_sync_and_feature_refresh(platform: Platform) -> None:
    source = platform.add_source("orders-file", "file", {})
    platform.add_dataset(source.id, "orders", "orders.csv")
    seen, scheduled = platform.poll_file_sources({})
    assert not scheduled
    seen, scheduled = platform.poll_file_sources(seen)
    assert len(scheduled) == 1
    assert work(platform, "background")["status"] == "succeeded"

    feature = platform.add_feature(
        "totals", None, ["orders"], ["customer_id"],
        {"mode": "append_sum_count", "measure": "amount",
         "sum_alias": "total", "count_alias": "row_count"},
    )
    platform.queue_refresh(feature.id)
    assert work(platform, "background")["status"] == "succeeded"

    path = platform.settings.source_root / "orders.csv"
    path.write_text(path.read_text() + "2,10,7\n", encoding="utf-8")
    seen, scheduled = platform.poll_file_sources(seen)
    assert not scheduled
    seen, scheduled = platform.poll_file_sources(seen)
    assert len(scheduled) == 1
    assert work(platform, "background")["status"] == "succeeded"
    assert work(platform, "background")["result"]["maintenance"] == "incremental"
    platform.queue_query("SELECT total FROM totals WHERE customer_id=2")
    assert work(platform, "query")["result"]["rows"] == [{"total": "10"}]

