"""Run the four-source business scenario against the deployed platform.

Use ``--phase seed`` to reset the demo sources, ``--phase optimize`` to show
query reuse, ``--phase update`` to apply changes, ``--phase schema-guard`` to
test a rejected source change, ``--phase restore`` to inspect release recovery,
and ``--phase verify`` to check the current result.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pymupdf
from fluxplane.application.service import Platform
from fluxplane.config import settings
from fluxplane.execution.query import materialization_signature
from fluxplane.scheduling.jobs import claim
from fluxplane.storage.models import (
    CatalogState,
    Dataset,
    DatasetVersion,
    Feature,
    FeatureMaintenance,
    Job,
    Materialization,
    Release,
    Source,
)
from sqlalchemy import create_engine, select, text

ORDER_TABLE = "demo_orders"
HTTP_URL = "http://demo-source:8091/refunds.json"

NET_SQL = (
    "WITH refunds_by_order AS (SELECT order_id, SUM(amount_cents) AS refunded "
    "FROM demo_refunds GROUP BY order_id) "
    "SELECT o.customer_id, "
    "SUM(o.amount_cents - COALESCE(r.refunded, 0)) AS net_cents, "
    "COUNT(*) AS order_count FROM demo_orders o "
    "LEFT JOIN refunds_by_order r ON o.order_id = r.order_id "
    "WHERE o.occurred_at >= CURRENT_TIMESTAMP - INTERVAL '30 days' "
    "GROUP BY o.customer_id"
)
WINDOW_NET_SQL = (
    "WITH refunds_by_order AS (SELECT order_id, SUM(amount_cents) AS refunded "
    "FROM demo_refunds GROUP BY order_id) "
    "SELECT o.customer_id, "
    "SUM(o.amount_cents - COALESCE(r.refunded, 0)) AS net_cents "
    "FROM demo_orders o LEFT JOIN refunds_by_order r ON o.order_id = r.order_id "
    "GROUP BY o.customer_id"
)
RETURN_SQL = (
    "WITH refunded AS (SELECT order_id, SUM(amount_cents) AS amount_cents "
    "FROM demo_refunds GROUP BY order_id) "
    "SELECT p.category, COUNT(*) AS order_count, "
    "SUM(CASE WHEN r.amount_cents IS NULL THEN 0 ELSE 1 END) AS refunded_orders, "
    "CAST(SUM(CASE WHEN r.amount_cents IS NULL THEN 0 ELSE 1 END) AS DOUBLE) "
    "/ COUNT(*) AS return_rate "
    "FROM demo_orders o JOIN demo_products p ON o.product_id = p.product_id "
    "LEFT JOIN refunded r ON o.order_id = r.order_id "
    "WHERE o.occurred_at >= CURRENT_TIMESTAMP - INTERVAL '30 days' "
    "GROUP BY p.category"
)
TOPIC_SQL = (
    "SELECT source, chunk_index, CASE WHEN lower(text) LIKE '%return%' "
    "OR text LIKE '%退货%' THEN 'returns' ELSE 'general' END AS topic "
    "FROM (SELECT source, chunk_index, text FROM demo_policy "
    "UNION ALL SELECT source, chunk_index, text FROM demo_faq)"
)
REPORT_SQL = (
    "WITH refunded AS (SELECT order_id, SUM(amount_cents) AS amount_cents "
    "FROM demo_refunds GROUP BY order_id), "
    "guidance AS (SELECT COUNT(*) AS documents FROM demo_policy_topics "
    "WHERE topic = 'returns') "
    "SELECT p.category, COUNT(*) AS order_count, "
    "SUM(o.amount_cents - COALESCE(r.amount_cents, 0)) AS net_cents, "
    "g.documents AS guidance_documents FROM demo_orders o "
    "JOIN demo_products p ON o.product_id = p.product_id "
    "LEFT JOIN refunded r ON o.order_id = r.order_id CROSS JOIN guidance g "
    "WHERE o.occurred_at >= CURRENT_TIMESTAMP - INTERVAL '30 days' "
    "GROUP BY p.category, g.documents ORDER BY p.category"
)
STABLE_REPORT_SQL = (
    "SELECT p.category, SUM(o.amount_cents) AS gross_cents "
    "FROM demo_orders o JOIN demo_products p ON o.product_id = p.product_id "
    "GROUP BY p.category ORDER BY p.category"
)


def write_refunds(root: Path, updated: bool) -> None:
    folder = root / "full_demo" / "http"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / "refunds.json"
    if updated:
        rows = json.loads(target.read_text(encoding="utf-8"))
        if not any(row["refund_id"] == 3 for row in rows):
            rows.append({"refund_id": 3, "order_id": 1, "amount_cents": 1000,
                         "updated_at": datetime.now(UTC).isoformat()})
    else:
        marker = datetime.now(UTC).isoformat()
        rows = [
            {"refund_id": 1, "order_id": 2, "amount_cents": 2000, "updated_at": marker},
            {"refund_id": 2, "order_id": 3, "amount_cents": 4000, "updated_at": marker},
        ]
    temporary = target.with_suffix(".partial")
    temporary.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, target)


def write_files(root: Path) -> None:
    folder = root / "full_demo"
    folder.mkdir(parents=True, exist_ok=True)
    product_path = folder / "products.parquet"
    temporary = folder / "products.partial"
    pq.write_table(pa.table({
        "product_id": [10, 11, 12],
        "name": ["Keyboard", "Mouse", "Display"],
        "category": ["Accessories", "Accessories", "Displays"],
    }), temporary)
    os.replace(temporary, product_path)
    (folder / "policy.md").write_text(
        "# 退货政策\n\n配件支持 30 天退货，显示设备支持 15 天退货。\n",
        encoding="utf-8",
    )
    pdf_path = folder / "faq.pdf"
    temporary_pdf = folder / "faq.partial"
    document = pymupdf.open()
    page = document.new_page()
    page.insert_text((72, 90), "Return FAQ: refunds are recorded after support approval.")
    document.save(temporary_pdf)
    document.close()
    os.replace(temporary_pdf, pdf_path)
    write_refunds(root, updated=False)


def source_engine():
    url = os.environ.get("FLUXPLANE_DEMO_SOURCE_URL")
    if not url:
        raise RuntimeError("FLUXPLANE_DEMO_SOURCE_URL is required for the full demo")
    engine = create_engine(url)
    for attempt in range(30):
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return engine
        except Exception:
            if attempt == 29:
                engine.dispose()
                raise
            time.sleep(1)
    raise AssertionError("unreachable")


def seed_orders(engine) -> None:
    current = datetime.now(UTC)
    with engine.begin() as connection:
        connection.execute(text(
            f"CREATE TABLE IF NOT EXISTS {ORDER_TABLE} ("
            "order_id integer PRIMARY KEY, customer_id integer NOT NULL, "
            "product_id integer NOT NULL, amount_cents integer NOT NULL, "
            "occurred_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, "
            "is_deleted boolean NOT NULL DEFAULT false)"
        ))
        connection.execute(text(f"TRUNCATE TABLE {ORDER_TABLE}"))
        for order_id, customer_id, product_id, amount, days_ago in [
            (1, 1001, 10, 12000, 5), (2, 1001, 11, 8000, 3),
            (3, 1002, 12, 20000, 2), (4, 1003, 10, 5000, 1),
        ]:
            connection.execute(text(
                f"INSERT INTO {ORDER_TABLE} "
                "(order_id, customer_id, product_id, amount_cents, occurred_at, "
                "updated_at, is_deleted) VALUES "
                "(:order_id, :customer_id, :product_id, :amount, :occurred, :updated, false)"
            ), {"order_id": order_id, "customer_id": customer_id,
                "product_id": product_id, "amount": amount,
                "occurred": current - timedelta(days=days_ago), "updated": current})


def update_orders(engine) -> None:
    current = datetime.now(UTC)
    with engine.begin() as connection:
        existing = connection.execute(text(
            f"SELECT amount_cents FROM {ORDER_TABLE} WHERE order_id = 5"
        )).scalar_one_or_none()
        if existing is not None:
            return
        connection.execute(text(
            f"UPDATE {ORDER_TABLE} SET amount_cents = 15000, "
            "updated_at = :updated WHERE order_id = 1"
        ), {"updated": current})
        connection.execute(text(
            f"UPDATE {ORDER_TABLE} SET is_deleted = true, "
            "updated_at = :updated WHERE order_id = 4"
        ), {"updated": current})
        connection.execute(text(
            f"INSERT INTO {ORDER_TABLE} "
            "(order_id, customer_id, product_id, amount_cents, occurred_at, "
            "updated_at, is_deleted) VALUES "
            "(5, 1002, 10, 7000, :occurred, :updated, false)"
        ), {"occurred": current - timedelta(days=25), "updated": current})


def get_source(platform: Platform, name: str, kind: str, config: dict) -> Source:
    with platform.db.session() as session:
        existing = session.scalar(select(Source).where(Source.name == name))
    if existing is not None:
        if existing.kind != kind or existing.config != config:
            if (name == "HTTP refunds" and existing.kind == "http" and
                    existing.config == {"url": HTTP_URL} and config["url"] == HTTP_URL):
                with platform.db.session() as session:
                    source = session.get(Source, existing.id, with_for_update=True)
                    source.config = config
                return existing
            raise RuntimeError(f"existing demo source {name} has a different definition")
        return existing
    return platform.add_source(name, kind, config)


def get_dataset(platform: Platform, source: Source, name: str, object_name: str) -> Dataset:
    with platform.db.session() as session:
        existing = session.scalar(select(Dataset).where(Dataset.name == name))
    if existing is not None:
        if existing.source_id != source.id or existing.source_object != object_name:
            raise RuntimeError(f"existing demo dataset {name} has a different definition")
        return existing
    return platform.add_dataset(source.id, name, object_name)


def get_feature(platform: Platform, name: str, sql: str,
                dependencies: list[str], keys: list[str]) -> Feature:
    with platform.db.session() as session:
        existing = session.scalar(select(Feature).where(Feature.name == name))
    if existing is not None:
        if existing.sql != sql or existing.dependencies != dependencies:
            raise RuntimeError(f"existing demo feature {name} has a different definition")
        return existing
    return platform.add_feature(name, sql, dependencies, keys)


def get_python_feature(platform: Platform) -> Feature:
    rule = {"mode": "python", "operator": "text_match_count", "version": "1",
            "text_column": "text", "terms": ["退货", "return", "refund"],
            "output_column": "match_count"}
    with platform.db.session() as session:
        existing = session.scalar(select(Feature).where(Feature.name == "demo_policy_matches"))
        if existing is not None:
            maintenance = session.get(FeatureMaintenance, existing.id)
            if existing.sql is not None or maintenance is None or maintenance.mode != "python" or \
                    maintenance.config != {key: value for key, value in rule.items() if key != "mode"}:
                raise RuntimeError("existing demo Python feature has a different definition")
            return existing
    return platform.add_feature("demo_policy_matches", None, ["demo_policy"],
                                ["source", "chunk_index"], rule)


def get_window_feature(platform: Platform) -> Feature:
    rule = {"mode": "window_sql", "windows": {
        "demo_orders": {"event_time": "occurred_at", "window_seconds": 30 * 86400},
        "demo_refunds": {"event_time": "updated_at", "window_seconds": 30 * 86400},
    }}
    name = "demo_customer_net_windowed"
    dependencies = ["demo_orders", "demo_refunds"]
    with platform.db.session() as session:
        existing = session.scalar(select(Feature).where(Feature.name == name))
        if existing is not None:
            maintenance = session.get(FeatureMaintenance, existing.id)
            if (existing.sql != WINDOW_NET_SQL or existing.dependencies != dependencies or
                    maintenance is None or maintenance.mode != "window_sql" or
                    maintenance.config != {"windows": rule["windows"]}):
                raise RuntimeError("existing demo window feature has a different definition")
            return existing
    return platform.add_feature(name, WINDOW_NET_SQL, dependencies, ["customer_id"], rule)


def wait_job(platform: Platform, job_id: str, lane: str, timeout_s: float = 90,
             expected_status: str = "succeeded") -> Job:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with platform.db.session() as session:
            item = session.get(Job, job_id)
            if item.status in {"succeeded", "failed", "cancelled"}:
                if item.status != expected_status:
                    raise RuntimeError(
                        f"demo task {job_id} ended as {item.status}: {item.error}"
                    )
                return item
            capacity = (platform.settings.query_workers if lane == "query"
                        else platform.settings.background_workers)
            claimed = claim(session, lane, "demo-runner", platform.settings.worker_lease_seconds,
                            capacity, cpu_budget=platform.settings.resource_cpu_millicores,
                            memory_budget=platform.settings.resource_memory_mb)
            work = (claimed.id, claimed.lease_generation) if claimed else None
        if work:
            platform.process(*work)
        else:
            time.sleep(0.2)
    raise TimeoutError(f"demo task {job_id} did not finish")


def require_http_source() -> None:
    for attempt in range(30):
        try:
            response = httpx.get(HTTP_URL, timeout=2)
            response.raise_for_status()
            return
        except httpx.HTTPError:
            if attempt == 29:
                raise RuntimeError("demo-source is unavailable; start Compose with --profile demo") from None
            time.sleep(1)


def register(platform: Platform, *, changed_only: bool = False) -> tuple[dict[str, Dataset], list[Feature]]:
    source_specs = [
        ("demo_orders", "PostgreSQL orders", "postgres", {
            "database_url_env": "FLUXPLANE_DEMO_SOURCE_URL", "primary_key": "order_id",
            "updated_at": "updated_at", "deleted_column": "is_deleted",
        }, ORDER_TABLE),
        ("demo_products", "Parquet products", "file", {}, "full_demo/products.parquet"),
        ("demo_refunds", "HTTP refunds", "http", {
            "url": HTTP_URL, "primary_key": "refund_id", "updated_at": "updated_at",
        }, "refunds"),
        ("demo_policy", "Markdown policy", "document", {}, "full_demo/policy.md"),
        ("demo_faq", "PDF FAQ", "document", {}, "full_demo/faq.pdf"),
    ]
    datasets: dict[str, Dataset] = {}
    for name, label, kind, config, object_name in source_specs:
        source = get_source(platform, label, kind, config)
        datasets[name] = get_dataset(platform, source, name, object_name)
    changed_datasets = {"demo_orders", "demo_refunds"}
    for name, dataset in datasets.items():
        if not changed_only or name in changed_datasets:
            wait_job(platform, platform.queue_sync(
                dataset.id, force_full=(name in {"demo_orders", "demo_refunds"} and
                                        not changed_only),
            ).id, "background")
    features = [
        get_feature(platform, "demo_customer_net_30d", NET_SQL,
                    ["demo_orders", "demo_refunds"], ["customer_id"]),
        get_feature(platform, "demo_return_rate_30d", RETURN_SQL,
                    ["demo_orders", "demo_refunds", "demo_products"], ["category"]),
        get_window_feature(platform),
        get_feature(platform, "demo_policy_topics", TOPIC_SQL,
                    ["demo_policy", "demo_faq"], ["source", "chunk_index"]),
        get_python_feature(platform),
    ]
    changed_features = {"demo_customer_net_30d", "demo_return_rate_30d",
                        "demo_customer_net_windowed"}
    for feature in features:
        if not changed_only or feature.name in changed_features:
            wait_job(platform, platform.queue_refresh(feature.id).id, "background")
    return datasets, features


def active_feature_snapshot(platform: Platform, name: str) -> str | None:
    with platform.db.session() as session:
        return session.scalar(select(Feature.active_snapshot_id).where(Feature.name == name))


def verify(platform: Platform, updated: bool) -> dict:
    report = wait_job(platform, platform.queue_query(REPORT_SQL).id, "query")
    rows = report.result["rows"]
    observed = {row["category"]: (int(row["order_count"]), int(row["net_cents"]),
                                  int(row["guidance_documents"])) for row in rows}
    expected = ({"Accessories": (3, 27000, 2), "Displays": (1, 16000, 2)} if updated
                else {"Accessories": (3, 23000, 2), "Displays": (1, 16000, 2)})
    if observed != expected:
        raise AssertionError(f"cross-source report mismatch: {observed} != {expected}")
    feature = wait_job(platform, platform.queue_query(
        "SELECT customer_id, net_cents FROM demo_customer_net_30d ORDER BY customer_id"
    ).id, "query")
    net = {int(row["customer_id"]): int(row["net_cents"])
           for row in feature.result["rows"]}
    expected_net = ({1001: 20000, 1002: 23000} if updated
                    else {1001: 18000, 1002: 16000, 1003: 5000})
    if net != expected_net:
        raise AssertionError(f"net-spend feature mismatch: {net} != {expected_net}")
    windowed = wait_job(platform, platform.queue_query(
        "SELECT customer_id, net_cents FROM demo_customer_net_windowed ORDER BY customer_id"
    ).id, "query")
    windowed_net = {int(row["customer_id"]): int(row["net_cents"])
                    for row in windowed.result["rows"]}
    if windowed_net != net:
        raise AssertionError(f"windowed cross-source feature mismatch: {windowed_net} != {net}")
    matches = wait_job(platform, platform.queue_query(
        "SELECT SUM(match_count) AS matches FROM demo_policy_matches"
    ).id, "query")
    if int(matches.result["rows"][0]["matches"]) != 3:
        raise AssertionError("document Python feature mismatch")
    return {"phase": "updated" if updated else "seeded", "report": rows,
            "customer_net_cents": net, "policy_matches": 3,
            "query_job_id": report.id}


def optimize(platform: Platform) -> dict:
    for attempt in range(12):
        query = wait_job(platform, platform.queue_query(STABLE_REPORT_SQL).id, "query")
        if query.result["cache_hit"]:
            with platform.db.session() as session:
                materialization = session.get(
                    Materialization, query.result["materialization_id"]
                )
                return {"phase": "optimized", "queries_to_hit": attempt + 1,
                        "rows": query.result["rows"],
                        "input_versions": materialization.input_versions,
                        "build_duration_ms": materialization.build_duration_ms,
                        "reuse_count": materialization.reuse_count}
        with platform.db.session() as session:
            signature = materialization_signature(STABLE_REPORT_SQL, query.result["versions"])
            candidate = session.scalar(select(Materialization).where(
                Materialization.signature == signature[0],
                Materialization.status == "building",
            ))
            pending = session.scalar(select(Job).where(
                Job.kind == "materialize", Job.target_id == candidate.id
            )) if candidate else None
        if pending is not None:
            wait_job(platform, pending.id, "background")
    raise RuntimeError("stable query did not produce a reusable materialization")


def schema_guard(platform: Platform, engine) -> dict:
    """Reject an incompatible source schema while serving the published release."""
    with platform.db.session() as session:
        dataset = session.scalar(select(Dataset).where(Dataset.name == "demo_products"))
        if dataset is None or dataset.active_version_id is None:
            raise RuntimeError("seed the full demo before testing the schema guard")
        dataset_id = dataset.id
        version_id = dataset.active_version_id
        release_id = session.get(CatalogState, 1).active_release_id
    path = platform.settings.source_root / "full_demo" / "products.parquet"
    original = path.read_bytes()
    table = pq.read_table(path)
    if "category" not in table.column_names:
        raise RuntimeError("demo products no longer contain the category field")
    with engine.connect() as connection:
        updated = connection.execute(text(
            f"SELECT EXISTS(SELECT 1 FROM {ORDER_TABLE} WHERE order_id = 5)"
        )).scalar_one()

    temporary = path.with_suffix(".schema-guard.partial")
    try:
        pq.write_table(table.drop(["category"]), temporary)
        os.replace(temporary, path)
        failed = wait_job(
            platform, platform.queue_sync(dataset_id, force_full=True).id,
            "background", expected_status="failed",
        )
        if "incompatible schema change" not in (failed.error or ""):
            raise AssertionError(f"unexpected rejection reason: {failed.error}")
        with platform.db.session() as session:
            if session.get(Dataset, dataset_id).active_version_id != version_id:
                raise AssertionError("incompatible products version was published")
            if session.get(CatalogState, 1).active_release_id != release_id:
                raise AssertionError("incompatible schema changed the active release")
        report = verify(platform, updated=updated)
    finally:
        temporary.write_bytes(original)
        os.replace(temporary, path)

    restored = wait_job(
        platform, platform.queue_sync(dataset_id, force_full=True).id, "background"
    )
    if restored.result["changed"] or restored.result["version_id"] != version_id:
        raise AssertionError("restoring the source changed the published products version")
    return {"phase": "schema-guard", "failed_job_id": failed.id,
            "rejection": failed.error, "active_release_id": release_id,
            "products_version_id": version_id, "restored_job_id": restored.id,
            "report": report["report"]}


def restore_demo(platform: Platform) -> dict:
    """Read the seeded release, then return to the latest updated release."""
    required = {"demo_customer_net_30d", "demo_return_rate_30d",
                "demo_customer_net_windowed", "demo_policy_topics", "demo_policy_matches"}
    with platform.db.session() as session:
        current = session.get(Release, session.get(CatalogState, 1).active_release_id)
        current_orders = current.dataset_versions.get("demo_orders")
        if current_orders is None or not required.issubset(current.feature_snapshots):
            raise RuntimeError("run the full demo update before restoring a release")
        previous = next((item for item in session.scalars(
            select(Release).order_by(Release.created_at.desc(), Release.id.desc())
        ) if item.dataset_versions.get("demo_orders") not in {None, current_orders}
            and required.issubset(item.feature_snapshots)), None)
        if previous is None:
            raise RuntimeError("no complete earlier demo release is available")
        current_id, previous_id = current.id, previous.id
        previous_orders = session.get(DatasetVersion, previous.dataset_versions["demo_orders"])
        current_orders_version = session.get(DatasetVersion, current_orders)
        previous_data_time = previous_orders.source_cursor["watermark"]
        current_data_time = current_orders_version.source_cursor["watermark"]

    restored_old = platform.restore_release(previous_id)
    try:
        old_report = verify(platform, updated=False)
    finally:
        restored_current = platform.restore_release(current_id)
    current_report = verify(platform, updated=True)
    return {"phase": "restore", "earlier_release_id": previous_id,
            "earlier_republished_as": restored_old.id,
            "earlier_orders_watermark": previous_data_time,
            "current_release_id": current_id,
            "current_republished_as": restored_current.id,
            "current_orders_watermark": current_data_time,
            "earlier_report": old_report["report"],
            "current_report": current_report["report"]}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["seed", "optimize", "update", "schema-guard",
                                            "restore", "verify"],
                        default="seed")
    args = parser.parse_args()
    platform = Platform(settings())
    platform.initialize()
    if "demo-source" not in {host.strip() for host in
                             platform.settings.http_allowed_hosts.split(",")}:
        raise RuntimeError("set FLUXPLANE_HTTP_ALLOWED_HOSTS=demo-source for this demo")
    engine = source_engine()
    try:
        if args.phase == "seed":
            seed_orders(engine)
            write_files(platform.settings.source_root)
        require_http_source()
        if args.phase == "seed":
            register(platform)
            result = verify(platform, updated=False)
        elif args.phase == "optimize":
            result = optimize(platform)
        elif args.phase == "update":
            policy_snapshot = active_feature_snapshot(platform, "demo_policy_topics")
            matches_snapshot = active_feature_snapshot(platform, "demo_policy_matches")
            update_orders(engine)
            write_refunds(platform.settings.source_root, updated=True)
            register(platform, changed_only=True)
            if active_feature_snapshot(platform, "demo_policy_topics") != policy_snapshot:
                raise AssertionError("unaffected document feature changed during the update")
            if active_feature_snapshot(platform, "demo_policy_matches") != matches_snapshot:
                raise AssertionError("unaffected Python feature changed during the update")
            result = verify(platform, updated=True)
        elif args.phase == "schema-guard":
            result = schema_guard(platform, engine)
        elif args.phase == "restore":
            result = restore_demo(platform)
        else:
            with engine.connect() as connection:
                updated = connection.execute(text(
                    f"SELECT EXISTS(SELECT 1 FROM {ORDER_TABLE} WHERE order_id = 5)"
                )).scalar_one()
            result = verify(platform, updated=updated)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
