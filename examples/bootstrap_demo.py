"""Populate a self-contained demo using the same catalog and workers as the API."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from fluxplane.application.service import Platform
from fluxplane.config import settings
from fluxplane.scheduling.jobs import claim
from fluxplane.storage.models import Dataset, Feature, Job, Source
from sqlalchemy import select


def make_sources(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    orders = root / "orders.csv"
    if not orders.exists():
        orders.write_text(
            "customer_id,product_id,amount,order_date\n"
            "1001,10,149.00,2026-11-01\n"
            "1001,11,89.00,2026-11-03\n"
            "1002,10,149.00,2026-11-04\n"
            "1003,12,219.00,2026-11-05\n",
            encoding="utf-8",
        )
    products = root / "products.sqlite3"
    if not products.exists():
        with sqlite3.connect(products) as connection:
            connection.execute(
                "CREATE TABLE products(product_id INTEGER PRIMARY KEY, name TEXT, category TEXT)"
            )
            connection.executemany(
                "INSERT INTO products VALUES (?, ?, ?)",
                [(10, "便携键盘", "外设"), (11, "无线鼠标", "外设"), (12, "桌面显示器", "显示设备")],
            )
    notes = root / "service_notes.md"
    if not notes.exists():
        notes.write_text(
            "# 服务说明\n\n外设产品支持 30 天退货。显示设备支持 15 天退货。"
            "退款金额由客服确认后记录在退款系统。\n",
            encoding="utf-8",
        )


def get_source(platform: Platform, name: str, kind: str, config: dict) -> Source:
    with platform.db.session() as session:
        item = session.scalar(select(Source).where(Source.name == name))
    return item or platform.add_source(name, kind, config)


def get_dataset(platform: Platform, source: Source, name: str, source_object: str) -> Dataset:
    with platform.db.session() as session:
        item = session.scalar(select(Dataset).where(Dataset.name == name))
    return item or platform.add_dataset(source.id, name, source_object)


def wait_job(platform: Platform, job_id: str, lane: str, timeout_s: float = 30) -> Job:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with platform.db.session() as session:
            item = session.get(Job, job_id)
            if item.status in {"succeeded", "failed", "cancelled"}:
                if item.status != "succeeded":
                    raise RuntimeError(f"demo task failed: {item.error}")
                return item
            claimed = claim(session, lane, "demo-bootstrap", platform.settings.worker_lease_seconds)
            work = (claimed.id, claimed.lease_generation) if claimed else None
        if work:
            platform.process(*work)
        else:
            time.sleep(0.2)
    raise TimeoutError(f"demo task {job_id} did not finish")


def main() -> None:
    platform = Platform(settings())
    platform.initialize()
    make_sources(platform.settings.source_root)
    table_source = get_source(platform, "演示订单文件", "file", {})
    catalog_source = get_source(platform, "演示商品数据库", "sqlite", {"path": "products.sqlite3"})
    notes_source = get_source(platform, "演示服务文档", "document", {})
    datasets = [
        get_dataset(platform, table_source, "orders", "orders.csv"),
        get_dataset(platform, catalog_source, "products", "products"),
        get_dataset(platform, notes_source, "service_notes", "service_notes.md"),
    ]
    for dataset in datasets:
        wait_job(platform, platform.queue_sync(dataset.id).id, "background")

    with platform.db.session() as session:
        feature = session.scalar(select(Feature).where(Feature.name == "customer_spend"))
    if feature is None:
        feature = platform.add_feature(
            "customer_spend",
            "SELECT o.customer_id, SUM(o.amount) AS gross_spend, "
            "COUNT(*) AS order_count FROM orders o "
            "JOIN products p ON o.product_id = p.product_id "
            "GROUP BY o.customer_id",
            ["orders", "products"],
            ["customer_id"],
        )
    wait_job(platform, platform.queue_refresh(feature.id).id, "background")
    result = wait_job(
        platform,
        platform.queue_query("SELECT * FROM customer_spend ORDER BY customer_id").id,
        "query",
    )
    print("FluxPlane demo ready")
    for row in result.result["rows"]:
        print(row)


if __name__ == "__main__":
    main()

