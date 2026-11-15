"""Measure deployed ingestion, feature refresh, and cross-source queries at scale."""

from __future__ import annotations

import argparse
import json
import shlex
import statistics
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from verify_deployed import api_key_from_file


def inside(action: str, dataset_name: str, rows: int, feature_name: str | None) -> dict:
    import hashlib
    import os

    import pyarrow as pa
    import pyarrow.parquet as pq
    from fluxplane.application.service import Platform
    from fluxplane.config import settings
    from fluxplane.storage.models import (
        Dataset,
        DatasetVersion,
        Feature,
        FeatureSnapshot,
        Job,
        Source,
    )
    from sqlalchemy import select

    platform = Platform(settings())
    platform.initialize()
    if action == "prepare":
        with platform.db.session() as session:
            busy = session.scalar(select(Job.id).where(
                Job.status.in_(("pending", "running", "cancelling")),
            ).limit(1))
            source = session.scalar(select(Source).where(Source.name == "Scale benchmark files"))
            products = session.scalar(select(Dataset).where(Dataset.name == "demo_products"))
        if busy:
            raise RuntimeError("wait for existing tasks before starting this benchmark")
        if products is None or products.active_version_id is None:
            raise RuntimeError("run full_demo.py --phase seed before this benchmark")
        if source is None:
            source = platform.add_source("Scale benchmark files", "file", {})
        source_object = f"benchmarks/{dataset_name}.parquet"
        path = platform.settings.source_root / source_object
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".partial")
        started = time.perf_counter()
        pq.write_table(pa.table({
            "row_id": pa.array(range(rows), type=pa.int64()),
            "product_id": pa.array((10 + index % 3 for index in range(rows)),
                                   type=pa.int32()),
            "amount": pa.array(((index * 17) % 1000 for index in range(rows)),
                               type=pa.int64()),
            "payload": pa.array(hashlib.blake2b(str(index).encode(), digest_size=8).hexdigest()
                                 for index in range(rows)),
        }), temporary, compression="zstd")
        os.replace(temporary, path)
        dataset = platform.add_dataset(source.id, dataset_name, source_object)
        return {"dataset_id": dataset.id, "source_file_bytes": path.stat().st_size,
                "source_generation_ms": round((time.perf_counter() - started) * 1000, 2)}
    if action == "inspect":
        with platform.db.session() as session:
            dataset = session.scalar(select(Dataset).where(Dataset.name == dataset_name))
            feature = session.scalar(select(Feature).where(Feature.name == feature_name))
            if dataset is None or feature is None:
                raise RuntimeError("benchmark dataset or feature is missing")
            version = session.get(DatasetVersion, dataset.active_version_id)
            snapshot = session.get(FeatureSnapshot, feature.active_snapshot_id)
            if version is None or snapshot is None:
                raise RuntimeError("benchmark version or feature was not published")
            return {"dataset_version_id": version.id, "dataset_rows": version.row_count,
                    "dataset_snapshot_bytes": Path(version.artifact_path).stat().st_size,
                    "feature_snapshot_id": snapshot.id, "feature_rows": snapshot.row_count,
                    "feature_snapshot_bytes": Path(snapshot.artifact_path).stat().st_size}
    raise ValueError(f"unknown inside action: {action}")


def wait_job(client: httpx.Client, job_id: str, timeout_s: float = 90) -> dict:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        response = client.get(f"/api/v1/runs/{job_id}")
        response.raise_for_status()
        job = response.json()
        if job["status"] in {"succeeded", "failed", "cancelled"}:
            if job["status"] != "succeeded":
                raise RuntimeError(f"task {job_id} ended as {job['status']}: {job['error']}")
            return job
        time.sleep(0.1)
    raise TimeoutError(f"task {job_id} did not finish")


def latency_ms(job: dict) -> float:
    created = datetime.fromisoformat(job["created_at"])
    completed = datetime.fromisoformat(job["updated_at"])
    return round((completed - created).total_seconds() * 1000, 2)


def expected(rows: int, offset: int, *, by_product: bool = False) -> list[dict]:
    sums: dict[int, tuple[int, int]] = {}
    for row_id in range(offset, rows):
        product = 10 + row_id % 3
        count, total = sums.get(product, (0, 0))
        sums[product] = count + 1, total + (row_id * 17) % 1000
    if by_product:
        return [{"product_id": product, "n": count, "total": total}
                for product, (count, total) in sorted(sums.items())]
    groups = {"Accessories": (0, 0), "Displays": (0, 0)}
    for product, (count, total) in sums.items():
        category = "Displays" if product == 12 else "Accessories"
        old_count, old_total = groups[category]
        groups[category] = old_count + count, old_total + total
    return [{"category": category, "n": count, "total": total,
             "payload_chars": count * 16}
            for category, (count, total) in sorted(groups.items())]


def normalize(rows: list[dict], key: str) -> list[dict]:
    return [{key: row[key], "n": int(row["n"]), "total": int(row["total"]),
             **({"payload_chars": int(row["payload_chars"])}
                if "payload_chars" in row else {})}
            for row in rows]


def measure_case(client: httpx.Client, api_inside, row_count: int, tag: str,
                 query_samples: int) -> dict:
    name = f"scale_{tag}_{row_count}"
    prepared = api_inside("prepare", name, row_count)
    response = client.post(f"/api/v1/datasets/{prepared['dataset_id']}/sync")
    response.raise_for_status()
    synchronized = wait_job(client, response.json()["id"], timeout_s=180)
    if synchronized["result"]["row_count"] != row_count:
        raise AssertionError("source synchronization returned the wrong row count")
    feature_name = f"total_{name}"
    feature_sql = (f"SELECT product_id, COUNT(*) AS n, SUM(amount) AS total FROM {name} "
                   "GROUP BY product_id")
    response = client.post("/api/v1/features", json={"name": feature_name,
                           "sql": feature_sql, "dependencies": [name],
                           "entity_keys": ["product_id"]})
    response.raise_for_status()
    feature_id = response.json()["id"]
    response = client.post(f"/api/v1/features/{feature_id}/refresh", json={})
    response.raise_for_status()
    refreshed = wait_job(client, response.json()["id"], timeout_s=180)
    query_records = []
    for offset in range(query_samples):
        sql = (f"SELECT p.category, COUNT(*) AS n, SUM(s.amount) AS total, "
               f"SUM(LENGTH(s.payload)) AS payload_chars FROM {name} s "
               "JOIN demo_products p ON s.product_id = p.product_id "
               f"WHERE s.row_id >= {offset} GROUP BY p.category ORDER BY p.category")
        response = client.post("/api/v1/queries", json={"sql": sql})
        response.raise_for_status()
        result = wait_job(client, response.json()["id"], timeout_s=180)
        observed = normalize(result["result"]["rows"], "category")
        if observed != expected(row_count, offset):
            raise AssertionError(f"cross-source query mismatch at offset {offset}: {observed}")
        query_records.append({"offset": offset, "latency_ms": latency_ms(result),
                              "execution_ms": result["runtime"]["duration_ms"],
                              "peak_worker_rss_mb": result["runtime"]["peak_rss_mb"],
                              "input_versions": result["result"]["versions"],
                              "created_at": result["created_at"],
                              "updated_at": result["updated_at"],
                              "job_id": result["id"]})
    response = client.post("/api/v1/queries", json={
        "sql": f"SELECT product_id, n, total FROM {feature_name} ORDER BY product_id"})
    response.raise_for_status()
    feature_read = wait_job(client, response.json()["id"])
    if normalize(feature_read["result"]["rows"], "product_id") != expected(
            row_count, 0, by_product=True):
        raise AssertionError("published feature differs from the independent reference")
    inspected = api_inside("inspect", name, row_count, feature_name)
    first = datetime.fromisoformat(synchronized["created_at"])
    first_answer = datetime.fromisoformat(query_records[0]["updated_at"])
    last = datetime.fromisoformat(feature_read["updated_at"])
    latencies = [item["latency_ms"] for item in query_records]
    return {"rows": row_count, "dataset": name,
            **prepared, **inspected,
            "source_sync_ms": latency_ms(synchronized),
            "source_sync_runtime": synchronized["runtime"],
            "feature_refresh_ms": latency_ms(refreshed),
            "feature_refresh_runtime": refreshed["runtime"],
            "query_p50_ms": round(statistics.median(latencies), 2),
            "query_p95_ms": sorted(latencies)[int((len(latencies) - 1) * 0.95)],
            "query_records": query_records,
            "feature_read_ms": latency_ms(feature_read),
            "first_answer_ms": round((first_answer - first).total_seconds() * 1000, 2),
            "scenario_wall_ms": round((last - first).total_seconds() * 1000, 2),
            "reference_checked": True}


def run(args: argparse.Namespace) -> dict:
    compose = shlex.split(args.compose) + (["-p", args.project] if args.project else [])

    def call(*command: str, timeout: int = 60) -> str:
        result = subprocess.run(compose + list(command), check=True, capture_output=True,
                                text=True, timeout=timeout)
        return result.stdout.strip()

    def api_inside(action: str, name: str, rows: int, feature: str | None = None) -> dict:
        command = ["exec", "-T", "api", "python", "examples/benchmark_compose_scale.py",
                   "--inside", action, "--dataset", name, "--rows", str(rows)]
        if feature:
            command.extend(["--feature", feature])
        return json.loads(call(*command, timeout=90))

    required = {"api", "scheduler", "worker-background", "worker-query"}
    running = set(call("ps", "--status", "running", "--services").splitlines())
    if not required.issubset(running):
        raise RuntimeError(f"start the Compose deployment first: {sorted(required - running)}")
    call("stop", "scheduler", timeout=45)
    try:
        with httpx.Client(base_url=args.base_url,
                          headers={"x-api-key": api_key_from_file(args.env_file)},
                          timeout=20) as client:
            client.get("/health/ready").raise_for_status()
            tag = uuid4().hex[:8]
            cases = [measure_case(client, api_inside, rows, f"{tag}_{index}",
                                  args.query_samples)
                     for index, rows in enumerate(args.sizes)]
    finally:
        call("start", "scheduler", timeout=45)
    summary = {str(rows): {
        "repeats": sum(case["rows"] == rows for case in cases),
        "source_sync_ms": round(statistics.median(case["source_sync_ms"] for case in cases
                                                  if case["rows"] == rows), 2),
        "feature_refresh_ms": round(statistics.median(case["feature_refresh_ms"]
                                                      for case in cases if case["rows"] == rows), 2),
        "query_p50_ms": round(statistics.median(case["query_p50_ms"] for case in cases
                                                if case["rows"] == rows), 2),
        "first_answer_ms": round(statistics.median(case["first_answer_ms"] for case in cases
                                                  if case["rows"] == rows), 2),
    } for rows in sorted(set(args.sizes))}
    return {"checked_at_utc": datetime.now(UTC).isoformat(),
            "deployment": "isolated Compose with PostgreSQL and separate workers",
            "query_samples": args.query_samples, "summary_by_rows": summary, "cases": cases}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside", choices=["prepare", "inspect"])
    parser.add_argument("--dataset")
    parser.add_argument("--feature")
    parser.add_argument("--rows", type=int)
    parser.add_argument("--compose", default="docker compose")
    parser.add_argument("--project")
    parser.add_argument("--base-url", default="http://127.0.0.1:8088")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 1_000_000])
    parser.add_argument("--query-samples", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.inside:
        if not args.dataset or not args.rows:
            parser.error("inside mode requires --dataset and --rows")
        report = inside(args.inside, args.dataset, args.rows, args.feature)
    else:
        if args.query_samples < 3 or any(size < 1000 for size in args.sizes):
            parser.error("at least three queries and at least 1,000 rows are required")
        report = run(args)
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()
