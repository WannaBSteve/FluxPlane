"""Measure a repeatable local workload in an isolated temporary catalog."""

from __future__ import annotations

import argparse
import json
import os
import platform as host_platform
import statistics
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from fluxplane.api.app import create_app
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.execution.query import run_sql
from fluxplane.scheduling.jobs import claim
from fluxplane.storage.models import Dataset, DatasetLayout, DatasetVersion
from sqlalchemy import select


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def execute(platform: Platform, job_id: str, lane: str) -> tuple[dict, float]:
    with platform.db.session() as session:
        job = claim(session, lane, "local-benchmark", 300)
        if job is None or job.id != job_id:
            raise RuntimeError("benchmark task was not next in its execution pool")
        generation = job.lease_generation
    started = time.perf_counter()
    result = platform.process(job_id, generation)
    duration = (time.perf_counter() - started) * 1000
    if result["status"] != "succeeded":
        raise RuntimeError(f"benchmark task failed: {result['error']}")
    return result["result"], duration


def measure(rows: int) -> dict:
    with tempfile.TemporaryDirectory(prefix="fluxplane-benchmark-") as directory:
        root = Path(directory)
        source_root = root / "sources"
        source_root.mkdir()
        config = Settings(
            database_url=f"sqlite:///{root / 'metadata.sqlite3'}",
            artifact_root=root / "artifacts",
            source_root=source_root,
        )
        regions = ["east", "west", "north", "south"]
        pq.write_table(pa.table({
            "row_id": pa.array(range(rows), type=pa.int64()),
            "region": pa.array([regions[index % 4] for index in range(rows)]),
            "amount": pa.array([(index * 17) % 1000 for index in range(rows)]),
            "payload": pa.array(["sale-" + str(index % 1000) for index in range(rows)]),
        }), source_root / "sales.parquet", compression="zstd")
        platform = Platform(config)
        platform.initialize()
        source = platform.add_source("sales-file", "file", {})
        dataset = platform.add_dataset(source.id, "sales", "sales.parquet")
        sync_job = platform.queue_sync(dataset.id)
        _, sync_ms = execute(platform, sync_job.id, "background")

        def query(index: int) -> tuple[dict, float]:
            sql = ("SELECT COUNT(*) AS n, SUM(amount) AS total FROM sales "
                   f"WHERE region = 'east' AND row_id >= {index}")
            submitted = platform.queue_query(sql)
            return execute(platform, submitted.id, "query")

        baseline = [query(index) for index in range(5)]
        layout_job = platform.queue_layout(dataset.id, "region")
        layout, build_ms = execute(platform, layout_job.id, "background")
        if not layout["published"]:
            raise RuntimeError("layout candidate failed its measured publication gate")
        organized = [query(index) for index in range(5)]
        if [result[0]["rows"] for result in baseline] != [result[0]["rows"] for result in organized]:
            raise RuntimeError("baseline and layout query results differ")
        if any(not result[0]["layout_ids"] for result in organized):
            raise RuntimeError("published layout was not selected")

        with platform.db.session() as session:
            current = session.get(Dataset, dataset.id)
            version = session.get(DatasetVersion, current.active_version_id)
            layout_record = session.scalar(select(DatasetLayout).where(
                DatasetLayout.version_id == version.id,
            ))
            source_bytes = Path(version.artifact_path).stat().st_size
            layout_bytes = layout_record.artifact_bytes
            layout_path = layout_record.artifact_dir

        engine_baseline = []
        engine_layout = []
        for index in range(5):
            sql = ("SELECT COUNT(*) AS n, SUM(amount) AS total FROM sales "
                   f"WHERE region = 'east' AND row_id >= {index}")
            order = ("baseline", "layout") if index % 2 == 0 else ("layout", "baseline")
            results = {}
            for variant in order:
                source_input = (version.artifact_path if variant == "baseline"
                                else platform.store.open_layout(layout_path))
                started = time.perf_counter()
                results[variant] = run_sql(sql, {"sales": source_input}, platform.store)
                elapsed = (time.perf_counter() - started) * 1000
                (engine_baseline if variant == "baseline" else engine_layout).append(elapsed)
            if not results["baseline"].equals(results["layout"]):
                raise RuntimeError("engine results differ between physical layouts")

        with TestClient(create_app(config)) as client:
            control = []
            for _ in range(100):
                started = time.perf_counter()
                response = client.get("/api/v1/system/metrics")
                control.append((time.perf_counter() - started) * 1000)
                response.raise_for_status()
        return {
            "host": host_platform.platform(),
            "python": host_platform.python_version(),
            "cpu_count": os.cpu_count(),
            "duckdb": duckdb.__version__,
            "pyarrow": pa.__version__,
            "rows": rows,
            "mode": "single process, SQLite catalog, local Parquet, in-process API client",
            "sync_ms": round(sync_ms, 2),
            "query_baseline_ms": [round(duration, 2) for _, duration in baseline],
            "query_layout_ms": [round(duration, 2) for _, duration in organized],
            "query_median_speedup": round(
                statistics.median(duration for _, duration in baseline) /
                statistics.median(duration for _, duration in organized), 3,
            ),
            "engine_baseline_ms": [round(value, 2) for value in engine_baseline],
            "engine_layout_ms": [round(value, 2) for value in engine_layout],
            "engine_median_speedup": round(
                statistics.median(engine_baseline) / statistics.median(engine_layout), 3,
            ),
            "layout_build_ms": round(build_ms, 2),
            "layout_probe_baseline_ms": layout["baseline_ms"],
            "layout_probe_organized_ms": layout["organized_ms"],
            "layout_files": layout["file_count"],
            "source_snapshot_bytes": source_bytes,
            "layout_bytes": layout_bytes,
            "control_get_p95_ms": round(percentile(control, 0.95), 2),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    if arguments.rows < 10000:
        parser.error("--rows must be at least 10000")
    report = measure(arguments.rows)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(output + "\n", encoding="utf-8")
    print(output)
