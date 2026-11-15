"""Compare append-safe feature refresh with full recomputation."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.features.aggregate import aggregate_sql
from fluxplane.scheduling.jobs import claim
from fluxplane.storage.models import Dataset, DatasetDelta, Feature, FeatureSnapshot, Observation
from sqlalchemy import select


def execute_background(platform: Platform) -> tuple[str | None, dict, dict]:
    with platform.db.session() as session:
        job = claim(session, "background", "incremental-benchmark", 300)
        if job is None:
            raise RuntimeError("expected a ready background task")
        job_id, target_id, generation = job.id, job.target_id, job.lease_generation
    started = time.perf_counter()
    outcome = platform.process(job_id, generation)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if outcome["status"] != "succeeded":
        raise RuntimeError(f"background task failed: {outcome['error']}")
    with platform.db.session() as session:
        observation = session.scalar(select(Observation).where(Observation.job_id == job_id))
        metrics = {"wall_ms": round(elapsed_ms, 2), "cpu_ms": observation.cpu_ms,
                   "peak_rss_mb": observation.peak_rss_mb}
    return target_id, outcome["result"], metrics


def measure_case(rows: int, change_fraction: float) -> dict:
    append_rows = round(rows * change_fraction)
    with tempfile.TemporaryDirectory(prefix="fluxplane-incremental-") as directory:
        root = Path(directory)
        source_root = root / "sources"
        source_root.mkdir()
        source_path = source_root / "orders.parquet"
        initial = pa.table({
            "customer_id": pa.array((index % 1000 for index in range(rows)), type=pa.int32()),
            "amount": pa.array((((index * 1103515245 + 12345) & 0x7fffffff) % 100000
                                for index in range(rows)), type=pa.int64()),
        })
        pq.write_table(initial, source_path, compression="zstd")
        platform = Platform(Settings(
            database_url=f"sqlite:///{root / 'metadata.sqlite3'}",
            artifact_root=root / "artifacts", source_root=source_root,
        ))
        platform.initialize()
        source = platform.add_source("orders-file", "file", {})
        dataset = platform.add_dataset(source.id, "orders", "orders.parquet")
        platform.queue_sync(dataset.id)
        execute_background(platform)

        sql = aggregate_sql("orders", ["customer_id"], "amount", "total_amount", "order_count")
        incremental = platform.add_feature(
            "incremental_totals", None, ["orders"], ["customer_id"],
            {"mode": "append_sum_count", "measure": "amount",
             "sum_alias": "total_amount", "count_alias": "order_count"},
        )
        full = platform.add_feature("full_totals", sql, ["orders"], ["customer_id"])
        platform.queue_refresh(incremental.id)
        platform.queue_refresh(full.id)
        execute_background(platform)
        execute_background(platform)

        delta = pa.table({
            "customer_id": pa.array((index % 1000 for index in range(rows, rows + append_rows)),
                                    type=pa.int32()),
            "amount": pa.array((((index * 1103515245 + 12345) & 0x7fffffff) % 100000
                                for index in range(rows, rows + append_rows)),
                               type=pa.int64()),
        })
        pq.write_table(pa.concat_tables([initial, delta]), source_path, compression="zstd")
        platform.queue_sync(dataset.id)
        _, sync_result, sync_metrics = execute_background(platform)
        if not sync_result["changed"]:
            raise RuntimeError("appended source did not produce a version")

        refresh: dict[str, tuple[dict, dict]] = {}
        for _ in range(2):
            target_id, result, metrics = execute_background(platform)
            refresh[target_id] = result, metrics
        selected_result, selected_metrics = refresh[incremental.id]
        full_result, full_metrics = refresh[full.id]
        expected_mode = "full" if change_fraction == 0.50 else "incremental"
        if selected_result["maintenance"] != expected_mode:
            raise RuntimeError("feature refresh did not choose the expected maintenance path")
        if full_result["maintenance"] != "full":
            raise RuntimeError("reference feature did not use full recomputation")

        with platform.db.session() as session:
            current = session.get(Dataset, dataset.id)
            change = session.get(DatasetDelta, current.active_version_id)
            left = session.get(FeatureSnapshot, session.get(Feature, incremental.id).active_snapshot_id)
            right = session.get(FeatureSnapshot, session.get(Feature, full.id).active_snapshot_id)
            delta_bytes = Path(change.artifact_path).stat().st_size
        left_table = platform.store.read_table(left.artifact_path).sort_by("customer_id")
        right_table = platform.store.read_table(right.artifact_path).sort_by("customer_id")
        if not left_table.equals(right_table):
            raise RuntimeError("incremental and full feature results differ")
        return {
            "input_rows": rows, "appended_rows": append_rows,
            "change_fraction": change_fraction,
            "source_sync_ms": sync_metrics["wall_ms"],
            "source_sync_cpu_ms": sync_metrics["cpu_ms"],
            "selected_mode": selected_result["maintenance"],
            "selected_refresh_ms": selected_metrics["wall_ms"],
            "selected_cpu_ms": selected_metrics["cpu_ms"],
            "selected_peak_rss_mb": selected_metrics["peak_rss_mb"],
            "reference_full_refresh_ms": full_metrics["wall_ms"],
            "reference_full_cpu_ms": full_metrics["cpu_ms"],
            "full_over_selected": round(full_metrics["wall_ms"] / selected_metrics["wall_ms"], 3),
            "delta_artifact_bytes": delta_bytes,
            "selected_snapshot_bytes": Path(left.artifact_path).stat().st_size,
            "full_snapshot_bytes": Path(right.artifact_path).stat().st_size,
            "equal_results": True,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.rows < 1000 or args.repeats < 1:
        parser.error("--rows must be at least 1000 and --repeats must be positive")
    cases = [measure_case(args.rows, fraction)
             for fraction in (0.01, 0.10, 0.50) for _ in range(args.repeats)]
    summary = {str(fraction): {
        "selected_ms": round(statistics.median(case["selected_refresh_ms"]
                                                  for case in cases if case["change_fraction"] == fraction), 2),
        "reference_full_ms": round(statistics.median(case["reference_full_refresh_ms"]
                                           for case in cases if case["change_fraction"] == fraction), 2),
        "selected_cpu_ms": round(statistics.median(case["selected_cpu_ms"]
                                                 for case in cases if case["change_fraction"] == fraction), 2),
        "reference_full_cpu_ms": round(statistics.median(case["reference_full_cpu_ms"]
                                                       for case in cases if case["change_fraction"] == fraction), 2),
    } for fraction in (0.01, 0.10, 0.50)}
    report = {"mode": "isolated local SQLite and Parquet; platform task execution",
              "rows": args.rows, "repeats": args.repeats, "cases": cases, "median_by_fraction": summary}
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)
