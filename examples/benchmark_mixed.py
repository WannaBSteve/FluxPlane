"""Compare shared FIFO, fixed pools, and idle background-worker lending."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import tempfile
import time
from pathlib import Path
from threading import Event, Lock, Thread

import pyarrow as pa
import pyarrow.parquet as pq
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.profiling.runtime import process_rss_mb
from fluxplane.scheduling.jobs import claim, claim_for_worker
from fluxplane.storage.models import Job
from sqlalchemy import select


def percentile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * fraction))]


def execute_setup(platform: Platform, job_id: str) -> None:
    with platform.db.session() as session:
        job = claim(session, "background", "setup", 300)
        if job is None or job.id != job_id:
            raise RuntimeError("setup task was not ready")
        generation = job.lease_generation
    outcome = platform.process(job_id, generation)
    if outcome["status"] != "succeeded":
        raise RuntimeError(f"setup failed: {outcome['error']}")


def rss_mb() -> float:
    return process_rss_mb() or 0.0


def measure(mode: str, rows: int, query_count: int, feature_count: int,
            deadline_ms: int, late_features: int = 0,
            late_delay_ms: int = 0, late_spacing_ms: int = 0,
            borrow_grace_ms: int = 500) -> dict:
    with tempfile.TemporaryDirectory(prefix=f"fluxplane-mixed-{mode}-") as directory:
        root = Path(directory)
        source_root = root / "sources"
        source_root.mkdir()
        platform = Platform(Settings(
            database_url=f"sqlite:///{root / 'metadata.sqlite3'}",
            artifact_root=root / "artifacts", source_root=source_root,
            query_workers=1, background_workers=1,
        ))
        platform.initialize()
        data = pa.table({
            "row_id": pa.array(range(rows), type=pa.int64()),
            "region": pa.array(["east", "west", "north", "south"] * (rows // 4)),
            "amount": pa.array(((index * 1103515245 + 12345) % 100000
                                for index in range(rows)), type=pa.int64()),
        })
        pq.write_table(data, source_root / "foreground.parquet", compression="zstd")
        pq.write_table(data, source_root / "background.parquet", compression="zstd")
        source = platform.add_source("benchmark-files", "file", {})
        foreground = platform.add_dataset(source.id, "foreground", "foreground.parquet")
        background = platform.add_dataset(source.id, "background", "background.parquet")
        for dataset in (foreground, background):
            execute_setup(platform, platform.queue_sync(dataset.id).id)
        feature_sql = [
            ("SELECT row_id, SUM(amount) OVER (PARTITION BY region ORDER BY row_id "
             "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value "
             "FROM background"),
            ("SELECT row_id, AVG(amount) OVER (PARTITION BY region ORDER BY row_id "
             "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value "
             "FROM background"),
        ]
        features = [platform.add_feature(
            f"rolling_{index}", feature_sql[index % len(feature_sql)],
            ["background"], ["row_id"],
        ) for index in range(feature_count)]
        submitted: dict[str, tuple[str, float]] = {}
        for feature in features[:feature_count - late_features]:
            job = platform.queue_refresh(feature.id)
            submitted[job.id] = "background", time.monotonic()
        for index in range(query_count):
            sql = ("SELECT SUM(amount) AS total FROM foreground "
                   f"WHERE region = 'east' AND row_id >= {index}")
            job = platform.queue_query(sql)
            submitted[job.id] = "query", time.monotonic()

        gate = Lock()
        stop = Event()
        finished: dict[str, tuple[dict, float]] = {}
        errors: list[str] = []
        late_ids: list[str] = []
        expected_jobs = query_count + feature_count
        peak = [rss_mb()]
        started = time.monotonic()

        def sample_memory() -> None:
            while not stop.wait(0.02):
                peak[0] = max(peak[0], rss_mb())

        def worker(worker_id: int, protected_lane: str) -> None:
            background_idle_since = time.monotonic()
            while not stop.is_set():
                try:
                    with gate, platform.db.session() as session:
                        if mode == "fixed":
                            oldest = session.scalar(select(Job).where(
                                Job.id.in_(submitted), Job.status == "pending",
                            ).order_by(Job.created_at.asc()).limit(1))
                            lane = oldest.lane if oldest else None
                            job = (claim(session, lane, f"benchmark-{worker_id}", 300,
                                         max_running=2) if lane else None)
                        elif mode == "protected":
                            job = claim(session, protected_lane, f"benchmark-{worker_id}",
                                        300, max_running=1)
                        else:
                            job = claim_for_worker(
                                session, protected_lane, f"benchmark-{worker_id}", 300,
                                query_workers=1, background_workers=1,
                                allow_borrow=(mode == "immediate" or
                                              protected_lane == "query" or
                                              (time.monotonic() - background_idle_since) * 1000
                                              >= borrow_grace_ms),
                            )
                        claimed = (job.id, job.lease_generation) if job else None
                    if claimed is None:
                        if len(finished) + len(errors) == expected_jobs:
                            return
                        time.sleep(0.005)
                        continue
                    outcome = platform.process(*claimed)
                    if protected_lane == "background" and job.lane == "background":
                        background_idle_since = time.monotonic()
                    with gate:
                        finished[claimed[0]] = outcome, time.monotonic()
                except Exception as exc:  # noqa: BLE001 - surface benchmark failures
                    with gate:
                        errors.append(f"{type(exc).__name__}: {exc}")
                    stop.set()

        memory_thread = Thread(target=sample_memory, daemon=True)
        workers = [Thread(target=worker, args=(index, lane), daemon=True)
                   for index, lane in enumerate(("query", "background"))]
        memory_thread.start()
        for thread in workers:
            thread.start()
        if late_features:
            time.sleep(late_delay_ms / 1000)
            for index, feature in enumerate(features[feature_count - late_features:]):
                if index:
                    time.sleep(late_spacing_ms / 1000)
                with gate:
                    job = platform.queue_refresh(feature.id)
                    submitted[job.id] = "background", time.monotonic()
                    late_ids.append(job.id)
        for thread in workers:
            thread.join(timeout=120)
        stop.set()
        memory_thread.join(timeout=1)
        if any(thread.is_alive() for thread in workers) or errors:
            raise RuntimeError(f"mixed workload did not finish: {errors}")
        if len(finished) != len(submitted) or any(outcome["status"] != "succeeded"
                                                  for outcome, _ in finished.values()):
            raise RuntimeError("mixed workload contains unfinished or failed tasks")

        query_latencies = [(finished[job_id][1] - queued) * 1000
                           for job_id, (lane, queued) in submitted.items() if lane == "query"]
        feature_latencies = [(finished[job_id][1] - queued) * 1000
                             for job_id, (lane, queued) in submitted.items() if lane == "background"]
        late_latencies = [(finished[job_id][1] - submitted[job_id][1]) * 1000
                          for job_id in late_ids]
        rows_by_job = [finished[job_id][0]["result"]["rows"]
                       for job_id, (lane, _queued) in submitted.items() if lane == "query"]
        digest = hashlib.sha256(json.dumps(rows_by_job, sort_keys=True).encode()).hexdigest()
        elapsed_seconds = time.monotonic() - started
        return {
            "mode": mode, "rows_per_dataset": rows, "queries": query_count,
            "features": feature_count,
            "late_features": late_features, "late_delay_ms": late_delay_ms,
            "late_spacing_ms": late_spacing_ms,
            "borrow_grace_ms": (borrow_grace_ms if mode == "dynamic" else
                                0 if mode == "immediate" else None),
            "workers": 2, "feature_deadline_ms": deadline_ms,
            "query_mean_ms": round(statistics.mean(query_latencies), 2),
            "query_p50_ms": round(statistics.median(query_latencies), 2),
            "query_p95_ms": round(percentile(query_latencies, 0.95), 2),
            "feature_completion_ms": [round(value, 2) for value in feature_latencies],
            "late_feature_completion_ms": [round(value, 2) for value in late_latencies],
            "feature_deadline_rate": round(sum(value <= deadline_ms for value in feature_latencies)
                                           / len(feature_latencies), 3),
            "throughput_jobs_per_second": round(len(submitted) / elapsed_seconds, 3),
            "peak_process_rss_mb": round(peak[0], 1),
            "result_digest": digest,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("fixed", "protected", "immediate", "dynamic"),
                        required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--features", type=int, default=2)
    parser.add_argument("--late-features", type=int, default=0)
    parser.add_argument("--late-delay-ms", type=int, default=0)
    parser.add_argument("--late-spacing-ms", type=int, default=0)
    parser.add_argument("--borrow-grace-ms", type=int, default=500)
    parser.add_argument("--deadline-ms", type=int, default=3000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.rows < 1000 or args.rows % 4 or args.queries < 1 or
            args.features < 1 or args.deadline_ms < 1 or
            args.late_features < 0 or args.late_features > args.features or
            args.late_delay_ms < 0 or args.late_spacing_ms < 0 or
            args.borrow_grace_ms < 0):
        parser.error("rows must be a multiple of four; task counts and deadline must be positive")
    report = measure(args.mode, args.rows, args.queries, args.features, args.deadline_ms,
                     args.late_features, args.late_delay_ms, args.late_spacing_ms,
                     args.borrow_grace_ms)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)
