"""Measure query and feature-refresh overlap with separate Compose workers."""

from __future__ import annotations

import argparse
import hashlib
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


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def prepare(tag: str, rows: int, features: int) -> dict:
    import os

    import pyarrow as pa
    import pyarrow.parquet as pq
    from fluxplane.application.service import Platform
    from fluxplane.config import settings
    from fluxplane.scheduling.jobs import claim
    from fluxplane.storage.models import Job, Source
    from sqlalchemy import select

    platform = Platform(settings())
    platform.initialize()
    with platform.db.session() as session:
        busy = session.scalar(select(Job.id).where(
            Job.lane == "background", Job.status.in_(("pending", "running", "cancelling")),
        ).limit(1))
        source = session.scalar(select(Source).where(Source.name == "Mixed benchmark files"))
    if busy:
        raise RuntimeError("background tasks must finish before benchmark setup")
    if source is None:
        source = platform.add_source("Mixed benchmark files", "file", {})
    name = f"mix_{tag}"
    source_object = f"benchmarks/{name}.parquet"
    path = platform.settings.source_root / source_object
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    pq.write_table(pa.table({
        "row_id": pa.array(range(rows), type=pa.int64()),
        "region": pa.array(["east", "west", "north", "south"] * (rows // 4)),
        "amount": pa.array(((index * 1103515245 + 12345) % 100000
                            for index in range(rows)), type=pa.int64()),
    }), temporary, compression="zstd")
    os.replace(temporary, path)
    dataset = platform.add_dataset(source.id, name, source_object)
    queued = platform.queue_sync(dataset.id)
    with platform.db.session() as session:
        claimed = claim(session, "background", "benchmark-setup", 300)
        if claimed is None or claimed.id != queued.id:
            raise RuntimeError("benchmark source sync was not ready")
        generation = claimed.lease_generation
    synced = platform.process(queued.id, generation)
    if synced["status"] != "succeeded":
        raise RuntimeError(f"benchmark source sync failed: {synced['error']}")
    identifiers = []
    for index in range(features):
        aggregate = "SUM" if index % 2 == 0 else "AVG"
        sql = (f"SELECT row_id, {aggregate}(amount) OVER ("
               "PARTITION BY region ORDER BY row_id "
               "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value "
               f"FROM {name}")
        feature = platform.add_feature(f"feature_{name}_{index}", sql, [name], ["row_id"])
        identifiers.append(feature.id)
    return {"dataset": name, "rows": rows, "feature_ids": identifiers,
            "source_sync_result": synced["result"]}


def measure(client: httpx.Client, setup: dict, queries: int, deadline_ms: int,
            late_delay_ms: int, late_spacing_ms: int) -> dict:
    dataset = setup["dataset"]
    submitted: dict[str, tuple[str, int]] = {}

    def post(path: str, payload: dict | None, lane: str, index: int) -> None:
        response = client.post(path, json=payload or {})
        response.raise_for_status()
        submitted[response.json()["id"]] = lane, index

    for index in range(queries):
        sql = ("SELECT SUM(value) AS total FROM (SELECT "
               "SUM(amount) OVER (PARTITION BY region ORDER BY row_id "
               "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS value "
               f"FROM {dataset} WHERE row_id >= {index})")
        post("/api/v1/queries", {"sql": sql}, "query", index)
    time.sleep(late_delay_ms / 1000)
    for index, feature_id in enumerate(setup["feature_ids"]):
        if index:
            time.sleep(late_spacing_ms / 1000)
        post(f"/api/v1/features/{feature_id}/refresh", None, "background", index)

    deadline = time.monotonic() + 120
    finished: dict[str, dict] = {}
    peak_reserved_cpu = 0
    peak_reserved_memory = 0
    while len(finished) < len(submitted) and time.monotonic() < deadline:
        response = client.get("/api/v1/runs?limit=200")
        response.raise_for_status()
        for job in response.json():
            if job["id"] in submitted and job["status"] in {"succeeded", "failed", "cancelled"}:
                finished[job["id"]] = job
        metrics = client.get("/api/v1/system/metrics")
        metrics.raise_for_status()
        resources = metrics.json()["resources"]
        peak_reserved_cpu = max(peak_reserved_cpu, resources["cpu_reserved_millicores"])
        peak_reserved_memory = max(peak_reserved_memory, resources["memory_reserved_mb"])
        if len(finished) < len(submitted):
            time.sleep(0.1)
    if len(finished) != len(submitted):
        raise TimeoutError(f"{len(submitted) - len(finished)} benchmark tasks did not finish")
    if any(job["status"] != "succeeded" for job in finished.values()):
        failed = [(job_id, job["error"]) for job_id, job in finished.items()
                  if job["status"] != "succeeded"]
        raise RuntimeError(f"benchmark tasks failed: {failed}")

    timings: dict[str, list[float]] = {"query": [], "background": []}
    query_rows: dict[int, list[dict]] = {}
    job_records = []
    for job_id, (lane, index) in submitted.items():
        job = finished[job_id]
        created = datetime.fromisoformat(job["created_at"])
        completed = datetime.fromisoformat(job["updated_at"])
        latency = (completed - created).total_seconds() * 1000
        timings[lane].append(latency)
        if lane == "query":
            query_rows[index] = job["result"]["rows"]
        elif job["result"]["row_count"] != setup["rows"]:
            raise AssertionError("feature refresh returned the wrong row count")
        job_records.append({"id": job_id, "lane": lane, "index": index,
                            "latency_ms": round(latency, 2),
                            "worker_rss_mb": job["runtime"]["peak_rss_mb"],
                            "created_at": job["created_at"],
                            "updated_at": job["updated_at"]})
    all_created = [datetime.fromisoformat(job["created_at"]) for job in finished.values()]
    all_completed = [datetime.fromisoformat(job["updated_at"]) for job in finished.values()]
    elapsed = (max(all_completed) - min(all_created)).total_seconds()
    digest = hashlib.sha256(json.dumps([query_rows[index] for index in range(queries)],
                                    sort_keys=True).encode()).hexdigest()
    rss = [job["worker_rss_mb"] for job in job_records if job["worker_rss_mb"] is not None]
    return {"dataset": dataset, "rows": setup["rows"], "queries": queries,
            "features": len(setup["feature_ids"]), "deadline_ms": deadline_ms,
            "query_mean_ms": round(statistics.mean(timings["query"]), 2),
            "query_p50_ms": round(statistics.median(timings["query"]), 2),
            "query_p95_ms": round(percentile(timings["query"], 0.95), 2),
            "feature_completion_ms": [round(value, 2) for value in timings["background"]],
            "feature_deadline_rate": round(sum(value <= deadline_ms
                                               for value in timings["background"]) /
                                           len(timings["background"]), 3),
            "throughput_jobs_per_second": round(len(submitted) / elapsed, 3),
            "peak_reserved_cpu_millicores": peak_reserved_cpu,
            "peak_reserved_memory_mb": peak_reserved_memory,
            "peak_observed_worker_rss_mb": max(rss) if rss else None,
            "result_digest": digest, "jobs": job_records}


def run(args: argparse.Namespace) -> dict:
    compose = shlex.split(args.compose) + (["-p", args.project] if args.project else [])

    def call(*command: str, timeout: int = 60) -> str:
        result = subprocess.run(compose + list(command), check=True, capture_output=True,
                                text=True, timeout=timeout)
        return result.stdout.strip()

    required = {"api", "scheduler", "worker-background", "worker-query"}
    running = set(call("ps", "--status", "running", "--services").splitlines())
    if not required.issubset(running):
        raise RuntimeError(f"start the Compose deployment first: {sorted(required - running)}")
    started_at = datetime.now(UTC).isoformat()
    key = api_key_from_file(args.env_file)
    runs = []
    worker_container = None
    call("stop", "scheduler", "worker-background", timeout=45)
    try:
        with httpx.Client(base_url=args.base_url, headers={"x-api-key": key},
                          timeout=20) as client:
            client.get("/health/ready").raise_for_status()
            for round_index in range(args.rounds):
                modes = ("fixed", "dynamic") if round_index % 2 == 0 else ("dynamic", "fixed")
                for mode in modes:
                    tag = f"{args.run_id}_{round_index}_{mode}"
                    raw = call("exec", "-T", "api", "python",
                               "examples/benchmark_compose_mixed.py", "--inside-prepare",
                               "--tag", tag, "--rows", str(args.rows),
                               "--features", str(args.features), timeout=120)
                    setup = json.loads(raw)
                    grace = 86_400_000 if mode == "fixed" else args.borrow_grace_ms
                    worker_container = call("run", "-d", "--rm", "--no-deps", "-e",
                                            f"FLUXPLANE_BACKGROUND_BORROW_GRACE_MS={grace}",
                                            "worker-background", timeout=45).splitlines()[-1]
                    time.sleep(1)
                    result = measure(client, setup, args.queries, args.deadline_ms,
                                     args.late_delay_ms, args.late_spacing_ms)
                    result.update({"round": round_index + 1, "mode": mode,
                                   "borrow_grace_ms": grace})
                    runs.append(result)
                    subprocess.run(["docker", "rm", "-f", worker_container],
                                   check=True, capture_output=True, text=True, timeout=30)
                    worker_container = None
    finally:
        if worker_container is not None:
            subprocess.run(["docker", "rm", "-f", worker_container],
                           check=True, capture_output=True, text=True, timeout=30)
        call("start", "worker-background", timeout=45)
        call("start", "scheduler", timeout=45)
    if len({item["result_digest"] for item in runs}) != 1:
        raise AssertionError("query results differ between scheduling modes")
    summary = {mode: {
        "query_mean_ms": round(statistics.median(item["query_mean_ms"] for item in runs
                                                 if item["mode"] == mode), 2),
        "query_p95_ms": round(statistics.median(item["query_p95_ms"] for item in runs
                                               if item["mode"] == mode), 2),
        "feature_deadline_rate": round(statistics.mean(item["feature_deadline_rate"]
                                                      for item in runs if item["mode"] == mode), 3),
        "throughput_jobs_per_second": round(statistics.median(
            item["throughput_jobs_per_second"] for item in runs if item["mode"] == mode), 3),
    } for mode in ("fixed", "dynamic")}
    return {"checked_at_utc": started_at, "deployment": "isolated Compose with PostgreSQL",
            "rows_per_run": args.rows, "queries_per_run": args.queries,
            "features_per_run": args.features, "rounds": args.rounds,
            "late_delay_ms": args.late_delay_ms,
            "late_spacing_ms": args.late_spacing_ms,
            "feature_deadline_ms": args.deadline_ms,
            "summary": summary, "runs": runs}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inside-prepare", action="store_true")
    parser.add_argument("--tag")
    parser.add_argument("--compose", default="docker compose")
    parser.add_argument("--project")
    parser.add_argument("--base-url", default="http://127.0.0.1:8088")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--features", type=int, default=6)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--late-delay-ms", type=int, default=50)
    parser.add_argument("--late-spacing-ms", type=int, default=250)
    parser.add_argument("--borrow-grace-ms", type=int, default=500)
    parser.add_argument("--deadline-ms", type=int, default=2000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.rows < 1000 or args.rows % 4 or args.queries < 1 or args.features < 1 or
            args.rounds < 1 or min(args.late_delay_ms, args.late_spacing_ms,
                                   args.borrow_grace_ms) < 0):
        parser.error("invalid benchmark size or timing")
    if args.inside_prepare:
        if not args.tag:
            parser.error("--tag is required for setup")
        report = prepare(args.tag, args.rows, args.features)
    else:
        args.run_id = uuid4().hex[:8]
        report = run(args)
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()
