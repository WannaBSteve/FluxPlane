"""Stop a real background worker mid-sync and verify fenced recovery.

Run from the Compose project directory after ``full_demo.py --phase seed``.
The host mode manages Compose; ``--inside`` commands run in the API container.
"""

from __future__ import annotations

import argparse
import json
import os
import select
import shlex
import subprocess
import sys
import time
from datetime import UTC, datetime

DATASET_NAME = "demo_recovery"
SOURCE_OBJECT = "full_demo/recovery.parquet"


def inside(action: str, job_id: str | None, baseline: dict | None) -> dict | None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from fluxplane.application.service import Platform
    from fluxplane.config import settings
    from fluxplane.storage.models import (
        CatalogState,
        Dataset,
        DatasetVersion,
        Job,
        JobAttempt,
        Release,
        Source,
    )
    from sqlalchemy import func
    from sqlalchemy import select as sql_select

    platform = Platform(settings())
    platform.initialize()
    if action == "hold-lock":
        with platform.db.session() as session:
            dataset = session.scalar(sql_select(Dataset).where(
                Dataset.name == DATASET_NAME
            ).with_for_update())
            if dataset is None:
                raise RuntimeError("recovery dataset is missing")
            print("LOCKED", flush=True)
            sys.stdin.readline()
        return None
    if action == "prepare":
        with platform.db.session() as session:
            active = session.scalar(sql_select(Job.id).where(
                Job.lane == "background",
                Job.status.in_(("pending", "running", "cancelling")),
            ).limit(1))
            source = session.scalar(sql_select(Source).where(Source.name == "Parquet products"))
        if active:
            raise RuntimeError("wait for background tasks to finish before this demo")
        if source is None:
            raise RuntimeError("run full_demo.py --phase seed first")
        with platform.db.session() as session:
            dataset = session.scalar(sql_select(Dataset).where(Dataset.name == DATASET_NAME))
        if dataset is None:
            dataset = platform.add_dataset(source.id, DATASET_NAME, SOURCE_OBJECT)
        elif dataset.source_id != source.id or dataset.source_object != SOURCE_OBJECT:
            raise RuntimeError("existing recovery dataset has a different definition")
        value = datetime.now(UTC).isoformat()
        path = platform.settings.source_root / SOURCE_OBJECT
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".partial")
        pq.write_table(pa.table({"value": [value]}), temporary)
        os.replace(temporary, path)
        with platform.db.session() as session:
            return {
                "dataset_id": dataset.id,
                "value": value,
                "release_id": session.get(CatalogState, 1).active_release_id,
                "release_count": session.scalar(sql_select(func.count(Release.id))),
                "version_count": session.scalar(sql_select(func.count(DatasetVersion.id)).where(
                    DatasetVersion.dataset_id == dataset.id
                )),
            }
    if action == "submit":
        with platform.db.session() as session:
            dataset = session.scalar(sql_select(Dataset).where(Dataset.name == DATASET_NAME))
        if dataset is None:
            raise RuntimeError("recovery dataset is missing")
        return {"job_id": platform.queue_sync(dataset.id, force_full=True).id}
    if job_id is None:
        raise ValueError("job ID is required")
    if action in {"wait-running", "verify"}:
        deadline = time.monotonic() + (35 if action == "wait-running" else 90)
        while time.monotonic() < deadline:
            with platform.db.session() as session:
                job = session.get(Job, job_id)
                if job is None:
                    raise RuntimeError("recovery task is missing")
                status, attempt, worker_id, error = (
                    job.status, job.attempt, job.worker_id, job.error
                )
            if action == "wait-running" and status == "running":
                if not worker_id or not worker_id.startswith("background-"):
                    raise AssertionError(f"task was not claimed by a background worker: {worker_id}")
                return {"job_id": job_id, "attempt": attempt, "worker_id": worker_id}
            if action == "verify" and status in {"succeeded", "failed", "cancelled"}:
                if status != "succeeded":
                    raise AssertionError(f"recovered task ended as {status}: {error}")
                break
            time.sleep(0.2)
        else:
            raise TimeoutError(f"task did not reach the expected state: {action}")
    if action != "verify" or baseline is None:
        raise ValueError("unknown action or missing baseline")
    with platform.db.session() as session:
        job = session.get(Job, job_id)
        dataset = session.get(Dataset, baseline["dataset_id"])
        state = session.get(CatalogState, 1)
        release = session.get(Release, state.active_release_id)
        attempts = session.scalars(sql_select(JobAttempt).where(
            JobAttempt.job_id == job_id
        ).order_by(JobAttempt.number)).all()
        version_count = session.scalar(sql_select(func.count(DatasetVersion.id)).where(
            DatasetVersion.dataset_id == dataset.id
        ))
        release_count = session.scalar(sql_select(func.count(Release.id)))
        result = {
            "job_id": job_id,
            "attempts": [item.status for item in attempts],
            "version_id": dataset.active_version_id,
            "release_id": release.id,
            "release_parent_id": release.parent_id,
            "release_reason": release.reason,
            "version_count_delta": version_count - baseline["version_count"],
            "release_count_delta": release_count - baseline["release_count"],
            "source_value": baseline["value"],
        }
        if job.attempt != 2 or result["attempts"] != ["expired", "succeeded"]:
            raise AssertionError(f"expected one expired attempt and one success: {result}")
        if (result["version_count_delta"] != 1 or result["release_count_delta"] != 1 or
                result["release_parent_id"] != baseline["release_id"] or
                result["release_reason"] != f"dataset:{DATASET_NAME}" or
                job.result["version_id"] != dataset.active_version_id):
            raise AssertionError(f"publication was not exactly once: {result}")
    query = platform.queue_query(f"SELECT value FROM {DATASET_NAME}")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with platform.db.session() as session:
            finished = session.get(Job, query.id)
            if finished.status in {"succeeded", "failed", "cancelled"}:
                if finished.status != "succeeded":
                    raise AssertionError(f"query after recovery failed: {finished.error}")
                rows = finished.result["rows"]
                if rows != [{"value": baseline["value"]}]:
                    raise AssertionError(f"recovered query returned {rows}")
                result["query_rows"] = rows
                return result
        time.sleep(0.2)
    raise TimeoutError("query after recovery did not finish")


def run_host(compose: list[str], project: str | None) -> dict:
    base = compose + (["-p", project] if project else [])

    def call(*args: str, timeout: int = 60) -> str:
        result = subprocess.run(base + list(args), check=True, capture_output=True,
                                text=True, timeout=timeout)
        return result.stdout.strip()

    def api(action: str, *args: str, timeout: int = 60) -> dict:
        output = call("exec", "-T", "api", "python", "examples/worker_recovery_demo.py",
                      "--inside", action, *args, timeout=timeout)
        return json.loads(output.splitlines()[-1])

    services = set(call("ps", "--status", "running", "--services").splitlines())
    required = {"api", "scheduler", "worker-background", "worker-query"}
    if not required.issubset(services):
        raise RuntimeError(f"start the demo services first; missing: {sorted(required - services)}")
    call("stop", "scheduler", timeout=40)
    lock: subprocess.Popen[str] | None = None
    try:
        baseline = api("prepare")
        lock = subprocess.Popen(
            base + ["exec", "-T", "api", "python", "examples/worker_recovery_demo.py",
                    "--inside", "hold-lock"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        ready, _, _ = select.select([lock.stdout], [], [], 30)
        if not ready or lock.stdout.readline().strip() != "LOCKED":
            raise RuntimeError("could not lock the recovery dataset")
        submitted = api("submit")
        running = api("wait-running", "--job-id", submitted["job_id"], timeout=45)
        call("stop", "worker-background", timeout=40)
        lock.stdin.write("\n")
        lock.stdin.flush()
        lock.communicate(timeout=15)
        lock = None
        call("start", "worker-background", timeout=40)
        result = api("verify", "--job-id", submitted["job_id"],
                     "--baseline", json.dumps(baseline), timeout=110)
        result["stopped_worker_id"] = running["worker_id"]
        return result
    finally:
        if lock is not None:
            if lock.poll() is None:
                lock.stdin.write("\n")
                lock.stdin.flush()
            try:
                lock.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                lock.kill()
                lock.communicate()
        call("start", "worker-background", timeout=40)
        call("start", "scheduler", timeout=40)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compose", default="docker compose",
                        help="Compose command; a standalone binary path also works")
    parser.add_argument("--project", help="Compose project name")
    parser.add_argument("--inside", choices=["prepare", "hold-lock", "submit",
                                            "wait-running", "verify"])
    parser.add_argument("--job-id")
    parser.add_argument("--baseline")
    args = parser.parse_args()
    if args.inside:
        result = inside(args.inside, args.job_id,
                        json.loads(args.baseline) if args.baseline else None)
    else:
        result = run_host(shlex.split(args.compose), args.project)
    if result is not None:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
