"""Development and service entrypoints."""

from __future__ import annotations

import argparse
import json
import time
from threading import Event, Thread
from uuid import uuid4

from fluxplane.application.service import Platform
from fluxplane.config import settings
from fluxplane.scheduling.jobs import claim_for_worker, heartbeat


def run_with_heartbeat(platform: Platform, job_id: str, generation: int) -> dict:
    stopped = Event()

    def renew() -> None:
        interval = max(2, platform.settings.worker_lease_seconds // 3)
        while not stopped.wait(interval):
            try:
                with platform.db.session() as session:
                    if not heartbeat(session, job_id, generation,
                                     platform.settings.worker_lease_seconds):
                        return
            except Exception as exc:  # noqa: BLE001 - renewal must not kill the worker
                print(f"lease renewal failed for {job_id}: {exc}", flush=True)

    thread = Thread(target=renew, name=f"lease-{job_id[:8]}", daemon=True)
    thread.start()
    try:
        return platform.process(job_id, generation)
    finally:
        stopped.set()
        thread.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser(prog="fluxplane")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("migrate")
    sub.add_parser("verify-storage")
    garbage = sub.add_parser("gc")
    garbage.add_argument("--apply", action="store_true")
    garbage.add_argument("--min-age-seconds", type=int, default=3600)
    scanner = sub.add_parser("scheduler")
    scanner.add_argument("--once", action="store_true")
    scanner.add_argument("--interval", type=float, default=2.0)
    server = sub.add_parser("api")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8040)
    worker = sub.add_parser("worker")
    worker.add_argument("--lane", choices=["query", "background"], required=True)
    worker.add_argument("--once", action="store_true")
    args = parser.parse_args()
    config = settings()
    platform = Platform(config)
    platform.initialize()
    if args.command == "init":
        print("FluxPlane metadata initialized")
        return
    if args.command == "migrate":
        print("FluxPlane metadata migrations are current")
        return
    if args.command == "verify-storage":
        report = platform.verify_storage()
        print(json.dumps(report, ensure_ascii=False))
        if not report["ok"]:
            raise SystemExit(1)
        return
    if args.command == "gc":
        print(json.dumps(platform.storage_summary(
            collect=args.apply, min_age_seconds=args.min_age_seconds
        ), ensure_ascii=False))
        return
    if args.command == "scheduler":
        if args.interval < 0.5:
            parser.error("scheduler interval must be at least 0.5 seconds")
        seen: dict[str, tuple[int, int, str]] = {}
        last_cache_maintenance = 0.0
        while True:
            seen, scheduled = platform.poll_file_sources(seen)
            scheduled.extend(platform.poll_remote_sources())
            scheduled.extend(platform.poll_time_features())
            if time.monotonic() - last_cache_maintenance >= 60:
                scheduled.extend(platform.poll_layout_candidates())
                retired = platform.maintain_materializations()
                last_cache_maintenance = time.monotonic()
                if retired["retired"]:
                    print(f"retired {retired['retired']} materializations; "
                          f"reclaimed {retired['reclaimed_bytes']} bytes; "
                          f"unlink failures {retired['unlink_failures']}", flush=True)
            for job_id in scheduled:
                print(f"scheduled synchronization {job_id}", flush=True)
            if args.once and seen:
                # A second observation is needed to confirm a stable file.
                if not scheduled:
                    time.sleep(args.interval)
                    _, scheduled = platform.poll_file_sources(seen)
                    scheduled.extend(platform.poll_remote_sources())
                    scheduled.extend(platform.poll_time_features())
                    for job_id in scheduled:
                        print(f"scheduled synchronization {job_id}", flush=True)
                return
            if args.once:
                return
            time.sleep(args.interval)
    if args.command == "api":
        import uvicorn

        uvicorn.run("fluxplane.api.app:app", host=args.host, port=args.port)
        return
    worker_id = f"{args.lane}-{uuid4().hex[:8]}"
    background_idle_since = time.monotonic()
    while True:
        with platform.db.session() as session:
            job = claim_for_worker(
                session, args.lane, worker_id, config.worker_lease_seconds,
                query_workers=config.query_workers,
                background_workers=config.background_workers,
                cpu_budget=config.resource_cpu_millicores,
                memory_budget=config.resource_memory_mb,
                allow_borrow=(args.once or args.lane == "query" or
                              (time.monotonic() - background_idle_since) * 1000 >=
                              config.background_borrow_grace_ms),
            )
            work = (job.id, job.lease_generation) if job else None
        if work:
            print(f"{worker_id} processing {work[0]}", flush=True)
            print(run_with_heartbeat(platform, *work), flush=True)
            if args.lane == "background" and job.lane == "background":
                background_idle_since = time.monotonic()
        if args.once:
            return
        if not work:
            time.sleep(0.3)


if __name__ == "__main__":
    main()

