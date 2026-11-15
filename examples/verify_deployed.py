"""Check a running Web/API deployment without exposing its access key."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


def api_key_from_file(path: Path) -> str:
    for line in path.read_text().splitlines():
        if line.startswith("FLUXPLANE_API_KEY="):
            return line.partition("=")[2].strip().strip('"\'')
    raise ValueError(f"FLUXPLANE_API_KEY is missing from {path}")


def verify(base_url: str, key: str, samples: int,
           expect_active_materialization: bool = False) -> dict:
    with httpx.Client(base_url=base_url, headers={"x-api-key": key}, timeout=10) as client:
        web = client.get("/")
        web.raise_for_status()
        ready = client.get("/health/ready")
        ready.raise_for_status()
        role = client.get("/api/v1/auth/me")
        role.raise_for_status()
        if role.json()["role"] != "admin":
            raise RuntimeError("the deployment key does not have the administrator role")

        counts = {}
        for name in ("sources", "datasets", "features", "releases"):
            response = client.get(f"/api/v1/{name}")
            response.raise_for_status()
            counts[name] = len(response.json())
        materializations = client.get("/api/v1/materializations?limit=30")
        materializations.raise_for_status()
        visible = materializations.json()
        current_active = sum(item["status"] == "active" and item["current"]
                             for item in visible)
        if expect_active_materialization and not current_active:
            raise RuntimeError("no current active materialization is visible")
        metrics = client.get("/api/v1/system/metrics")
        metrics.raise_for_status()
        timings = []
        for _ in range(samples):
            started = time.perf_counter()
            response = client.get("/api/v1/system/metrics")
            response.raise_for_status()
            timings.append((time.perf_counter() - started) * 1000)
        return {
            "checked_at_utc": datetime.now(UTC).isoformat(),
            "base_url": base_url,
            "web_status": web.status_code,
            "ready_status": ready.json()["status"],
            "role": role.json()["role"],
            "counts": counts,
            "materialization_view": {
                "records_returned": len(visible),
                "current_active": current_active,
                "first_status": visible[0]["status"] if visible else None,
            },
            "pools": metrics.json()["pools"],
            "control_request_samples": samples,
            "control_request_p50_ms": round(statistics.median(timings), 2),
            "control_request_p95_ms": round(sorted(timings)[int((samples - 1) * 0.95)], 2),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8088")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--expect-active-materialization", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.samples < 20:
        parser.error("at least 20 control requests are required")
    report = verify(args.base_url, api_key_from_file(args.env_file), args.samples,
                    args.expect_active_materialization)
    output = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output)
    print(output, end="")


if __name__ == "__main__":
    main()
