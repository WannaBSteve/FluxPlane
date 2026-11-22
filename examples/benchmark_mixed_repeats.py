"""Repeat the mixed-arrival comparison in rotated mode order."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from benchmark_mixed import measure


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=20)
    parser.add_argument("--features", type=int, default=2)
    parser.add_argument("--late-features", type=int, default=2)
    parser.add_argument("--late-delay-ms", type=int, default=50)
    parser.add_argument("--late-spacing-ms", type=int, default=0)
    parser.add_argument("--borrow-grace-ms", type=int, default=500)
    parser.add_argument("--deadline-ms", type=int, default=2000)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.rows < 1000 or args.rows % 4 or args.queries < 1 or args.features < 1 or
            not 0 <= args.late_features <= args.features or args.late_delay_ms < 0 or
            args.late_spacing_ms < 0 or args.borrow_grace_ms < 0 or
            args.deadline_ms < 1 or args.rounds < 1):
        parser.error("invalid workload size, arrival schedule, deadline, or round count")

    modes = ("fixed", "protected", "immediate", "dynamic")
    results = []
    for round_number in range(args.rounds):
        offset = (-round_number) % len(modes)
        for mode in modes[offset:] + modes[:offset]:
            result = measure(mode, args.rows, args.queries, args.features,
                             args.deadline_ms, args.late_features, args.late_delay_ms,
                             args.late_spacing_ms, args.borrow_grace_ms)
            result["round"] = round_number + 1
            results.append(result)
            print(f"round {round_number + 1}: {mode} complete", flush=True)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, ensure_ascii=False, indent=2) + "\n")
    summary = {}
    for mode in modes:
        runs = [result for result in results if result["mode"] == mode]
        summary[mode] = {
            "query_mean_median_ms": round(statistics.median(
                result["query_mean_ms"] for result in runs), 2),
            "query_p95_median_ms": round(statistics.median(
                result["query_p95_ms"] for result in runs), 2),
            "late_feature_deadline_hits": sum(
                sum(value <= args.deadline_ms for value in result["late_feature_completion_ms"])
                for result in runs),
            "late_feature_total": args.late_features * args.rounds,
            "throughput_median_jobs_per_second": round(statistics.median(
                result["throughput_jobs_per_second"] for result in runs), 3),
        }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
