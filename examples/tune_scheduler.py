"""Tune mixed-workload scheduling with constrained Nash policy selection."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

from benchmark_mixed import measure
from fluxplane.scheduling.optimizer import PolicyMeasurement, select_policy


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=25_000)
    parser.add_argument("--queries", type=int, default=12)
    parser.add_argument("--features", type=int, default=4)
    parser.add_argument("--late-features", type=int, default=4)
    parser.add_argument("--late-delay-ms", type=int, default=50)
    parser.add_argument("--late-spacing-ms", type=int, default=250)
    parser.add_argument("--deadline-ms", type=int, default=2_000)
    parser.add_argument("--grace-ms", type=int, nargs="+", default=[100, 250, 500, 1_000])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--minimum-deadline-rate", type=float, default=0.99)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.rows < 1_000 or args.rows % 4 or min(
        args.queries, args.features, args.deadline_ms, args.rounds
    ) < 1 or not 0 <= args.late_features <= args.features or
            min(args.grace_ms) < 0 or not 0 <= args.minimum_deadline_rate <= 1):
        parser.error("invalid workload, policy, or constraint")

    policies = [("shared_fifo", "fixed", 0), ("fixed_pools", "protected", 0),
                ("borrow_immediate", "immediate", 0)]
    policies.extend((f"borrow_{grace}ms", "dynamic", grace) for grace in args.grace_ms)
    runs = []
    for round_number in range(1, args.rounds + 1):
        offset = (round_number - 1) % len(policies)
        rotated = policies[offset:] + policies[:offset]
        for name, mode, grace in rotated:
            result = measure(
                mode,
                args.rows,
                args.queries,
                args.features,
                args.deadline_ms,
                args.late_features,
                args.late_delay_ms,
                args.late_spacing_ms,
                grace,
            )
            result.update({"policy": name, "round": round_number})
            runs.append(result)
            print(f"round {round_number}: {name} complete", flush=True)

    measurements = []
    for name, _mode, _grace in policies:
        policy_runs = [run for run in runs if run["policy"] == name]
        measurements.append(PolicyMeasurement(
            name=name,
            query_p95_ms=statistics.median(run["query_p95_ms"] for run in policy_runs),
            feature_deadline_rate=statistics.median(
                run["feature_deadline_rate"] for run in policy_runs
            ),
            throughput_jobs_per_second=statistics.median(
                run["throughput_jobs_per_second"] for run in policy_runs
            ),
            details={"rounds": len(policy_runs)},
        ))
    selected, scores = select_policy(
        measurements, minimum_deadline_rate=args.minimum_deadline_rate
    )
    report = {
        "schema_version": 1,
        "algorithm": "constrained_weighted_nash_social_welfare",
        "objective_weights": {"query_latency": 2, "feature_freshness": 2, "throughput": 1},
        "minimum_feature_deadline_rate": args.minimum_deadline_rate,
        "selected_policy": selected,
        "scores": scores,
        "summary": {
            item.name: {
                "query_mean_ms": round(statistics.median(
                    run["query_mean_ms"] for run in runs if run["policy"] == item.name
                ), 3),
                "query_p95_ms": round(item.query_p95_ms, 3),
                "feature_deadline_rate": round(item.feature_deadline_rate, 6),
                "throughput_jobs_per_second": round(item.throughput_jobs_per_second, 6),
            } for item in measurements
        },
        "workload": {
            "rows": args.rows,
            "queries": args.queries,
            "features": args.features,
            "late_features": args.late_features,
            "deadline_ms": args.deadline_ms,
            "rounds": args.rounds,
        },
        "runs": runs,
    }
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
