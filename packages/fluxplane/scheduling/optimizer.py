"""Explainable policy selection for mixed query/background workloads."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class PolicyMeasurement:
    name: str
    query_p95_ms: float
    feature_deadline_rate: float
    throughput_jobs_per_second: float
    details: dict


def nash_scores(measurements: list[PolicyMeasurement], *,
                minimum_deadline_rate: float = 0.99,
                query_weight: float = 2.0,
                freshness_weight: float = 2.0,
                throughput_weight: float = 1.0) -> dict[str, float]:
    """Score feasible policies with weighted Nash social welfare.

    Query latency, feature freshness, and throughput act as competing utilities.
    A hard freshness floor prevents a fast policy from winning by starving updates.
    The product form makes a near-zero utility impossible to hide behind another
    lane's large gain, while the explicit weights remain auditable.
    """
    if not measurements:
        raise ValueError("at least one policy measurement is required")
    if (not 0 <= minimum_deadline_rate <= 1 or
            min(query_weight, freshness_weight, throughput_weight) <= 0):
        raise ValueError("invalid deadline floor or utility weight")
    if any(min(item.query_p95_ms, item.throughput_jobs_per_second) <= 0 or
           not 0 <= item.feature_deadline_rate <= 1 for item in measurements):
        raise ValueError("policy metrics are outside their valid range")
    best_latency = min(item.query_p95_ms for item in measurements)
    best_throughput = max(item.throughput_jobs_per_second for item in measurements)
    result = {}
    for item in measurements:
        if item.feature_deadline_rate < minimum_deadline_rate:
            result[item.name] = 0.0
            continue
        utilities = (
            (best_latency / item.query_p95_ms, query_weight),
            (item.feature_deadline_rate, freshness_weight),
            (item.throughput_jobs_per_second / best_throughput, throughput_weight),
        )
        result[item.name] = round(math.exp(sum(
            weight * math.log(max(utility, 1e-12)) for utility, weight in utilities
        )), 9)
    return result


def select_policy(measurements: list[PolicyMeasurement], **kwargs) -> tuple[str, dict[str, float]]:
    scores = nash_scores(measurements, **kwargs)
    if not any(scores.values()):
        raise ValueError("no policy satisfies the feature freshness constraint")
    return max(scores, key=lambda name: (scores[name], name)), scores
