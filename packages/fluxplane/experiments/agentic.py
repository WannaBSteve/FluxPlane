"""Evidence arbitration and selective large-model routing."""

from __future__ import annotations

import math
from dataclasses import dataclass


def _normalize(values: tuple[float, ...], epsilon: float = 1e-12) -> tuple[float, ...]:
    if not values or any(value < 0 or not math.isfinite(value) for value in values):
        raise ValueError("probabilities must be finite and non-negative")
    total = sum(values)
    if total <= 0:
        raise ValueError("probability distribution must have positive mass")
    result = tuple(max(value / total, epsilon) for value in values)
    normalized = sum(result)
    return tuple(value / normalized for value in result)


def normalized_entropy(probabilities: tuple[float, ...]) -> float:
    values = _normalize(probabilities)
    if len(values) == 1:
        return 0.0
    return -sum(value * math.log(value) for value in values) / math.log(len(values))


def jensen_shannon(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    if len(left) != len(right):
        raise ValueError("distributions must have equal length")
    a, b = _normalize(left), _normalize(right)
    middle = tuple((x + y) / 2 for x, y in zip(a, b))

    def divergence(source: tuple[float, ...]) -> float:
        return sum(x * math.log(x / y) for x, y in zip(source, middle))

    return (divergence(a) + divergence(b)) / (2 * math.log(2))


@dataclass(frozen=True)
class EvidenceOpinion:
    agent: str
    probabilities: tuple[float, ...]
    reliability: float = 1.0
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _normalize(self.probabilities)
        if not 0 < self.reliability <= 1:
            raise ValueError("agent reliability must be in (0, 1]")


@dataclass(frozen=True)
class Arbitration:
    probabilities: tuple[float, ...]
    confidence: float
    uncertainty: float
    disagreement: float
    evidence: tuple[str, ...]


class ConflictAwareArbiter:
    """Combine independent agent opinions with a reliability-weighted log pool."""

    def combine(self, opinions: tuple[EvidenceOpinion, ...]) -> Arbitration:
        if not opinions:
            raise ValueError("at least one opinion is required")
        width = len(opinions[0].probabilities)
        if any(len(item.probabilities) != width for item in opinions):
            raise ValueError("agent opinions must share the same label space")
        normalized = [_normalize(item.probabilities) for item in opinions]
        weight_total = sum(item.reliability for item in opinions)
        logits = [
            sum(
                item.reliability * math.log(distribution[index])
                for item, distribution in zip(opinions, normalized)
            ) / weight_total
            for index in range(width)
        ]
        offset = max(logits)
        probabilities = _normalize(tuple(math.exp(value - offset) for value in logits))
        pairwise = [
            jensen_shannon(normalized[left], normalized[right])
            for left in range(len(normalized))
            for right in range(left + 1, len(normalized))
        ]
        disagreement = sum(pairwise) / len(pairwise) if pairwise else 0.0
        evidence = tuple(
            item for opinion in opinions for item in opinion.evidence
        )
        return Arbitration(
            probabilities=probabilities,
            confidence=max(probabilities),
            uncertainty=normalized_entropy(probabilities),
            disagreement=disagreement,
            evidence=evidence,
        )


def classwise_linear_pool(
    opinions: tuple[tuple[float, ...], ...],
    reliabilities: tuple[tuple[float, ...], ...],
) -> tuple[float, ...]:
    """Pool agents with a separate reliability vector for every target class.

    A text model, case memory and field rules can be good at different outcomes.
    One global ensemble weight erases that distinction, so this pool makes the
    learned-on-validation reliability matrix explicit and inspectable.
    """
    if not opinions or len(opinions) != len(reliabilities):
        raise ValueError("opinions and reliabilities must have the same non-zero length")
    width = len(opinions[0])
    if any(len(item) != width for item in (*opinions, *reliabilities)):
        raise ValueError("all opinions and reliabilities must share one label space")
    normalized = tuple(_normalize(item) for item in opinions)
    scores = []
    for class_index in range(width):
        weights = tuple(item[class_index] for item in reliabilities)
        if any(not math.isfinite(value) or value < 0 for value in weights):
            raise ValueError("class reliabilities must be finite and non-negative")
        total = sum(weights)
        if total <= 0:
            raise ValueError("every class must have positive reliability mass")
        scores.append(sum(
            weight * distribution[class_index]
            for weight, distribution in zip(weights, normalized)
        ) / total)
    return _normalize(tuple(scores))


@dataclass(frozen=True)
class RoutingSignal:
    identity: str
    confidence: float
    disagreement: float
    retrieval_support: float
    shifted: bool = False

    def __post_init__(self) -> None:
        for name, value in (
            ("confidence", self.confidence),
            ("disagreement", self.disagreement),
            ("retrieval_support", self.retrieval_support),
        ):
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be in [0, 1]")


@dataclass(frozen=True)
class RoutingDecision:
    identity: str
    route_to_large_model: bool
    priority: float
    reasons: tuple[str, ...]


class BudgetedUtilityRouter:
    """Rank cases by expected value of additional semantic reasoning.

    The router is deliberately training-free.  Its four signals are inspectable,
    can be ablated independently, and can later be replaced by a learned utility
    model without changing the experiment contract.
    """

    def __init__(
        self,
        *,
        confidence_weight: float = 0.4,
        disagreement_weight: float = 0.3,
        retrieval_weight: float = 0.2,
        shift_weight: float = 0.1,
    ) -> None:
        weights = (
            confidence_weight,
            disagreement_weight,
            retrieval_weight,
            shift_weight,
        )
        if any(value < 0 for value in weights) or sum(weights) <= 0:
            raise ValueError("routing weights must be non-negative with positive sum")
        total = sum(weights)
        self.weights = tuple(value / total for value in weights)

    def priority(self, signal: RoutingSignal) -> float:
        confidence, disagreement, retrieval, shift = self.weights
        return (
            confidence * (1 - signal.confidence)
            + disagreement * signal.disagreement
            + retrieval * (1 - signal.retrieval_support)
            + shift * float(signal.shifted)
        )

    def select(
        self, signals: list[RoutingSignal], *, budget_rate: float
    ) -> list[RoutingDecision]:
        if not 0 <= budget_rate <= 1:
            raise ValueError("budget rate must be in [0, 1]")
        budget = math.floor(len(signals) * budget_rate)
        ranked = sorted(
            ((self.priority(item), item) for item in signals),
            key=lambda pair: (-pair[0], pair[1].identity),
        )
        selected = {item.identity for _, item in ranked[:budget]}
        decisions = []
        for priority, item in ranked:
            reasons = []
            if item.confidence < 0.6:
                reasons.append("low_confidence")
            if item.disagreement > 0.2:
                reasons.append("agent_disagreement")
            if item.retrieval_support < 0.5:
                reasons.append("weak_case_support")
            if item.shifted:
                reasons.append("distribution_shift")
            decisions.append(RoutingDecision(
                identity=item.identity,
                route_to_large_model=item.identity in selected,
                priority=priority,
                reasons=tuple(reasons),
            ))
        return decisions
