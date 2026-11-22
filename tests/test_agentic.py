"""Checks for inspectable agent arbitration and budget routing."""

import pytest
from fluxplane.experiments.agentic import (
    BudgetedUtilityRouter,
    ConflictAwareArbiter,
    EvidenceOpinion,
    RoutingSignal,
    classwise_linear_pool,
    jensen_shannon,
    normalized_entropy,
)


def test_classwise_pool_can_assign_different_experts_to_each_class() -> None:
    pooled = classwise_linear_pool(
        ((0.8, 0.2), (0.3, 0.7)),
        ((1.0, 0.0), (0.0, 1.0)),
    )
    assert pooled == pytest.approx((0.8 / 1.5, 0.7 / 1.5))


def test_arbiter_rewards_agreement_and_preserves_evidence() -> None:
    arbiter = ConflictAwareArbiter()
    decision = arbiter.combine((
        EvidenceOpinion("text", (0.8, 0.2), evidence=("narrative",)),
        EvidenceOpinion("structured", (0.7, 0.3), evidence=("product",)),
    ))
    assert decision.probabilities[0] > 0.7
    assert decision.confidence == max(decision.probabilities)
    assert decision.evidence == ("narrative", "product")
    assert 0 <= decision.disagreement < 0.1


def test_distribution_diagnostics_are_bounded() -> None:
    assert normalized_entropy((1.0, 0.0)) < 0.01
    assert normalized_entropy((0.5, 0.5)) == pytest.approx(1.0)
    assert jensen_shannon((0.9, 0.1), (0.1, 0.9)) > 0.5


def test_router_respects_batch_budget_and_prioritizes_conflict() -> None:
    router = BudgetedUtilityRouter()
    signals = [
        RoutingSignal("easy", 0.95, 0.01, 0.9),
        RoutingSignal("conflict", 0.55, 0.8, 0.2),
        RoutingSignal("shift", 0.7, 0.2, 0.4, shifted=True),
        RoutingSignal("supported", 0.8, 0.1, 0.95),
    ]
    decisions = router.select(signals, budget_rate=0.25)
    selected = [item for item in decisions if item.route_to_large_model]
    assert [item.identity for item in selected] == ["conflict"]
    assert "agent_disagreement" in selected[0].reasons
