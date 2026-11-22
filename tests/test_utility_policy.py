from fluxplane.experiments.utility import (
    CorrectionSignal,
    EmpiricalCorrectionPolicy,
    SupervisedCorrectionPolicy,
)


def _signal(identity: str, *, candidate: str | None = None) -> CorrectionSignal:
    return CorrectionSignal(
        identity=identity,
        local_label="local-a",
        local_confidence=0.6,
        disagreement=0.25,
        retrieval_support=0.4,
        candidate_label=candidate,
        candidate_confidence=0.8 if candidate else None,
    )


def test_pre_call_policy_selects_only_positive_utility_groups() -> None:
    policy = EmpiricalCorrectionPolicy(post_adjudication=False, prior_strength=1).fit(
        [_signal("a"), _signal("b"), _signal("c")],
        [1.0, 1.0, -0.25],
    )
    decisions = policy.select([_signal("x"), _signal("y")], budget_rate=0.5)
    assert sum(item.selected for item in decisions) == 1
    assert decisions[0].expected_utility > 0


def test_post_call_policy_distinguishes_candidate_transitions() -> None:
    signals = [
        _signal("a", candidate="candidate-good"),
        _signal("b", candidate="candidate-good"),
        _signal("c", candidate="candidate-bad"),
        _signal("d", candidate="candidate-bad"),
    ]
    policy = EmpiricalCorrectionPolicy(post_adjudication=True, prior_strength=0.1).fit(
        signals, [1.0, 1.0, -1.0, -1.0]
    )
    assert policy.expected_utility(_signal("x", candidate="candidate-good")) > 0
    assert policy.expected_utility(_signal("y", candidate="candidate-bad")) < 0


def test_post_call_policy_rejects_missing_candidate() -> None:
    policy = EmpiricalCorrectionPolicy(post_adjudication=True)
    try:
        policy.fit([_signal("a")], [1.0])
    except ValueError as error:
        assert "requires candidate" in str(error)
    else:
        raise AssertionError("missing candidate must fail")


def test_supervised_policy_learns_benefit_and_harm_patterns() -> None:
    signals = []
    utilities = []
    for index in range(120):
        beneficial = index % 2 == 0
        signals.append(CorrectionSignal(
            identity=str(index),
            local_label="local-a" if beneficial else "local-b",
            local_confidence=0.55 if beneficial else 0.9,
            disagreement=0.4 if beneficial else 0.05,
            retrieval_support=0.7,
            candidate_label="local-b" if beneficial else "local-a",
            candidate_confidence=0.9,
            local_probabilities=(
                (0.55, 0.45) if beneficial else (0.1, 0.9)
            ),
        ))
        utilities.append(1.0 if beneficial else -1.0)
    policy = SupervisedCorrectionPolicy(
        classes=["local-a", "local-b"],
        post_adjudication=True,
        random_state=7,
    ).fit(signals, utilities)
    scores = policy.scores(signals)
    assert scores[::2].mean() > 0.5
    assert scores[1::2].mean() < -0.5
