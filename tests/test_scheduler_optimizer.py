from fluxplane.scheduling.optimizer import PolicyMeasurement, nash_scores, select_policy


def measurement(name: str, latency: float, freshness: float, throughput: float
                ) -> PolicyMeasurement:
    return PolicyMeasurement(name, latency, freshness, throughput, {})


def test_nash_policy_rejects_fast_but_stale_allocation() -> None:
    items = [
        measurement("shared", 100, 0.5, 10),
        measurement("fixed", 200, 1.0, 5),
        measurement("dynamic", 120, 1.0, 9),
    ]
    selected, scores = select_policy(items, minimum_deadline_rate=0.99)
    assert scores["shared"] == 0
    assert selected == "dynamic"


def test_nash_policy_validates_metrics() -> None:
    item = measurement("invalid", 0, 1.0, 1)
    try:
        nash_scores([item])
    except ValueError as exc:
        assert "outside" in str(exc)
    else:
        raise AssertionError("invalid policy metrics were accepted")
