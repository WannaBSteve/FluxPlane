"""Small dependency-free checks for the multimodal experiment protocol."""

from datetime import date

import pytest

np = pytest.importorskip("numpy")

from fluxplane.experiments.fusion import (
    FusionRecord,
    apply_taxonomy_mask,
    chronological_split,
    semantic_consistency,
)


def record(index: int, label: str = "issue-a", product: str = "card") -> FusionRecord:
    return FusionRecord(str(index), date(2026, 1, index), "text", label, product, {})


def test_chronological_split_keeps_later_samples_for_evaluation() -> None:
    train, validation, test = chronological_split([record(index) for index in range(1, 11)])
    assert [len(train), len(validation), len(test)] == [6, 2, 2]
    assert max(item.received for item in train) < min(item.received for item in validation)
    assert max(item.received for item in validation) < min(item.received for item in test)


def test_taxonomy_mask_removes_semantically_invalid_issue() -> None:
    probabilities = np.array([[0.2, 0.8], [0.6, 0.4]])
    classes = ["issue-a", "issue-b"]
    allowed = {"card": {"issue-a"}, "loan": {"issue-b"}}
    masked = apply_taxonomy_mask(probabilities, classes, ["card", "loan"], allowed)
    predictions = [classes[index] for index in np.argmax(masked, axis=1)]
    assert predictions == ["issue-a", "issue-b"]
    assert semantic_consistency(predictions, ["card", "loan"], allowed) == 1.0
