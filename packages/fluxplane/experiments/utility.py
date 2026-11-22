"""Transparent validation-fitted policies for selective LLM correction."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class CorrectionSignal:
    """Signals available before or after a candidate LLM adjudication."""

    identity: str
    local_label: str
    local_confidence: float
    disagreement: float
    retrieval_support: float
    candidate_label: str | None = None
    candidate_confidence: float | None = None
    local_probabilities: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        for name, value in (
            ("local_confidence", self.local_confidence),
            ("disagreement", self.disagreement),
            ("retrieval_support", self.retrieval_support),
        ):
            if not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"{name} must be finite and in [0, 1]")
        if self.candidate_confidence is not None and (
            not math.isfinite(self.candidate_confidence)
            or not 0 <= self.candidate_confidence <= 1
        ):
            raise ValueError("candidate_confidence must be finite and in [0, 1]")
        if (self.candidate_label is None) != (self.candidate_confidence is None):
            raise ValueError("candidate label and confidence must be provided together")
        if self.local_probabilities:
            if any(
                not math.isfinite(value) or not 0 <= value <= 1
                for value in self.local_probabilities
            ):
                raise ValueError("local probabilities must be finite and in [0, 1]")
            if not math.isclose(sum(self.local_probabilities), 1.0, abs_tol=1e-5):
                raise ValueError("local probabilities must sum to one")


@dataclass(frozen=True)
class UtilityDecision:
    identity: str
    selected: bool
    expected_utility: float


class EmpiricalCorrectionPolicy:
    """Estimate correction utility with an auditable, hierarchically shrunk table.

    This is deliberately not a learned black-box model. Validation outcomes are
    grouped into fixed bins, sparse cells shrink to a coarser parent group, and
    parent groups shrink to the global mean. A positive score means the observed
    correction benefit exceeded the harm under the chosen validation utility.
    """

    def __init__(self, *, post_adjudication: bool, prior_strength: float = 20.0) -> None:
        if prior_strength <= 0 or not math.isfinite(prior_strength):
            raise ValueError("prior_strength must be finite and positive")
        self.post_adjudication = post_adjudication
        self.prior_strength = prior_strength
        self._global_mean: float | None = None
        self._parent: dict[tuple[str, ...], tuple[float, int]] = {}
        self._cells: dict[tuple[str, ...], tuple[float, int]] = {}

    @staticmethod
    def _bin(value: float, boundaries: tuple[float, ...]) -> str:
        return str(sum(value >= boundary for boundary in boundaries))

    def _keys(self, signal: CorrectionSignal) -> tuple[tuple[str, ...], tuple[str, ...]]:
        local_confidence = self._bin(signal.local_confidence, (0.45, 0.65, 0.82))
        if self.post_adjudication:
            if signal.candidate_label is None or signal.candidate_confidence is None:
                raise ValueError("post-adjudication policy requires candidate signals")
            candidate_confidence = self._bin(
                signal.candidate_confidence, (0.5, 0.7, 0.85)
            )
            parent = (signal.local_label, signal.candidate_label)
            cell = (*parent, local_confidence, candidate_confidence)
        else:
            disagreement = self._bin(signal.disagreement, (0.08, 0.2))
            support = self._bin(signal.retrieval_support, (0.45, 0.65))
            parent = (signal.local_label,)
            cell = (*parent, local_confidence, disagreement, support)
        return parent, cell

    def fit(
        self, signals: list[CorrectionSignal], utilities: list[float]
    ) -> EmpiricalCorrectionPolicy:
        if not signals or len(signals) != len(utilities):
            raise ValueError("signals and utilities must have the same non-zero length")
        if any(not math.isfinite(value) for value in utilities):
            raise ValueError("utilities must be finite")
        parent_values: dict[tuple[str, ...], list[float]] = defaultdict(list)
        cell_values: dict[tuple[str, ...], list[float]] = defaultdict(list)
        for signal, utility in zip(signals, utilities):
            parent, cell = self._keys(signal)
            parent_values[parent].append(float(utility))
            cell_values[cell].append(float(utility))
        self._global_mean = sum(utilities) / len(utilities)
        self._parent = {
            key: (sum(values), len(values)) for key, values in parent_values.items()
        }
        self._cells = {
            key: (sum(values), len(values)) for key, values in cell_values.items()
        }
        return self

    def expected_utility(self, signal: CorrectionSignal) -> float:
        if self._global_mean is None:
            raise RuntimeError("policy must be fitted before scoring")
        parent, cell = self._keys(signal)
        parent_sum, parent_count = self._parent.get(parent, (0.0, 0))
        parent_mean = (
            parent_sum + self.prior_strength * self._global_mean
        ) / (parent_count + self.prior_strength)
        cell_sum, cell_count = self._cells.get(cell, (0.0, 0))
        return (cell_sum + self.prior_strength * parent_mean) / (
            cell_count + self.prior_strength
        )

    def select(
        self,
        signals: list[CorrectionSignal],
        *,
        budget_rate: float,
        minimum_utility: float = 0.0,
    ) -> list[UtilityDecision]:
        if not 0 <= budget_rate <= 1:
            raise ValueError("budget_rate must be in [0, 1]")
        if not math.isfinite(minimum_utility):
            raise ValueError("minimum_utility must be finite")
        budget = math.floor(len(signals) * budget_rate)
        scored = sorted(
            ((self.expected_utility(signal), signal) for signal in signals),
            key=lambda pair: (-pair[0], pair[1].identity),
        )
        selected = {
            signal.identity
            for utility, signal in scored[:budget]
            if utility > minimum_utility
        }
        return [
            UtilityDecision(
                identity=signal.identity,
                selected=signal.identity in selected,
                expected_utility=utility,
            )
            for utility, signal in scored
        ]

    def audit(self) -> dict[str, float | int | bool]:
        if self._global_mean is None:
            raise RuntimeError("policy must be fitted before audit")
        return {
            "post_adjudication": self.post_adjudication,
            "prior_strength": self.prior_strength,
            "global_mean_utility": self._global_mean,
            "parent_groups": len(self._parent),
            "cells": len(self._cells),
        }


class SupervisedCorrectionPolicy:
    """Learn a nonlinear correction score from validation-only outcomes.

    The model predicts whether replacing the local decision is beneficial,
    neutral, or harmful. Its score is ``P(benefit) - P(harm)``. All features
    are available either before the LLM call or immediately after the
    candidate response, depending on ``post_adjudication``.
    """

    def __init__(
        self,
        *,
        classes: list[str],
        post_adjudication: bool,
        random_state: int = 0,
    ) -> None:
        if not classes or len(classes) != len(set(classes)):
            raise ValueError("classes must be unique and non-empty")
        self.classes = tuple(classes)
        self.post_adjudication = post_adjudication
        self.random_state = random_state
        self._model = None
        self._outcome_counts: dict[str, int] = {}

    def _local_probabilities(self, signal: CorrectionSignal) -> np.ndarray:
        if signal.local_probabilities:
            if len(signal.local_probabilities) != len(self.classes):
                raise ValueError("local probability count must match classes")
            return np.asarray(signal.local_probabilities, dtype=float)
        probabilities = np.full(
            len(self.classes),
            (1 - signal.local_confidence) / max(len(self.classes) - 1, 1),
            dtype=float,
        )
        try:
            probabilities[self.classes.index(signal.local_label)] = signal.local_confidence
        except ValueError as exc:
            raise ValueError("local label must belong to classes") from exc
        return probabilities

    def _features(self, signals: list[CorrectionSignal]) -> np.ndarray:
        rows = []
        for signal in signals:
            probabilities = self._local_probabilities(signal)
            ordered = np.sort(probabilities)
            margin = float(ordered[-1] - ordered[-2]) if len(ordered) > 1 else 1.0
            entropy = float(-np.sum(probabilities * np.log(probabilities + 1e-12)))
            local_one_hot = [float(signal.local_label == label) for label in self.classes]
            row = [
                *probabilities.tolist(),
                *local_one_hot,
                signal.local_confidence,
                margin,
                entropy,
                signal.disagreement,
                signal.retrieval_support,
                signal.local_confidence * signal.disagreement,
                signal.local_confidence * signal.retrieval_support,
                signal.disagreement * signal.retrieval_support,
            ]
            if self.post_adjudication:
                if signal.candidate_label is None or signal.candidate_confidence is None:
                    raise ValueError("post-adjudication policy requires candidate signals")
                candidate_one_hot = [
                    float(signal.candidate_label == label) for label in self.classes
                ]
                row.extend([
                    *candidate_one_hot,
                    signal.candidate_confidence,
                    float(signal.candidate_label == signal.local_label),
                    signal.candidate_confidence - signal.local_confidence,
                    signal.candidate_confidence * signal.disagreement,
                    signal.candidate_confidence * signal.retrieval_support,
                ])
            rows.append(row)
        return np.asarray(rows, dtype=float)

    def fit(
        self, signals: list[CorrectionSignal], utilities: list[float]
    ) -> SupervisedCorrectionPolicy:
        if not signals or len(signals) != len(utilities):
            raise ValueError("signals and utilities must have the same non-zero length")
        if any(not math.isfinite(value) for value in utilities):
            raise ValueError("utilities must be finite")
        from sklearn.ensemble import HistGradientBoostingClassifier

        outcomes = np.sign(np.asarray(utilities, dtype=float)).astype(int)
        unique, counts = np.unique(outcomes, return_counts=True)
        if len(unique) < 2:
            raise ValueError("supervised policy requires at least two correction outcomes")
        self._outcome_counts = {
            {-1: "harm", 0: "neutral", 1: "benefit"}[int(label)]: int(count)
            for label, count in zip(unique, counts)
        }
        self._model = HistGradientBoostingClassifier(
            learning_rate=0.06,
            max_iter=160,
            max_leaf_nodes=15,
            min_samples_leaf=30,
            l2_regularization=3.0,
            class_weight="balanced",
            early_stopping=False,
            random_state=self.random_state,
        ).fit(self._features(signals), outcomes)
        return self

    def scores(self, signals: list[CorrectionSignal]) -> np.ndarray:
        if self._model is None:
            raise RuntimeError("policy must be fitted before scoring")
        probabilities = self._model.predict_proba(self._features(signals))
        indices = {int(label): index for index, label in enumerate(self._model.classes_)}
        benefit = (
            probabilities[:, indices[1]] if 1 in indices else np.zeros(len(signals))
        )
        harm = probabilities[:, indices[-1]] if -1 in indices else np.zeros(len(signals))
        return benefit - harm

    def audit(self) -> dict[str, object]:
        if self._model is None:
            raise RuntimeError("policy must be fitted before audit")
        return {
            "post_adjudication": self.post_adjudication,
            "model": "HistGradientBoostingClassifier",
            "score": "P(benefit)-P(harm)",
            "classes": list(self.classes),
            "outcomes": self._outcome_counts,
            "iterations": int(self._model.n_iter_),
            "random_state": self.random_state,
        }
