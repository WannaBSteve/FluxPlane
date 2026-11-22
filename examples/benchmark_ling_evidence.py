"""Evaluate strong local evidence, Ling and validation-fitted selective correction."""

from __future__ import annotations

import argparse
import json
import platform
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path

import numpy as np
from benchmark_agentic_fusion import (
    _sparse32,
)
from benchmark_frozen_e5 import (
    STRUCTURED_FIELDS,
    _align,
    _catboost,
    _classwise_pool,
    _encode_fields,
    _fit_catboost,
    _joint_frame,
    _pca_fields,
    _retrieval_probabilities,
    _rule_probabilities,
    _rule_tables,
    _tune_decision_bias,
    _tune_reliability,
)
from fluxplane.config import settings
from fluxplane.experiments.agentic import BudgetedUtilityRouter, RoutingSignal
from fluxplane.experiments.fusion import (
    FusionRecord,
    audit_dataset,
    chronological_split,
    load_records,
    validate_audit,
)
from fluxplane.experiments.llm_routing import (
    CachedEvidenceRouter,
    LlmPrediction,
    RoutingItem,
    RoutingStats,
)
from fluxplane.experiments.utility import (
    CorrectionSignal,
    EmpiricalCorrectionPolicy,
    SupervisedCorrectionPolicy,
)
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    log_loss,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import label_binarize, normalize


def _retrieval_with_context(
    train_lexical,
    query_lexical,
    train_semantic: np.ndarray,
    query_semantic: np.ndarray,
    train: list[FusionRecord],
    queries: list[FusionRecord],
    classes: list[str],
    *,
    neighbors: int,
    context_cases: int,
    chunk_size: int = 256,
) -> tuple[np.ndarray, np.ndarray, list[tuple[str, ...]]]:
    class_index = {label: index for index, label in enumerate(classes)}
    label_indices = np.asarray([class_index[record.label] for record in train])
    prior = np.asarray([
        sum(record.label == label for record in train) + 1 for label in classes
    ], dtype=float)
    prior /= prior.sum()
    probabilities = np.empty((query_lexical.shape[0], len(classes)), dtype=float)
    support = np.empty(query_lexical.shape[0], dtype=float)
    contexts: list[tuple[str, ...]] = []
    train_transpose = train_lexical.transpose().tocsr()
    normalized_train_semantic = normalize(train_semantic)
    normalized_query_semantic = normalize(query_semantic)
    train_issue = np.asarray([record.semantic.get("Issue", "<missing>") for record in train])
    train_product = np.asarray([
        record.structured.get("Product", "<missing>") for record in train
    ])
    train_company = np.asarray([
        record.semantic.get("Company", "<missing>") for record in train
    ])
    k = min(neighbors, train_lexical.shape[0])
    for offset in range(0, query_lexical.shape[0], chunk_size):
        block = query_lexical[offset:offset + chunk_size] @ train_transpose
        semantic_block = (
            normalized_query_semantic[offset:offset + chunk_size]
            @ normalized_train_semantic.T
        )
        for local_row in range(block.shape[0]):
            query = queries[offset + local_row]
            lexical = block.getrow(local_row).toarray().ravel()
            semantic = np.clip(np.asarray(semantic_block[local_row]).ravel(), 0, 1)
            similarities = (
                0.5 * lexical
                + 0.3 * semantic
                + 0.1 * (train_issue == query.semantic.get("Issue", "<missing>"))
                + 0.07 * (
                    train_product == query.structured.get("Product", "<missing>")
                )
                + 0.03 * (train_company == query.semantic.get("Company", "<missing>"))
            )
            indices = np.argpartition(similarities, -k)[-k:]
            indices = indices[np.argsort(similarities[indices])[::-1]]
            weights = np.square(np.clip(similarities[indices], 0, None))
            counts = 0.25 * prior
            for index, weight in zip(indices, weights):
                counts[label_indices[index]] += max(float(weight), 1e-6)
            probabilities[offset + local_row] = counts / counts.sum()
            support[offset + local_row] = float(np.clip(similarities[indices[0]], 0, 1))
            case_descriptions = []
            for index in indices[:context_cases]:
                case = train[index]
                excerpt = " ".join((case.narrative or case.text).split())[:180]
                case_descriptions.append(
                    f"outcome={case.label}; issue={case.semantic.get('Issue', '<missing>')}; "
                    f"product={case.structured.get('Product', '<missing>')}; excerpt={excerpt}"
                )
            contexts.append(tuple(case_descriptions))
    return probabilities, support, contexts


def _mean_pairwise_js(experts: tuple[np.ndarray, ...]) -> np.ndarray:
    """Mean normalized Jensen-Shannon divergence for every record."""
    if len(experts) < 2:
        raise ValueError("at least two experts are required")
    result = np.zeros(experts[0].shape[0], dtype=float)
    pairs = 0
    for left in range(len(experts)):
        for right in range(left + 1, len(experts)):
            first = np.clip(experts[left], 1e-12, 1)
            second = np.clip(experts[right], 1e-12, 1)
            middle = (first + second) / 2
            divergence = 0.5 * (
                np.sum(first * np.log(first / middle), axis=1)
                + np.sum(second * np.log(second / middle), axis=1)
            ) / np.log(2)
            result += divergence
            pairs += 1
    return np.clip(result / pairs, 0, 1)


def _local_evidence(
    train: list[FusionRecord],
    validation: list[FusionRecord],
    test: list[FusionRecord],
    *,
    seed: int,
    text_features: int,
    neighbors: int,
    context_cases: int,
    embedding_model: str,
    embedding_cache: Path,
    pca_dimensions: int,
    embedding_batch_size: int,
    embedding_device: str | None,
) -> dict:
    train_labels = [record.label for record in train]
    validation_labels = [record.label for record in validation]
    classes = sorted(set(train_labels))

    all_records = train + validation + test
    embeddings = _encode_fields(
        all_records,
        model_name=embedding_model,
        cache_dir=embedding_cache,
        dimensions=pca_dimensions,
        batch_size=embedding_batch_size,
        device=embedding_device,
    )
    train_e5, validation_e5, test_e5 = _pca_fields(
        embeddings, len(train), len(validation), pca_dimensions, seed
    )

    train_joint = _joint_frame(train, train_e5)
    validation_joint_frame = _joint_frame(validation, validation_e5)
    test_joint_frame = _joint_frame(test, test_e5)
    joint_model = _fit_catboost(
        _catboost(seed),
        train_joint,
        train_labels,
        validation_joint_frame,
        validation_labels,
        list(STRUCTURED_FIELDS),
    )

    def probabilities(frame) -> np.ndarray:
        return _align(
            joint_model.predict_proba(frame),
            [str(value) for value in joint_model.classes_],
            classes,
        )

    validation_joint = probabilities(validation_joint_frame)
    test_joint = probabilities(test_joint_frame)
    narrative_width = pca_dimensions
    validation_retrieval = _retrieval_probabilities(
        train_e5[:, :narrative_width],
        validation_e5[:, :narrative_width],
        train_labels,
        classes,
        neighbors,
    )
    test_retrieval = _retrieval_probabilities(
        train_e5[:, :narrative_width],
        test_e5[:, :narrative_width],
        train_labels,
        classes,
        neighbors,
    )
    tables, counts = _rule_tables(train)
    validation_rules = _rule_probabilities(validation, classes, tables, counts)
    test_rules = _rule_probabilities(test, classes, tables, counts)
    reliability, _ = _tune_reliability(
        validation_labels,
        (validation_joint, validation_retrieval, validation_rules),
        classes,
    )
    validation_probabilities = _classwise_pool(
        (validation_joint, validation_retrieval, validation_rules), reliability
    )
    test_probabilities = _classwise_pool(
        (test_joint, test_retrieval, test_rules), reliability
    )
    decision_bias, validation_macro_f1 = _tune_decision_bias(
        validation_labels, validation_probabilities, classes
    )

    text_vectorizer = TfidfVectorizer(
        max_features=text_features,
        min_df=2,
        ngram_range=(1, 2),
        strip_accents="unicode",
        sublinear_tf=True,
    )
    train_text = _sparse32(text_vectorizer.fit_transform(record.text for record in train))
    validation_text = _sparse32(
        text_vectorizer.transform(record.text for record in validation)
    )
    test_text = _sparse32(text_vectorizer.transform(record.text for record in test))
    retrieval_train = normalize(train_text, norm="l2", copy=True)
    _, validation_support, validation_contexts = _retrieval_with_context(
        retrieval_train,
        normalize(validation_text, norm="l2", copy=True),
        train_e5[:, :narrative_width],
        validation_e5[:, :narrative_width],
        train,
        validation,
        classes,
        neighbors=neighbors,
        context_cases=context_cases,
    )
    _, test_support, test_contexts = _retrieval_with_context(
        retrieval_train,
        normalize(test_text, norm="l2", copy=True),
        train_e5[:, :narrative_width],
        test_e5[:, :narrative_width],
        train,
        test,
        classes,
        neighbors=neighbors,
        context_cases=context_cases,
    )
    validation_disagreement = _mean_pairwise_js(
        (validation_joint, validation_retrieval, validation_rules)
    )
    test_disagreement = _mean_pairwise_js((test_joint, test_retrieval, test_rules))
    return {
        "classes": classes,
        "decision_bias": decision_bias,
        "validation_macro_f1_after_bias": validation_macro_f1,
        "validation": {
            "probabilities": validation_probabilities,
            "disagreement": validation_disagreement,
            "retrieval_support": validation_support,
            "contexts": validation_contexts,
        },
        "test": {
            "probabilities": test_probabilities,
            "disagreement": test_disagreement,
            "retrieval_support": test_support,
            "contexts": test_contexts,
        },
        "validation_mean_disagreement": float(validation_disagreement.mean()),
    }


def _items(
    records: list[FusionRecord],
    classes: list[str],
    probabilities: np.ndarray,
    contexts: list[tuple[str, ...]],
    *,
    with_evidence: bool,
    predicted_labels: np.ndarray | None = None,
) -> list[RoutingItem]:
    predictions = (
        np.asarray(classes)[np.argmax(probabilities, axis=1)]
        if predicted_labels is None
        else predicted_labels
    )
    confidence = np.max(probabilities, axis=1)
    result = []
    for index, record in enumerate(records):
        structured = (
            ("Issue", record.semantic.get("Issue", "<missing>")),
            ("Sub-issue", record.semantic.get("Sub-issue", "<missing>")),
            ("Company", record.semantic.get("Company", "<missing>")),
            *(tuple(record.structured.items())),
        )
        result.append(RoutingItem(
            identity=record.identity,
            text=record.narrative or record.text,
            product=record.product,
            sub_product=record.structured.get("Sub-product", "<missing>"),
            local_label=str(predictions[index]) if with_evidence else "",
            local_confidence=float(confidence[index]) if with_evidence else 0.0,
            candidates=tuple(classes),
            task=(
                "Predict the company's eventual response using only information available "
                "before that response."
            ),
            structured_evidence=structured if with_evidence else (),
            retrieved_cases=contexts[index] if with_evidence else (),
        ))
    return result


def _prediction_probabilities(
    records: list[FusionRecord],
    predictions: dict[str, LlmPrediction],
    classes: list[str],
) -> np.ndarray:
    class_index = {label: index for index, label in enumerate(classes)}
    result = []
    for record in records:
        prediction = predictions[record.identity]
        confidence = min(0.999, max(1 / len(classes), prediction.confidence))
        distribution = np.full(
            len(classes), (1 - confidence) / (len(classes) - 1), dtype=float
        )
        distribution[class_index[prediction.label]] = confidence
        result.append(distribution)
    return np.asarray(result)


def _decision_labels(
    probabilities: np.ndarray, classes: list[str], decision_bias: np.ndarray
) -> np.ndarray:
    return np.asarray(classes)[np.argmax(probabilities * decision_bias, axis=1)]


def _metrics(
    truth: list[str],
    probabilities: np.ndarray,
    classes: list[str],
    predictions: np.ndarray | None = None,
) -> dict:
    predicted = (
        np.asarray(classes)[np.argmax(probabilities, axis=1)]
        if predictions is None
        else np.asarray(predictions)
    )
    binary = label_binarize(truth, classes=classes)
    confidence = np.max(probabilities, axis=1)
    truth_array = np.asarray(truth)
    correct = predicted == truth_array
    calibration = 0.0
    for lower, upper in zip(np.linspace(0, 0.9, 10), np.linspace(0.1, 1, 10)):
        selected = (confidence > lower) & (confidence <= upper)
        if selected.any():
            calibration += selected.mean() * abs(
                correct[selected].mean() - confidence[selected].mean()
            )
    return {
        "macro_average_precision": round(float(average_precision_score(
            binary, probabilities, average="macro"
        )), 6),
        "macro_ovr_auroc": round(float(roc_auc_score(
            binary, probabilities, average="macro", multi_class="ovr"
        )), 6),
        "macro_f1": round(float(f1_score(truth_array, predicted, average="macro")), 6),
        "weighted_f1": round(float(f1_score(
            truth_array, predicted, average="weighted"
        )), 6),
        "accuracy": round(float(accuracy_score(truth_array, predicted)), 6),
        "log_loss": round(float(log_loss(
            truth_array, probabilities, labels=classes
        )), 6),
        "expected_calibration_error": round(float(calibration), 6),
    }


def _correction_signals(
    records: list[FusionRecord],
    local_probabilities: np.ndarray,
    local_labels: np.ndarray,
    disagreement: np.ndarray,
    support: np.ndarray,
    candidates: dict[str, LlmPrediction] | None = None,
) -> list[CorrectionSignal]:
    result = []
    for index, record in enumerate(records):
        candidate = candidates.get(record.identity) if candidates else None
        result.append(CorrectionSignal(
            identity=record.identity,
            local_label=str(local_labels[index]),
            local_confidence=float(local_probabilities[index].max()),
            disagreement=float(disagreement[index]),
            retrieval_support=float(support[index]),
            candidate_label=candidate.label if candidate else None,
            candidate_confidence=candidate.confidence if candidate else None,
            local_probabilities=tuple(
                float(value) for value in local_probabilities[index]
            ),
        ))
    return result


def _balanced_correction_utilities(
    truth: list[str], local_labels: np.ndarray, candidate_labels: np.ndarray
) -> list[float]:
    counts = Counter(truth)
    scale = len(truth) / len(counts)
    return [
        scale / counts[label] * (
            float(candidate == label) - float(local == label)
        )
        for label, local, candidate in zip(truth, local_labels, candidate_labels)
    ]


def _heuristic_hybrid(
    local: np.ndarray,
    local_labels: np.ndarray,
    ling: np.ndarray,
    ling_labels: np.ndarray,
    test: list[FusionRecord],
    disagreement: np.ndarray,
    support: np.ndarray,
    rate: float,
) -> tuple[np.ndarray, np.ndarray, int]:
    router = BudgetedUtilityRouter()
    signals = [
        RoutingSignal(
            identity=record.identity,
            confidence=float(local[index].max()),
            disagreement=float(disagreement[index]),
            retrieval_support=float(support[index]),
        )
        for index, record in enumerate(test)
    ]
    selected = {
        decision.identity
        for decision in router.select(signals, budget_rate=rate)
        if decision.route_to_large_model
    }
    probabilities = local.copy()
    predictions = local_labels.copy()
    for index, record in enumerate(test):
        if record.identity in selected:
            probabilities[index] = ling[index]
            predictions[index] = ling_labels[index]
    return probabilities, predictions, len(selected)


def _learned_hybrid(
    local: np.ndarray,
    local_labels: np.ndarray,
    ling: np.ndarray,
    ling_labels: np.ndarray,
    pre_signals: list[CorrectionSignal],
    post_signals: list[CorrectionSignal],
    pre_policy: EmpiricalCorrectionPolicy,
    post_policy: EmpiricalCorrectionPolicy,
    rate: float,
) -> tuple[np.ndarray, np.ndarray, int, int]:
    routed = {
        decision.identity
        for decision in pre_policy.select(pre_signals, budget_rate=rate)
        if decision.selected
    }
    accepted = {
        decision.identity
        for decision in post_policy.select(post_signals, budget_rate=1.0)
        if decision.selected and decision.identity in routed
    }
    probabilities = local.copy()
    predictions = local_labels.copy()
    for index, signal in enumerate(pre_signals):
        if signal.identity in accepted:
            probabilities[index] = ling[index]
            predictions[index] = ling_labels[index]
    return probabilities, predictions, len(routed), len(accepted)


def _cross_validated_supervised_scores(
    signals: list[CorrectionSignal],
    utilities: list[float],
    classes: list[str],
    *,
    post_adjudication: bool,
    folds: int,
    seed: int,
) -> np.ndarray:
    outcomes = np.sign(np.asarray(utilities, dtype=float)).astype(int)
    _, counts = np.unique(outcomes, return_counts=True)
    split_count = min(folds, int(counts.min()))
    if split_count < 2:
        raise ValueError("supervised routing requires at least two examples per outcome")
    scores = np.empty(len(signals), dtype=float)
    splitter = StratifiedKFold(
        n_splits=split_count, shuffle=True, random_state=seed
    )
    indices = np.arange(len(signals))
    for fold, (fit_indices, holdout_indices) in enumerate(
        splitter.split(indices, outcomes)
    ):
        policy = SupervisedCorrectionPolicy(
            classes=classes,
            post_adjudication=post_adjudication,
            random_state=seed + fold,
        ).fit(
            [signals[index] for index in fit_indices],
            [utilities[index] for index in fit_indices],
        )
        scores[holdout_indices] = policy.scores(
            [signals[index] for index in holdout_indices]
        )
    return scores


def _supervised_selection(
    signals: list[CorrectionSignal],
    pre_scores: np.ndarray,
    post_scores: np.ndarray,
    *,
    rate: float,
    acceptance_threshold: float,
) -> tuple[set[int], set[int]]:
    budget = int(np.floor(len(signals) * rate))
    order = sorted(
        range(len(signals)),
        key=lambda index: (-pre_scores[index], signals[index].identity),
    )
    routed = set(order[:budget])
    accepted = {
        index
        for index in routed
        if post_scores[index] >= acceptance_threshold
    }
    return routed, accepted


def _tune_acceptance_threshold(
    truth: list[str],
    local_labels: np.ndarray,
    candidate_labels: np.ndarray,
    signals: list[CorrectionSignal],
    pre_scores: np.ndarray,
    post_scores: np.ndarray,
    *,
    rate: float,
) -> dict[str, float | int]:
    routed, _ = _supervised_selection(
        signals,
        pre_scores,
        post_scores,
        rate=rate,
        acceptance_threshold=float(np.max(post_scores) + 1),
    )
    routed_scores = post_scores[list(routed)] if routed else post_scores[:0]
    if routed_scores.size == 0:
        return {"threshold": 1.0, "macro_f1": 0.0, "routed": 0, "accepted": 0}
    thresholds = np.unique(np.quantile(routed_scores, np.linspace(0, 1, 101)))
    thresholds = np.concatenate((
        [float(np.max(routed_scores) + 1e-6)],
        thresholds,
        [float(np.min(routed_scores) - 1e-6)],
    ))
    best: tuple[float, int, float] | None = None
    truth_array = np.asarray(truth)
    for threshold in thresholds:
        _, accepted = _supervised_selection(
            signals,
            pre_scores,
            post_scores,
            rate=rate,
            acceptance_threshold=float(threshold),
        )
        predictions = local_labels.copy()
        if accepted:
            accepted_indices = np.asarray(sorted(accepted), dtype=int)
            predictions[accepted_indices] = candidate_labels[accepted_indices]
        score = float(f1_score(truth_array, predictions, average="macro"))
        candidate = (score, -len(accepted), float(threshold))
        if best is None or candidate > best:
            best = candidate
    assert best is not None
    return {
        "threshold": best[2],
        "macro_f1": best[0],
        "routed": len(routed),
        "accepted": -best[1],
    }


def _supervised_hybrid(
    local: np.ndarray,
    local_labels: np.ndarray,
    candidate: np.ndarray,
    candidate_labels: np.ndarray,
    signals: list[CorrectionSignal],
    pre_scores: np.ndarray,
    post_scores: np.ndarray,
    *,
    rate: float,
    acceptance_threshold: float,
) -> tuple[np.ndarray, np.ndarray, int, int]:
    routed, accepted = _supervised_selection(
        signals,
        pre_scores,
        post_scores,
        rate=rate,
        acceptance_threshold=acceptance_threshold,
    )
    probabilities = local.copy()
    predictions = local_labels.copy()
    if accepted:
        accepted_indices = np.asarray(sorted(accepted), dtype=int)
        probabilities[accepted_indices] = candidate[accepted_indices]
        predictions[accepted_indices] = candidate_labels[accepted_indices]
    return probabilities, predictions, len(routed), len(accepted)


def run(args: argparse.Namespace) -> dict:
    started = time.perf_counter()
    audit = audit_dataset(args.input, "response")
    labels = validate_audit(
        audit,
        min_narratives=args.min_narratives,
        min_label_support=args.min_label_support,
        label_count=args.labels,
    )
    records = load_records(
        args.input, "response", labels, max_rows=args.max_rows, seed=args.seed
    )
    train, validation, test = chronological_split(records)
    local = _local_evidence(
        train,
        validation,
        test,
        seed=args.seed,
        text_features=args.text_features,
        neighbors=args.neighbors,
        context_cases=args.context_cases,
        embedding_model=args.embedding_model,
        embedding_cache=args.embedding_cache,
        pca_dimensions=args.pca_dimensions,
        embedding_batch_size=args.embedding_batch_size,
        embedding_device=args.embedding_device,
    )
    classes = local["classes"]
    config = settings()
    router = CachedEvidenceRouter(
        base_url=config.analysis_base_url,
        model=config.analysis_model,
        api_key=config.analysis_api_key,
        cache_dir=args.cache_dir,
        timeout_seconds=config.analysis_timeout_seconds,
        backend=args.api_backend,
    )
    validation_local = local["validation"]
    test_local = local["test"]
    validation_local_labels = _decision_labels(
        validation_local["probabilities"], classes, local["decision_bias"]
    )
    test_local_labels = _decision_labels(
        test_local["probabilities"], classes, local["decision_bias"]
    )
    ling_only_items = _items(
        test,
        classes,
        test_local["probabilities"],
        test_local["contexts"],
        with_evidence=False,
    )
    validation_rag_items = _items(
        validation,
        classes,
        validation_local["probabilities"],
        validation_local["contexts"],
        with_evidence=True,
        predicted_labels=validation_local_labels,
    )
    test_rag_items = _items(
        test,
        classes,
        test_local["probabilities"],
        test_local["contexts"],
        with_evidence=True,
        predicted_labels=test_local_labels,
    )
    ling_only, ling_only_stats = router.classify(
        ling_only_items,
        batch_size=args.batch_size,
        workers=args.workers,
    )
    validation_rag, validation_rag_stats = router.classify(
        validation_rag_items,
        batch_size=args.rag_batch_size,
        workers=args.workers,
    )
    test_rag, test_rag_stats = router.classify(
        test_rag_items,
        batch_size=args.rag_batch_size,
        workers=args.workers,
    )
    ling_only_probabilities = _prediction_probabilities(test, ling_only, classes)
    test_rag_probabilities = _prediction_probabilities(test, test_rag, classes)
    validation_rag_labels = np.asarray([
        validation_rag[record.identity].label for record in validation
    ])
    test_rag_labels = np.asarray([test_rag[record.identity].label for record in test])
    ling_only_labels = np.asarray([ling_only[record.identity].label for record in test])
    validation_truth = [record.label for record in validation]
    truth = [record.label for record in test]

    validation_pre_signals = _correction_signals(
        validation,
        validation_local["probabilities"],
        validation_local_labels,
        validation_local["disagreement"],
        validation_local["retrieval_support"],
    )
    validation_post_signals = _correction_signals(
        validation,
        validation_local["probabilities"],
        validation_local_labels,
        validation_local["disagreement"],
        validation_local["retrieval_support"],
        validation_rag,
    )
    validation_utilities = _balanced_correction_utilities(
        validation_truth, validation_local_labels, validation_rag_labels
    )
    pre_policy = EmpiricalCorrectionPolicy(
        post_adjudication=False, prior_strength=args.utility_prior_strength
    ).fit(validation_pre_signals, validation_utilities)
    post_policy = EmpiricalCorrectionPolicy(
        post_adjudication=True, prior_strength=args.utility_prior_strength
    ).fit(validation_post_signals, validation_utilities)
    validation_pre_oof_scores = _cross_validated_supervised_scores(
        validation_pre_signals,
        validation_utilities,
        classes,
        post_adjudication=False,
        folds=args.router_folds,
        seed=args.seed,
    )
    validation_post_oof_scores = _cross_validated_supervised_scores(
        validation_post_signals,
        validation_utilities,
        classes,
        post_adjudication=True,
        folds=args.router_folds,
        seed=args.seed,
    )
    supervised_pre_policy = SupervisedCorrectionPolicy(
        classes=classes, post_adjudication=False, random_state=args.seed
    ).fit(validation_pre_signals, validation_utilities)
    supervised_post_policy = SupervisedCorrectionPolicy(
        classes=classes, post_adjudication=True, random_state=args.seed
    ).fit(validation_post_signals, validation_utilities)
    test_pre_signals = _correction_signals(
        test,
        test_local["probabilities"],
        test_local_labels,
        test_local["disagreement"],
        test_local["retrieval_support"],
    )
    test_post_signals = _correction_signals(
        test,
        test_local["probabilities"],
        test_local_labels,
        test_local["disagreement"],
        test_local["retrieval_support"],
        test_rag,
    )
    test_pre_scores = supervised_pre_policy.scores(test_pre_signals)
    test_post_scores = supervised_post_policy.scores(test_post_signals)
    metrics = {
        "strong_local_classwise_e5_consensus": _metrics(
            truth, test_local["probabilities"], classes, test_local_labels
        ),
        "ling_only": _metrics(
            truth, ling_only_probabilities, classes, ling_only_labels
        ),
        "ling_with_hybrid_rag": _metrics(
            truth, test_rag_probabilities, classes, test_rag_labels
        ),
    }
    route_curve = []
    supervised_thresholds: dict[str, dict[str, float | int]] = {}
    for rate in args.route_rates:
        heuristic, heuristic_labels, heuristic_routed = _heuristic_hybrid(
            test_local["probabilities"],
            test_local_labels,
            test_rag_probabilities,
            test_rag_labels,
            test,
            test_local["disagreement"],
            test_local["retrieval_support"],
            rate,
        )
        heuristic_name = f"heuristic_route_{int(rate * 100):03d}pct"
        metrics[heuristic_name] = _metrics(
            truth, heuristic, classes, heuristic_labels
        )
        learned, learned_labels, learned_routed, learned_accepted = _learned_hybrid(
            test_local["probabilities"],
            test_local_labels,
            test_rag_probabilities,
            test_rag_labels,
            test_pre_signals,
            test_post_signals,
            pre_policy,
            post_policy,
            rate,
        )
        learned_name = f"learned_utility_route_{int(rate * 100):03d}pct"
        metrics[learned_name] = _metrics(truth, learned, classes, learned_labels)
        tuned = _tune_acceptance_threshold(
            validation_truth,
            validation_local_labels,
            validation_rag_labels,
            validation_post_signals,
            validation_pre_oof_scores,
            validation_post_oof_scores,
            rate=rate,
        )
        supervised_thresholds[f"{rate:.6f}"] = tuned
        supervised, supervised_labels, supervised_routed, supervised_accepted = (
            _supervised_hybrid(
                test_local["probabilities"],
                test_local_labels,
                test_rag_probabilities,
                test_rag_labels,
                test_post_signals,
                test_pre_scores,
                test_post_scores,
                rate=rate,
                acceptance_threshold=float(tuned["threshold"]),
            )
        )
        supervised_name = f"supervised_router_{int(rate * 100):03d}pct"
        metrics[supervised_name] = _metrics(
            truth, supervised, classes, supervised_labels
        )
        route_curve.append({
            "rate": rate,
            "heuristic": {
                "routed": heuristic_routed,
                "metrics_key": heuristic_name,
            },
            "learned_utility": {
                "routed": learned_routed,
                "accepted": learned_accepted,
                "metrics_key": learned_name,
            },
            "supervised_router": {
                "routed": supervised_routed,
                "accepted": supervised_accepted,
                "validation_oof_macro_f1": round(float(tuned["macro_f1"]), 6),
                "acceptance_threshold": round(float(tuned["threshold"]), 6),
                "metrics_key": supervised_name,
            },
        })
    if args.router_artifact:
        import joblib

        args.router_artifact.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "schema_version": 1,
                "classes": classes,
                "pre_call_policy": supervised_pre_policy,
                "post_adjudication_policy": supervised_post_policy,
                "thresholds_by_rate": supervised_thresholds,
                "objective": "validation OOF macro-F1 under route budget",
            },
            args.router_artifact,
        )
    oracle_probabilities = test_local["probabilities"].copy()
    oracle_labels = test_local_labels.copy()
    for index, label in enumerate(truth):
        if test_local_labels[index] != label and test_rag_labels[index] == label:
            oracle_probabilities[index] = test_rag_probabilities[index]
            oracle_labels[index] = test_rag_labels[index]
    metrics["oracle_correction_upper_bound"] = _metrics(
        truth, oracle_probabilities, classes, oracle_labels
    )
    corrected = int(sum(
        test_local_labels[index] != test[index].label
        and test_rag[test[index].identity].label == test[index].label
        for index in range(len(test))
    ))
    harmed = int(sum(
        test_local_labels[index] == test[index].label
        and test_rag[test[index].identity].label != test[index].label
        for index in range(len(test))
    ))
    changed_indices = [
        index for index, record in enumerate(test)
        if test_local_labels[index] != test_rag[record.identity].label
    ][:12]
    if len(changed_indices) < 12:
        changed_indices.extend(
            index for index in range(len(test))
            if index not in changed_indices
        )
        changed_indices = changed_indices[:12]
    explanation_error = None
    try:
        detailed, detailed_stats = router.classify(
            [
                replace(test_rag_items[index], require_explanation=True)
                for index in changed_indices
            ],
            batch_size=4,
            workers=min(args.workers, 3),
        )
    except RuntimeError as exc:
        detailed = {}
        detailed_stats = RoutingStats(0, 0, 0, ())
        explanation_error = type(exc).__name__
    all_latencies = (
        ling_only_stats.request_latencies_ms
        + validation_rag_stats.request_latencies_ms
        + test_rag_stats.request_latencies_ms
        + detailed_stats.request_latencies_ms
    )
    examples = []
    for index in changed_indices:
        record = test[index]
        prediction = detailed.get(record.identity, test_rag[record.identity])
        examples.append({
            "record_id": record.identity,
            "truth": record.label,
            "prediction": prediction.label,
            "evidence": list(prediction.evidence),
            "rationale": prediction.rationale,
            "counterfactual": prediction.counterfactual,
        })
    return {
        "schema_version": 1,
        "experiment": "cfpb_response_ling_evidence_adjudication",
        "track": "pre_response_business_track",
        "input": {
            "file": args.input.name,
            "usable_rows": audit.narrative_rows,
            "evaluated_rows": len(records),
            "labels": labels,
        },
        "split": {
            "method": "complete_chronological_60_20_20",
            "train": len(train),
            "validation": len(validation),
            "test": len(test),
        },
        "test_metrics": metrics,
        "route_curve": route_curve,
        "routing_policy": {
            "objective": "class-balanced validation correction utility",
            "pre_call": pre_policy.audit(),
            "post_adjudication": post_policy.audit(),
            "validation_rag_predictions": len(validation_rag),
            "strong_local_validation_macro_f1": round(
                local["validation_macro_f1_after_bias"], 6
            ),
            "supervised": {
                "training": "validation-only five-fold out-of-fold threshold selection",
                "pre_call": supervised_pre_policy.audit(),
                "post_adjudication": supervised_post_policy.audit(),
                "thresholds_by_rate": supervised_thresholds,
                "artifact": str(args.router_artifact) if args.router_artifact else None,
            },
        },
        "llm": {
            "model": config.analysis_model,
            "thinking_enabled": False,
            "batch_size": args.batch_size,
            "rag_batch_size": args.rag_batch_size,
            "workers": args.workers,
            "ling_only_predictions_evaluated": len(ling_only),
            "validation_rag_predictions_evaluated": len(validation_rag),
            "test_rag_predictions_evaluated": len(test_rag),
            "ling_only_api_calls_this_run": ling_only_stats.api_calls,
            "validation_rag_api_calls_this_run": validation_rag_stats.api_calls,
            "test_rag_api_calls_this_run": test_rag_stats.api_calls,
            "explanation_api_calls_this_run": detailed_stats.api_calls,
            "explanation_error": explanation_error,
            "cache_hits": (
                ling_only_stats.cache_hits
                + validation_rag_stats.cache_hits
                + test_rag_stats.cache_hits
                + detailed_stats.cache_hits
            ),
            "cache_misses": (
                ling_only_stats.cache_misses
                + validation_rag_stats.cache_misses
                + test_rag_stats.cache_misses
                + detailed_stats.cache_misses
            ),
            "request_latency_median_ms": (
                round(float(np.median(all_latencies)), 3) if all_latencies else None
            ),
            "request_latency_p95_ms": (
                round(float(np.percentile(all_latencies, 95)), 3) if all_latencies else None
            ),
            "rag_net_corrections": corrected - harmed,
            "rag_corrected_local_errors": corrected,
            "rag_harmed_local_correct": harmed,
        },
        "explanation_examples": examples,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "seed": args.seed,
            "wall_seconds": round(time.perf_counter() - started, 3),
        },
    }


def _rate(value: str) -> float:
    result = float(value)
    if not 0 <= result <= 1:
        raise argparse.ArgumentTypeError("route rates must be in [0, 1]")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache-dir", type=Path, default=Path("runtime/llm-response-cache"))
    parser.add_argument("--labels", type=int, default=4)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--text-features", type=int, default=50_000)
    parser.add_argument("--min-narratives", type=int, default=1_000)
    parser.add_argument("--min-label-support", type=int, default=200)
    parser.add_argument("--neighbors", type=int, default=15)
    parser.add_argument("--context-cases", type=int, default=3)
    parser.add_argument("--embedding-model", default="intfloat/e5-small-v2")
    parser.add_argument("--embedding-cache", type=Path, default=Path("runtime/e5-cache"))
    parser.add_argument("--pca-dimensions", type=int, default=30)
    parser.add_argument("--embedding-batch-size", type=int, default=128)
    parser.add_argument("--embedding-device", default="mps")
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--rag-batch-size", type=int, default=30)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--utility-prior-strength", type=float, default=20.0)
    parser.add_argument("--router-folds", type=int, default=5)
    parser.add_argument(
        "--router-artifact",
        type=Path,
        default=Path("artifacts/cfpb-response-supervised-router.joblib"),
    )
    parser.add_argument("--api-backend", choices=("httpx", "curl"), default="curl")
    parser.add_argument(
        "--route-rates", type=_rate, nargs="+", default=[0.1, 0.2, 0.3, 0.5, 1.0]
    )
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args()
    if min(
        args.labels,
        args.max_rows,
        args.text_features,
        args.min_narratives,
        args.min_label_support,
        args.neighbors,
        args.context_cases,
        args.pca_dimensions,
        args.embedding_batch_size,
        args.batch_size,
        args.rag_batch_size,
        args.workers,
        args.router_folds,
        args.utility_prior_strength,
    ) < 1:
        parser.error("counts must be positive")
    report = run(args)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
