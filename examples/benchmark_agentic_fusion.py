"""Benchmark complete CFPB response prediction with explicit evidence agents."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import platform
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import sklearn
from fluxplane.experiments.agentic import (
    ConflictAwareArbiter,
    EvidenceOpinion,
    classwise_linear_pool,
)
from fluxplane.experiments.fusion import (
    FusionRecord,
    audit_dataset,
    chronological_split,
    load_records,
    validate_audit,
)
from fluxplane.experiments.protocols import CFPB_RESPONSE
from scipy.sparse import hstack
from sklearn.feature_extraction import DictVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    log_loss,
    roc_auc_score,
)
from sklearn.preprocessing import label_binarize, normalize


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sparse32(matrix):
    matrix = matrix.tocsr()
    matrix.indices = matrix.indices.astype(np.int32, copy=False)
    matrix.indptr = matrix.indptr.astype(np.int32, copy=False)
    return matrix


def _classifier(seed: int, c_value: float = 4.0) -> LogisticRegression:
    return LogisticRegression(
        C=c_value,
        class_weight="balanced",
        max_iter=750,
        random_state=seed,
        solver="saga",
        tol=1e-3,
    )


def _expected_calibration_error(truth: list[str], probabilities: np.ndarray,
                                classes: list[str], bins: int = 10) -> float:
    truth_array = np.asarray(truth)
    predictions = np.asarray(classes)[np.argmax(probabilities, axis=1)]
    confidences = np.max(probabilities, axis=1)
    correctness = predictions == truth_array
    result = 0.0
    boundaries = np.linspace(0, 1, bins + 1)
    for lower, upper in itertools.pairwise(boundaries):
        selected = (confidences > lower) & (confidences <= upper)
        if selected.any():
            result += selected.mean() * abs(
                correctness[selected].mean() - confidences[selected].mean()
            )
    return float(result)


def _metrics(truth: list[str], probabilities: np.ndarray, classes: list[str]) -> dict:
    predictions = np.asarray(classes)[np.argmax(probabilities, axis=1)]
    binary = label_binarize(truth, classes=classes)
    return {
        "macro_average_precision": round(float(average_precision_score(
            binary, probabilities, average="macro"
        )), 6),
        "macro_ovr_auroc": round(float(roc_auc_score(
            binary, probabilities, average="macro", multi_class="ovr"
        )), 6),
        "macro_f1": round(float(f1_score(truth, predictions, average="macro")), 6),
        "weighted_f1": round(float(f1_score(truth, predictions, average="weighted")), 6),
        "accuracy": round(float(accuracy_score(truth, predictions)), 6),
        "log_loss": round(float(log_loss(truth, probabilities, labels=classes)), 6),
        "expected_calibration_error": round(
            _expected_calibration_error(truth, probabilities, classes), 6
        ),
    }


def _relative_gain(candidate: float, baseline: float) -> float:
    return round(100 * (candidate / baseline - 1), 3) if baseline else 0.0


def _prior_probabilities(labels: list[str], classes: list[str], rows: int) -> np.ndarray:
    counts = Counter(labels)
    distribution = np.asarray([counts[label] + 1 for label in classes], dtype=float)
    distribution /= distribution.sum()
    return np.tile(distribution, (rows, 1))


def _tune_late_fusion(
    truth: list[str], text: np.ndarray, structured: np.ndarray, classes: list[str]
) -> tuple[float, float]:
    candidates = []
    binary = label_binarize(truth, classes=classes)
    for alpha in np.linspace(0, 1, 21):
        probabilities = alpha * text + (1 - alpha) * structured
        score = average_precision_score(binary, probabilities, average="macro")
        candidates.append((float(score), float(alpha)))
    return max(candidates, key=lambda item: (item[0], -abs(item[1] - 0.5)))


def _case_probabilities(
    train_matrix,
    query_matrix,
    train_labels: list[str],
    classes: list[str],
    *,
    neighbors: int,
    chunk_size: int = 256,
) -> tuple[np.ndarray, np.ndarray]:
    """Weighted sparse cosine retrieval without materializing a full similarity matrix."""
    class_index = {label: index for index, label in enumerate(classes)}
    label_indices = np.asarray([class_index[label] for label in train_labels])
    prior = _prior_probabilities(train_labels, classes, 1)[0]
    probabilities = np.empty((query_matrix.shape[0], len(classes)), dtype=float)
    support = np.empty(query_matrix.shape[0], dtype=float)
    train_transpose = train_matrix.transpose().tocsr()
    k = min(neighbors, train_matrix.shape[0])
    for offset in range(0, query_matrix.shape[0], chunk_size):
        block = query_matrix[offset:offset + chunk_size] @ train_transpose
        for local_row in range(block.shape[0]):
            similarities = block.getrow(local_row).toarray().ravel()
            indices = np.argpartition(similarities, -k)[-k:]
            indices = indices[np.argsort(similarities[indices])[::-1]]
            weights = np.square(np.clip(similarities[indices], 0, None))
            counts = 0.25 * prior
            counts = counts.copy()
            for index, weight in zip(indices, weights):
                counts[label_indices[index]] += max(float(weight), 1e-6)
            probabilities[offset + local_row] = counts / counts.sum()
            support[offset + local_row] = float(similarities[indices[0]])
    return probabilities, support


def _rule_tables(train: list[FusionRecord], classes: list[str]):
    tables: dict[str, dict[str, Counter[str]]] = {
        name: defaultdict(Counter)
        for name in ("Issue", "Company", "Product", "Submitted via")
    }
    global_counts = Counter(record.label for record in train)
    for record in train:
        values = {
            "Issue": record.semantic.get("Issue", "<missing>"),
            "Company": record.semantic.get("Company", "<missing>"),
            "Product": record.structured.get("Product", "<missing>"),
            "Submitted via": record.structured.get("Submitted via", "<missing>"),
        }
        for name, value in values.items():
            tables[name][value][record.label] += 1
    return tables, global_counts


def _rule_probabilities(
    records: list[FusionRecord],
    classes: list[str],
    tables,
    global_counts: Counter[str],
) -> tuple[np.ndarray, np.ndarray]:
    global_prior = np.asarray([global_counts[label] + 1 for label in classes], dtype=float)
    global_prior /= global_prior.sum()
    probabilities = []
    support = []
    for record in records:
        values = {
            "Issue": record.semantic.get("Issue", "<missing>"),
            "Company": record.semantic.get("Company", "<missing>"),
            "Product": record.structured.get("Product", "<missing>"),
            "Submitted via": record.structured.get("Submitted via", "<missing>"),
        }
        distributions = []
        counts_seen = []
        for name, value in values.items():
            counts = tables[name].get(value)
            if not counts:
                continue
            total = sum(counts.values())
            distribution = np.asarray(
                [counts[label] + global_prior[index] for index, label in enumerate(classes)],
                dtype=float,
            )
            distributions.append(distribution / distribution.sum())
            counts_seen.append(total)
        if not distributions:
            probabilities.append(global_prior)
            support.append(0.0)
            continue
        weights = np.asarray([min(1.0, math.log1p(value) / math.log(101))
                              for value in counts_seen])
        combined = np.average(np.asarray(distributions), axis=0, weights=weights)
        probabilities.append(combined / combined.sum())
        support.append(float(max(weights)))
    return np.asarray(probabilities), np.asarray(support)


def _agentic_probabilities(
    joint: np.ndarray,
    retrieval: np.ndarray,
    rules: np.ndarray,
    weights: tuple[float, float, float],
) -> tuple[np.ndarray, np.ndarray]:
    arbiter = ConflictAwareArbiter()
    probabilities = []
    disagreements = []
    for local, case, rule in zip(joint, retrieval, rules):
        opinions = tuple(
            EvidenceOpinion(name, tuple(float(value) for value in distribution), weight)
            for name, distribution, weight in (
                ("joint_local", local, weights[0]),
                ("case_memory", case, weights[1]),
                ("field_rules", rule, weights[2]),
            )
            if weight > 0
        )
        decision = arbiter.combine(opinions)
        probabilities.append(decision.probabilities)
        disagreements.append(decision.disagreement)
    return np.asarray(probabilities), np.asarray(disagreements)


def _tune_agent_weights(
    truth: list[str],
    joint: np.ndarray,
    retrieval: np.ndarray,
    rules: np.ndarray,
    classes: list[str],
) -> tuple[tuple[float, float, float], float]:
    binary = label_binarize(truth, classes=classes)
    candidates = []
    for local_weight in (0.4, 0.55, 0.7, 0.85):
        remainder = 1 - local_weight
        for retrieval_share in (0.25, 0.5, 0.75):
            weights = (
                local_weight,
                remainder * retrieval_share,
                remainder * (1 - retrieval_share),
            )
            probabilities, _ = _agentic_probabilities(joint, retrieval, rules, weights)
            score = average_precision_score(binary, probabilities, average="macro")
            candidates.append((float(score), weights))
    score, weights = max(candidates, key=lambda item: item[0])
    return weights, score


def _classwise_probabilities(
    joint: np.ndarray,
    retrieval: np.ndarray,
    rules: np.ndarray,
    reliability_by_agent: np.ndarray,
) -> np.ndarray:
    return np.asarray([
        classwise_linear_pool(
            tuple(tuple(float(value) for value in item) for item in opinions),
            tuple(tuple(float(value) for value in item) for item in reliability_by_agent),
        )
        for opinions in zip(joint, retrieval, rules)
    ])


def _tune_classwise_reliability(
    truth: list[str],
    joint: np.ndarray,
    retrieval: np.ndarray,
    rules: np.ndarray,
    classes: list[str],
) -> tuple[np.ndarray, float]:
    """Coordinate-search an inspectable agent-by-class reliability matrix."""
    binary = label_binarize(truth, classes=classes)
    experts = (joint, retrieval, rules)
    reliability = np.full((len(experts), len(classes)), 1 / len(experts))
    simplex = [
        (left / 10, middle / 10, (10 - left - middle) / 10)
        for left in range(11)
        for middle in range(11 - left)
    ]
    best_probabilities = _classwise_probabilities(
        joint, retrieval, rules, reliability
    )
    best_score = float(average_precision_score(
        binary, best_probabilities, average="macro"
    ))
    for _ in range(2):
        improved = False
        for class_index in range(len(classes)):
            class_best = best_score
            class_weights = reliability[:, class_index].copy()
            for candidate in simplex:
                trial = reliability.copy()
                trial[:, class_index] = candidate
                probabilities = _classwise_probabilities(joint, retrieval, rules, trial)
                score = float(average_precision_score(
                    binary, probabilities, average="macro"
                ))
                if score > class_best + 1e-9:
                    class_best = score
                    class_weights = np.asarray(candidate)
                    best_probabilities = probabilities
            if class_best > best_score + 1e-9:
                reliability[:, class_index] = class_weights
                best_score = class_best
                improved = True
        if not improved:
            break
    return reliability, best_score


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
    train_labels = [record.label for record in train]
    validation_labels = [record.label for record in validation]
    test_labels = [record.label for record in test]

    text_vectorizer = TfidfVectorizer(
        max_features=args.text_features,
        min_df=2,
        ngram_range=(1, 2),
        strip_accents="unicode",
        sublinear_tf=True,
    )
    structured_vectorizer = DictVectorizer(sparse=True)
    train_text = _sparse32(text_vectorizer.fit_transform(record.text for record in train))
    validation_text = _sparse32(text_vectorizer.transform(record.text for record in validation))
    test_text = _sparse32(text_vectorizer.transform(record.text for record in test))
    train_structured = _sparse32(
        structured_vectorizer.fit_transform(record.structured for record in train)
    )
    validation_structured = _sparse32(
        structured_vectorizer.transform(record.structured for record in validation)
    )
    test_structured = _sparse32(
        structured_vectorizer.transform(record.structured for record in test)
    )
    train_joint = _sparse32(hstack([train_text, train_structured]))
    validation_joint = _sparse32(hstack([validation_text, validation_structured]))
    test_joint = _sparse32(hstack([test_text, test_structured]))

    text_model = _classifier(args.seed, 1.0).fit(train_text, train_labels)
    structured_model = _classifier(args.seed).fit(train_structured, train_labels)
    joint_model = _classifier(args.seed).fit(train_joint, train_labels)
    classes = [str(value) for value in text_model.classes_]
    if classes != [str(value) for value in structured_model.classes_] or classes != [
        str(value) for value in joint_model.classes_
    ]:
        raise RuntimeError("baseline class orders differ")

    validation_text_probabilities = text_model.predict_proba(validation_text)
    validation_structured_probabilities = structured_model.predict_proba(
        validation_structured
    )
    validation_joint_probabilities = joint_model.predict_proba(validation_joint)
    test_text_probabilities = text_model.predict_proba(test_text)
    test_structured_probabilities = structured_model.predict_proba(test_structured)
    test_joint_probabilities = joint_model.predict_proba(test_joint)
    late_score, alpha = _tune_late_fusion(
        validation_labels,
        validation_text_probabilities,
        validation_structured_probabilities,
        classes,
    )
    test_late = alpha * test_text_probabilities + (1 - alpha) * test_structured_probabilities

    retrieval_train = normalize(train_text, norm="l2", copy=True)
    validation_retrieval, validation_retrieval_support = _case_probabilities(
        retrieval_train,
        normalize(validation_text, norm="l2", copy=True),
        train_labels,
        classes,
        neighbors=args.neighbors,
    )
    test_retrieval, test_retrieval_support = _case_probabilities(
        retrieval_train,
        normalize(test_text, norm="l2", copy=True),
        train_labels,
        classes,
        neighbors=args.neighbors,
    )
    rule_tables, global_counts = _rule_tables(train, classes)
    validation_rules, validation_rule_support = _rule_probabilities(
        validation, classes, rule_tables, global_counts
    )
    test_rules, test_rule_support = _rule_probabilities(
        test, classes, rule_tables, global_counts
    )
    weights, validation_agentic_score = _tune_agent_weights(
        validation_labels,
        validation_joint_probabilities,
        validation_retrieval,
        validation_rules,
        classes,
    )
    _, validation_disagreement = _agentic_probabilities(
        validation_joint_probabilities, validation_retrieval, validation_rules, weights
    )
    test_agentic, test_disagreement = _agentic_probabilities(
        test_joint_probabilities, test_retrieval, test_rules, weights
    )
    classwise_reliability, validation_classwise_score = _tune_classwise_reliability(
        validation_labels,
        validation_joint_probabilities,
        validation_retrieval,
        validation_rules,
        classes,
    )
    test_classwise = _classwise_probabilities(
        test_joint_probabilities, test_retrieval, test_rules, classwise_reliability
    )

    majority = _prior_probabilities(train_labels, classes, len(test))
    test_metrics = {
        "b0_majority": _metrics(test_labels, majority, classes),
        "b1_tfidf_lr": _metrics(test_labels, test_text_probabilities, classes),
        "b2_structured_lr": _metrics(test_labels, test_structured_probabilities, classes),
        "b3_early_fusion_lr": _metrics(test_labels, test_joint_probabilities, classes),
        "b4_late_fusion": _metrics(test_labels, test_late, classes),
        "a1_case_retrieval": _metrics(test_labels, test_retrieval, classes),
        "a2_field_rule_agent": _metrics(test_labels, test_rules, classes),
        "a3_global_evidence_consensus": _metrics(test_labels, test_agentic, classes),
        "proposed_classwise_evidence_consensus": _metrics(
            test_labels, test_classwise, classes
        ),
    }
    proposed_score = test_metrics["proposed_classwise_evidence_consensus"][
        CFPB_RESPONSE.primary_metric.name
    ]
    required_baselines = (
        "b1_tfidf_lr", "b2_structured_lr", "b3_early_fusion_lr", "b4_late_fusion"
    )
    strongest_name = max(
        required_baselines,
        key=lambda name: test_metrics[name][CFPB_RESPONSE.primary_metric.name],
    )
    return {
        "schema_version": 2,
        "experiment": CFPB_RESPONSE.identity,
        "protocol": CFPB_RESPONSE.manifest(),
        "input": {
            "file": args.input.name,
            "sha256": _sha256(args.input),
            "total_rows": audit.total_rows,
            "usable_rows": audit.narrative_rows,
            "selected_labels": labels,
            "evaluated_rows": len(records),
            "feature_policy": (
                "pre-response narrative, issue, sub-issue, company, product, sub-product, "
                "state, tags, channel, received year/month"
            ),
            "blocked_post_outcome_fields": ["Company public response", "Timely response?"],
        },
        "split": {
            "method": "complete_chronological_60_20_20",
            "train": len(train),
            "validation": len(validation),
            "test": len(test),
            "train_end": train[-1].received.isoformat(),
            "validation_end": validation[-1].received.isoformat(),
            "test_end": test[-1].received.isoformat(),
        },
        "selection": {
            "late_fusion_text_weight": round(alpha, 2),
            "late_fusion_validation_macro_ap": round(late_score, 6),
            "agent_reliability_weights": {
                "joint_local": round(weights[0], 4),
                "case_memory": round(weights[1], 4),
                "field_rules": round(weights[2], 4),
            },
            "agentic_validation_macro_ap": round(validation_agentic_score, 6),
            "classwise_validation_macro_ap": round(validation_classwise_score, 6),
            "classwise_reliability": {
                agent: {
                    label: round(float(classwise_reliability[agent_index, class_index]), 2)
                    for class_index, label in enumerate(classes)
                }
                for agent_index, agent in enumerate(
                    ("joint_local", "case_memory", "field_rules")
                )
            },
        },
        "test_metrics": test_metrics,
        "headline": {
            "primary_metric": CFPB_RESPONSE.primary_metric.name,
            "strongest_required_baseline": strongest_name,
            "relative_gain_vs_b1_pct": _relative_gain(
                proposed_score,
                test_metrics["b1_tfidf_lr"][CFPB_RESPONSE.primary_metric.name],
            ),
            "relative_gain_vs_strongest_baseline_pct": _relative_gain(
                proposed_score,
                test_metrics[strongest_name][CFPB_RESPONSE.primary_metric.name],
            ),
            "thirty_pct_vs_b1_met": _relative_gain(
                proposed_score,
                test_metrics["b1_tfidf_lr"][CFPB_RESPONSE.primary_metric.name],
            ) >= 30,
        },
        "agent_diagnostics": {
            "validation_mean_disagreement": round(float(validation_disagreement.mean()), 6),
            "test_mean_disagreement": round(float(test_disagreement.mean()), 6),
            "validation_mean_retrieval_support": round(
                float(validation_retrieval_support.mean()), 6
            ),
            "test_mean_retrieval_support": round(float(test_retrieval_support.mean()), 6),
            "validation_mean_rule_support": round(float(validation_rule_support.mean()), 6),
            "test_mean_rule_support": round(float(test_rule_support.mean()), 6),
        },
        "environment": {
            "python": platform.python_version(),
            "scikit_learn": sklearn.__version__,
            "platform": platform.platform(),
            "seed": args.seed,
            "wall_seconds": round(time.perf_counter() - started, 3),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--labels", type=int, default=4)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--text-features", type=int, default=50_000)
    parser.add_argument("--min-narratives", type=int, default=1_000)
    parser.add_argument("--min-label-support", type=int, default=200)
    parser.add_argument("--neighbors", type=int, default=15)
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args()
    if min(
        args.labels,
        args.max_rows,
        args.text_features,
        args.min_narratives,
        args.min_label_support,
        args.neighbors,
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
