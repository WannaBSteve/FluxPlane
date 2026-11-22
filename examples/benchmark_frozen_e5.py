"""Run frozen field-level E5 and CatBoost baselines on the CFPB response task."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from fluxplane.experiments.fusion import (
    FusionRecord,
    audit_dataset,
    chronological_split,
    load_records,
    validate_audit,
)
from fluxplane.experiments.protocols import CFPB_RESPONSE
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    log_loss,
    roc_auc_score,
)
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import label_binarize, normalize

TEXT_FIELDS = ("narrative", "Issue", "Sub-issue", "Company")
STRUCTURED_FIELDS = (
    "Product",
    "Sub-product",
    "State",
    "Tags",
    "Submitted via",
    "Received year",
    "Received month",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _field_value(record: FusionRecord, field: str) -> str:
    if field == "narrative":
        return record.narrative or record.text
    return record.semantic.get(field, "<missing>")


def _embedding_cache_key(
    records: list[FusionRecord], model_name: str, dimensions: int
) -> str:
    digest = hashlib.sha256()
    digest.update(f"{model_name}:{dimensions}".encode())
    for record in records:
        digest.update(record.identity.encode())
        for field in TEXT_FIELDS:
            digest.update(_field_value(record, field).encode())
    return digest.hexdigest()[:24]


def _encode_fields(
    records: list[FusionRecord],
    *,
    model_name: str,
    cache_dir: Path,
    dimensions: int,
    batch_size: int,
    device: str | None,
) -> dict[str, np.ndarray]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _embedding_cache_key(records, model_name, dimensions)
    cache_path = cache_dir / f"{key}.npz"
    if cache_path.exists():
        with np.load(cache_path) as cached:
            return {field: cached[field] for field in TEXT_FIELDS}

    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name, device=device)
    result = {}
    for field in TEXT_FIELDS:
        values = [_field_value(record, field) for record in records]
        unique_values = list(dict.fromkeys(values))
        encoded = model.encode(
            [f"passage: {value}" for value in unique_values],
            batch_size=batch_size,
            show_progress_bar=True,
            normalize_embeddings=True,
            convert_to_numpy=True,
        ).astype(np.float32)
        lookup = dict(zip(unique_values, encoded))
        result[field] = np.asarray([lookup[value] for value in values], dtype=np.float32)
    np.savez_compressed(cache_path, **result)
    return result


def _pca_fields(
    embeddings: dict[str, np.ndarray],
    train_size: int,
    validation_size: int,
    dimensions: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    train_parts = []
    validation_parts = []
    test_parts = []
    validation_end = train_size + validation_size
    for field in TEXT_FIELDS:
        reducer = PCA(n_components=dimensions, random_state=seed)
        train_parts.append(reducer.fit_transform(embeddings[field][:train_size]))
        validation_parts.append(
            reducer.transform(embeddings[field][train_size:validation_end])
        )
        test_parts.append(reducer.transform(embeddings[field][validation_end:]))
    return (
        np.hstack(train_parts).astype(np.float32),
        np.hstack(validation_parts).astype(np.float32),
        np.hstack(test_parts).astype(np.float32),
    )


def _structured_frame(records: list[FusionRecord]):
    import pandas as pd

    return pd.DataFrame({
        field: [record.structured.get(field, "<missing>") for record in records]
        for field in STRUCTURED_FIELDS
    })


def _embedding_frame(values: np.ndarray):
    import pandas as pd

    return pd.DataFrame(values, columns=[f"e5_{index}" for index in range(values.shape[1])])


def _joint_frame(records: list[FusionRecord], embeddings: np.ndarray):
    import pandas as pd

    return pd.concat([
        _structured_frame(records).reset_index(drop=True),
        _embedding_frame(embeddings),
    ], axis=1)


def _catboost(seed: int):
    from catboost import CatBoostClassifier

    return CatBoostClassifier(
        iterations=700,
        depth=7,
        learning_rate=0.05,
        loss_function="MultiClass",
        auto_class_weights="Balanced",
        random_seed=seed,
        l2_leaf_reg=5,
        random_strength=0.5,
        verbose=False,
        allow_writing_files=False,
        thread_count=-1,
    )


def _fit_catboost(
    model,
    train_frame,
    train_labels: list[str],
    validation_frame,
    validation_labels: list[str],
    categorical: list[str],
):
    return model.fit(
        train_frame,
        train_labels,
        cat_features=categorical,
        eval_set=(validation_frame, validation_labels),
        early_stopping_rounds=75,
        verbose=False,
    )


def _align(probabilities: np.ndarray, source: list[str], target: list[str]) -> np.ndarray:
    index = {label: position for position, label in enumerate(source)}
    return probabilities[:, [index[label] for label in target]]


def _rule_tables(train: list[FusionRecord]):
    fields = ("Issue", "Company", "Product", "Submitted via")
    tables = {field: defaultdict(Counter) for field in fields}
    global_counts = Counter(record.label for record in train)
    for record in train:
        values = {
            "Issue": record.semantic.get("Issue", "<missing>"),
            "Company": record.semantic.get("Company", "<missing>"),
            "Product": record.structured.get("Product", "<missing>"),
            "Submitted via": record.structured.get("Submitted via", "<missing>"),
        }
        for field, value in values.items():
            tables[field][value][record.label] += 1
    return tables, global_counts


def _rule_probabilities(
    records: list[FusionRecord], classes: list[str], tables, global_counts: Counter[str]
) -> np.ndarray:
    prior = np.asarray([global_counts[label] + 1 for label in classes], dtype=float)
    prior /= prior.sum()
    result = []
    for record in records:
        values = {
            "Issue": record.semantic.get("Issue", "<missing>"),
            "Company": record.semantic.get("Company", "<missing>"),
            "Product": record.structured.get("Product", "<missing>"),
            "Submitted via": record.structured.get("Submitted via", "<missing>"),
        }
        distributions = []
        weights = []
        for field, value in values.items():
            counts = tables[field].get(value)
            if not counts:
                continue
            total = sum(counts.values())
            distribution = np.asarray(
                [counts[label] + prior[index] for index, label in enumerate(classes)]
            )
            distributions.append(distribution / distribution.sum())
            weights.append(min(1.0, np.log1p(total) / np.log(101)))
        result.append(
            np.average(distributions, axis=0, weights=weights) if distributions else prior
        )
    return np.asarray(result)


def _retrieval_probabilities(
    train_embeddings: np.ndarray,
    query_embeddings: np.ndarray,
    train_labels: list[str],
    classes: list[str],
    neighbors: int,
) -> np.ndarray:
    class_index = {label: index for index, label in enumerate(classes)}
    label_indices = np.asarray([class_index[label] for label in train_labels])
    prior = np.asarray([train_labels.count(label) + 1 for label in classes], dtype=float)
    prior /= prior.sum()
    model = NearestNeighbors(
        n_neighbors=min(neighbors, len(train_labels)), metric="cosine", n_jobs=-1
    ).fit(normalize(train_embeddings))
    distances, indices = model.kneighbors(normalize(query_embeddings))
    similarities = np.clip(1 - distances, 0, None) ** 2
    result = []
    for row_indices, row_weights in zip(indices, similarities):
        counts = 0.25 * prior
        for index, weight in zip(row_indices, row_weights):
            counts[label_indices[index]] += max(float(weight), 1e-6)
        result.append(counts / counts.sum())
    return np.asarray(result)


def _hybrid_retrieval_probabilities(
    train_lexical,
    query_lexical,
    train_semantic: np.ndarray,
    query_semantic: np.ndarray,
    train: list[FusionRecord],
    queries: list[FusionRecord],
    classes: list[str],
    *,
    neighbors: int,
    chunk_size: int = 256,
) -> tuple[np.ndarray, np.ndarray]:
    """Retrieve cases with lexical, semantic, and structured agreement."""
    class_index = {label: index for index, label in enumerate(classes)}
    label_indices = np.asarray([class_index[record.label] for record in train])
    prior = np.asarray([
        sum(record.label == label for record in train) + 1 for label in classes
    ], dtype=float)
    prior /= prior.sum()
    probabilities = np.empty((query_lexical.shape[0], len(classes)), dtype=float)
    support = np.empty(query_lexical.shape[0], dtype=float)
    train_transpose = train_lexical.transpose().tocsr()
    normalized_train_semantic = normalize(train_semantic)
    normalized_query_semantic = normalize(query_semantic)
    train_issue = np.asarray([
        record.semantic.get("Issue", "<missing>") for record in train
    ])
    train_product = np.asarray([
        record.structured.get("Product", "<missing>") for record in train
    ])
    train_company = np.asarray([
        record.semantic.get("Company", "<missing>") for record in train
    ])
    k = min(neighbors, train_lexical.shape[0])
    for offset in range(0, query_lexical.shape[0], chunk_size):
        lexical_block = query_lexical[offset:offset + chunk_size] @ train_transpose
        semantic_block = (
            normalized_query_semantic[offset:offset + chunk_size]
            @ normalized_train_semantic.T
        )
        for local_row in range(lexical_block.shape[0]):
            query = queries[offset + local_row]
            lexical = lexical_block.getrow(local_row).toarray().ravel()
            semantic = np.clip(np.asarray(semantic_block[local_row]).ravel(), 0, 1)
            similarities = (
                0.5 * lexical
                + 0.3 * semantic
                + 0.1 * (train_issue == query.semantic.get("Issue", "<missing>"))
                + 0.07 * (
                    train_product == query.structured.get("Product", "<missing>")
                )
                + 0.03 * (
                    train_company == query.semantic.get("Company", "<missing>")
                )
            )
            indices = np.argpartition(similarities, -k)[-k:]
            indices = indices[np.argsort(similarities[indices])[::-1]]
            weights = np.square(np.clip(similarities[indices], 0, None))
            counts = 0.25 * prior
            for index, weight in zip(indices, weights):
                counts[label_indices[index]] += max(float(weight), 1e-6)
            probabilities[offset + local_row] = counts / counts.sum()
            support[offset + local_row] = float(
                np.clip(similarities[indices[0]], 0, 1)
            )
    return probabilities, support


def _classwise_pool(experts: tuple[np.ndarray, ...], reliability: np.ndarray) -> np.ndarray:
    stacked = np.stack(experts, axis=0)
    scores = (stacked * reliability[:, None, :]).sum(axis=0) / reliability.sum(axis=0)
    return scores / scores.sum(axis=1, keepdims=True)


def _tune_reliability(
    truth: list[str], experts: tuple[np.ndarray, ...], classes: list[str]
) -> tuple[np.ndarray, float]:
    binary = label_binarize(truth, classes=classes)
    reliability = np.full((len(experts), len(classes)), 1 / len(experts))
    simplex = [
        (left / 10, middle / 10, (10 - left - middle) / 10)
        for left in range(11)
        for middle in range(11 - left)
    ]
    probabilities = _classwise_pool(experts, reliability)
    best_score = float(average_precision_score(binary, probabilities, average="macro"))
    for _ in range(2):
        improved = False
        for class_index in range(len(classes)):
            class_best = best_score
            class_weights = reliability[:, class_index].copy()
            for candidate in simplex:
                trial = reliability.copy()
                trial[:, class_index] = candidate
                score = float(average_precision_score(
                    binary, _classwise_pool(experts, trial), average="macro"
                ))
                if score > class_best + 1e-9:
                    class_best = score
                    class_weights = np.asarray(candidate)
            if class_best > best_score + 1e-9:
                reliability[:, class_index] = class_weights
                best_score = class_best
                improved = True
        if not improved:
            break
    return reliability, best_score


def _tune_decision_bias(
    truth: list[str], probabilities: np.ndarray, classes: list[str]
) -> tuple[np.ndarray, float]:
    truth_array = np.asarray(truth)
    biases = np.ones(len(classes))
    best = float(f1_score(
        truth_array, np.asarray(classes)[np.argmax(probabilities, axis=1)], average="macro"
    ))
    candidates = np.geomspace(0.2, 5.0, 21)
    for _ in range(3):
        improved = False
        for class_index in range(len(classes)):
            class_best = best
            class_bias = biases[class_index]
            for candidate in candidates:
                trial = biases.copy()
                trial[class_index] = candidate
                predictions = np.asarray(classes)[np.argmax(probabilities * trial, axis=1)]
                score = float(f1_score(truth_array, predictions, average="macro"))
                if score > class_best + 1e-9:
                    class_best = score
                    class_bias = float(candidate)
            if class_best > best + 1e-9:
                biases[class_index] = class_bias
                best = class_best
                improved = True
        if not improved:
            break
    return biases, best


def _metrics(
    truth: list[str],
    probabilities: np.ndarray,
    classes: list[str],
    decision_bias: np.ndarray | None = None,
) -> dict:
    scores = probabilities if decision_bias is None else probabilities * decision_bias
    predictions = np.asarray(classes)[np.argmax(scores, axis=1)]
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
    }


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
    embeddings = _encode_fields(
        records,
        model_name=args.model,
        cache_dir=args.cache_dir,
        dimensions=args.pca_dimensions,
        batch_size=args.batch_size,
        device=args.device,
    )
    train_e5, validation_e5, test_e5 = _pca_fields(
        embeddings,
        len(train),
        len(validation),
        args.pca_dimensions,
        args.seed,
    )

    train_structured = _structured_frame(train)
    validation_structured = _structured_frame(validation)
    test_structured = _structured_frame(test)
    structured_model = _fit_catboost(
        _catboost(args.seed),
        train_structured,
        train_labels,
        validation_structured,
        validation_labels,
        list(STRUCTURED_FIELDS),
    )
    text_model = _fit_catboost(
        _catboost(args.seed),
        _embedding_frame(train_e5),
        train_labels,
        _embedding_frame(validation_e5),
        validation_labels,
        [],
    )
    train_joint = _joint_frame(train, train_e5)
    validation_joint_frame = _joint_frame(validation, validation_e5)
    test_joint_frame = _joint_frame(test, test_e5)
    joint_model = _fit_catboost(
        _catboost(args.seed),
        train_joint,
        train_labels,
        validation_joint_frame,
        validation_labels,
        list(STRUCTURED_FIELDS),
    )
    classes = sorted(labels)

    def probabilities(model, frame) -> np.ndarray:
        return _align(
            model.predict_proba(frame), [str(value) for value in model.classes_], classes
        )

    validation_structured_probabilities = probabilities(
        structured_model, validation_structured
    )
    validation_text_probabilities = probabilities(
        text_model, _embedding_frame(validation_e5)
    )
    validation_joint = probabilities(joint_model, validation_joint_frame)
    test_structured_probabilities = probabilities(structured_model, test_structured)
    test_text_probabilities = probabilities(text_model, _embedding_frame(test_e5))
    test_joint = probabilities(joint_model, test_joint_frame)
    narrative_width = args.pca_dimensions
    validation_retrieval = _retrieval_probabilities(
        train_e5[:, :narrative_width],
        validation_e5[:, :narrative_width],
        train_labels,
        classes,
        args.neighbors,
    )
    test_retrieval = _retrieval_probabilities(
        train_e5[:, :narrative_width],
        test_e5[:, :narrative_width],
        train_labels,
        classes,
        args.neighbors,
    )
    lexical_vectorizer = TfidfVectorizer(
        max_features=args.text_features,
        min_df=2,
        ngram_range=(1, 2),
        strip_accents="unicode",
        sublinear_tf=True,
    )
    train_lexical = normalize(
        lexical_vectorizer.fit_transform(record.text for record in train).astype(
            np.float32
        )
    )
    validation_lexical = normalize(
        lexical_vectorizer.transform(record.text for record in validation).astype(
            np.float32
        )
    )
    test_lexical = normalize(
        lexical_vectorizer.transform(record.text for record in test).astype(np.float32)
    )
    validation_hybrid_retrieval, validation_hybrid_support = (
        _hybrid_retrieval_probabilities(
            train_lexical,
            validation_lexical,
            train_e5[:, :narrative_width],
            validation_e5[:, :narrative_width],
            train,
            validation,
            classes,
            neighbors=args.neighbors,
        )
    )
    test_hybrid_retrieval, test_hybrid_support = _hybrid_retrieval_probabilities(
        train_lexical,
        test_lexical,
        train_e5[:, :narrative_width],
        test_e5[:, :narrative_width],
        train,
        test,
        classes,
        neighbors=args.neighbors,
    )
    tables, global_counts = _rule_tables(train)
    validation_rules = _rule_probabilities(validation, classes, tables, global_counts)
    test_rules = _rule_probabilities(test, classes, tables, global_counts)
    reliability, validation_macro_ap = _tune_reliability(
        validation_labels,
        (validation_joint, validation_retrieval, validation_rules),
        classes,
    )
    validation_proposed = _classwise_pool(
        (validation_joint, validation_retrieval, validation_rules), reliability
    )
    test_proposed = _classwise_pool(
        (test_joint, test_retrieval, test_rules), reliability
    )
    decision_bias, validation_macro_f1 = _tune_decision_bias(
        validation_labels, validation_proposed, classes
    )
    core_reliability, validation_core_macro_ap = _tune_reliability(
        validation_labels,
        (
            validation_text_probabilities,
            validation_structured_probabilities,
            validation_joint,
        ),
        classes,
    )
    validation_core = _classwise_pool(
        (
            validation_text_probabilities,
            validation_structured_probabilities,
            validation_joint,
        ),
        core_reliability,
    )
    test_core = _classwise_pool(
        (test_text_probabilities, test_structured_probabilities, test_joint),
        core_reliability,
    )
    evidence_reliability, validation_hierarchical_macro_ap = _tune_reliability(
        validation_labels,
        (validation_core, validation_retrieval, validation_rules),
        classes,
    )
    validation_hierarchical = _classwise_pool(
        (validation_core, validation_retrieval, validation_rules),
        evidence_reliability,
    )
    test_hierarchical = _classwise_pool(
        (test_core, test_retrieval, test_rules), evidence_reliability
    )
    hierarchical_bias, validation_hierarchical_macro_f1 = _tune_decision_bias(
        validation_labels, validation_hierarchical, classes
    )
    hybrid_reliability, validation_hybrid_macro_ap = _tune_reliability(
        validation_labels,
        (validation_joint, validation_hybrid_retrieval, validation_rules),
        classes,
    )
    validation_hybrid_agentic = _classwise_pool(
        (validation_joint, validation_hybrid_retrieval, validation_rules),
        hybrid_reliability,
    )
    test_hybrid_agentic = _classwise_pool(
        (test_joint, test_hybrid_retrieval, test_rules), hybrid_reliability
    )
    hybrid_bias, validation_hybrid_macro_f1 = _tune_decision_bias(
        validation_labels, validation_hybrid_agentic, classes
    )
    candidate_agent_sets = {
        "semantic_joint_rules": (
            (validation_text_probabilities, validation_joint, validation_rules),
            (test_text_probabilities, test_joint, test_rules),
            ("semantic_e5", "joint_e5", "rules"),
        ),
        "semantic_cases_rules": (
            (validation_text_probabilities, validation_retrieval, validation_rules),
            (test_text_probabilities, test_retrieval, test_rules),
            ("semantic_e5", "semantic_cases", "rules"),
        ),
        "semantic_joint_cases": (
            (validation_text_probabilities, validation_joint, validation_retrieval),
            (test_text_probabilities, test_joint, test_retrieval),
            ("semantic_e5", "joint_e5", "semantic_cases"),
        ),
        "structured_semantic_rules": (
            (
                validation_structured_probabilities,
                validation_text_probabilities,
                validation_rules,
            ),
            (test_structured_probabilities, test_text_probabilities, test_rules),
            ("structured", "semantic_e5", "rules"),
        ),
    }
    candidate_agentic = {}
    candidate_metrics = {}
    candidate_validation_probabilities = {}
    candidate_test_probabilities = {}
    for name, (validation_experts, test_experts, agent_names) in (
        candidate_agent_sets.items()
    ):
        candidate_reliability, candidate_validation_ap = _tune_reliability(
            validation_labels, validation_experts, classes
        )
        candidate_validation = _classwise_pool(
            validation_experts, candidate_reliability
        )
        candidate_test = _classwise_pool(test_experts, candidate_reliability)
        candidate_bias, candidate_validation_f1 = _tune_decision_bias(
            validation_labels, candidate_validation, classes
        )
        candidate_agentic[name] = {
            "agents": list(agent_names),
            "classwise_reliability": {
                agent: {
                    label: round(
                        float(candidate_reliability[agent_index, class_index]), 2
                    )
                    for class_index, label in enumerate(classes)
                }
                for agent_index, agent in enumerate(agent_names)
            },
            "validation_macro_ap": round(candidate_validation_ap, 6),
            "validation_macro_f1_after_bias": round(candidate_validation_f1, 6),
            "decision_bias": {
                label: round(float(candidate_bias[index]), 4)
                for index, label in enumerate(classes)
            },
        }
        candidate_metrics[f"agent_set_{name}"] = _metrics(
            test_labels, candidate_test, classes, candidate_bias
        )
        candidate_validation_probabilities[name] = candidate_validation
        candidate_test_probabilities[name] = candidate_test
    selected_agent_set = max(
        candidate_agentic,
        key=lambda name: candidate_agentic[name]["validation_macro_ap"],
    )
    binary_validation = label_binarize(validation_labels, classes=classes)
    blended_agentic = {}
    blended_metrics = {}
    for name in candidate_agent_sets:
        best_alpha = 0.0
        best_validation_ap = validation_macro_ap
        best_validation = validation_proposed
        for alpha in np.linspace(0, 1, 21):
            blended_validation = (
                (1 - alpha) * validation_proposed
                + alpha * candidate_validation_probabilities[name]
            )
            score = float(average_precision_score(
                binary_validation, blended_validation, average="macro"
            ))
            if score > best_validation_ap + 1e-9:
                best_alpha = float(alpha)
                best_validation_ap = score
                best_validation = blended_validation
        blended_test = (
            (1 - best_alpha) * test_proposed
            + best_alpha * candidate_test_probabilities[name]
        )
        blended_bias, blended_validation_f1 = _tune_decision_bias(
            validation_labels, best_validation, classes
        )
        blended_agentic[name] = {
            "candidate_weight": round(best_alpha, 2),
            "base_weight": round(1 - best_alpha, 2),
            "validation_macro_ap": round(best_validation_ap, 6),
            "validation_macro_f1_after_bias": round(blended_validation_f1, 6),
            "decision_bias": {
                label: round(float(blended_bias[index]), 4)
                for index, label in enumerate(classes)
            },
        }
        blended_metrics[f"blend_base_with_{name}"] = _metrics(
            test_labels, blended_test, classes, blended_bias
        )
    selected_blend = max(
        blended_agentic,
        key=lambda name: blended_agentic[name]["validation_macro_ap"],
    )
    metrics = {
        "b1_structured_catboost": _metrics(
            test_labels, test_structured_probabilities, classes
        ),
        "b2_frozen_e5_catboost": _metrics(test_labels, test_text_probabilities, classes),
        "b3_joint_frozen_e5_catboost": _metrics(test_labels, test_joint, classes),
        "a1_semantic_case_retrieval": _metrics(test_labels, test_retrieval, classes),
        "a1b_hybrid_case_retrieval": _metrics(
            test_labels, test_hybrid_retrieval, classes
        ),
        "a2_field_rule_agent": _metrics(test_labels, test_rules, classes),
        "proposed_classwise_e5_consensus": _metrics(
            test_labels, test_proposed, classes, decision_bias
        ),
        "proposed_hierarchical_agentic_consensus": _metrics(
            test_labels, test_hierarchical, classes, hierarchical_bias
        ),
        "proposed_hybrid_case_consensus": _metrics(
            test_labels, test_hybrid_agentic, classes, hybrid_bias
        ),
        **candidate_metrics,
        **blended_metrics,
    }
    primary = CFPB_RESPONSE.primary_metric.name
    required = (
        "b1_structured_catboost",
        "b2_frozen_e5_catboost",
        "b3_joint_frozen_e5_catboost",
    )
    strongest = max(required, key=lambda name: metrics[name][primary])
    proposed_name = "proposed_hybrid_case_consensus"
    proposed = metrics[proposed_name][primary]
    return {
        "schema_version": 1,
        "experiment": "cfpb_response_frozen_e5",
        "track": "pre_response_business_track",
        "input": {
            "file": args.input.name,
            "sha256": _sha256(args.input),
            "total_rows": audit.total_rows,
            "usable_rows": audit.narrative_rows,
            "evaluated_rows": len(records),
            "selected_labels": labels,
            "blocked_post_outcome_fields": ["Company public response", "Timely response?"],
        },
        "split": {
            "method": "complete_chronological_60_20_20",
            "train": len(train),
            "validation": len(validation),
            "test": len(test),
        },
        "representation": {
            "model": args.model,
            "fine_tuned": False,
            "field_level_embeddings": list(TEXT_FIELDS),
            "pca_dimensions_per_field": args.pca_dimensions,
            "structured_fields": list(STRUCTURED_FIELDS),
        },
        "selection": {
            "classwise_reliability": {
                agent: {
                    label: round(float(reliability[agent_index, class_index]), 2)
                    for class_index, label in enumerate(classes)
                }
                for agent_index, agent in enumerate(("joint_e5", "semantic_cases", "rules"))
            },
            "validation_macro_ap": round(validation_macro_ap, 6),
            "decision_bias": {
                label: round(float(decision_bias[index]), 4)
                for index, label in enumerate(classes)
            },
            "validation_macro_f1_after_bias": round(validation_macro_f1, 6),
            "hierarchical_agentic": {
                "core_classwise_reliability": {
                    agent: {
                        label: round(float(core_reliability[agent_index, class_index]), 2)
                        for class_index, label in enumerate(classes)
                    }
                    for agent_index, agent in enumerate(
                        ("semantic_e5", "structured", "joint_e5")
                    )
                },
                "evidence_classwise_reliability": {
                    agent: {
                        label: round(
                            float(evidence_reliability[agent_index, class_index]), 2
                        )
                        for class_index, label in enumerate(classes)
                    }
                    for agent_index, agent in enumerate(
                        ("core_debate", "semantic_cases", "rules")
                    )
                },
                "validation_core_macro_ap": round(validation_core_macro_ap, 6),
                "validation_macro_ap": round(validation_hierarchical_macro_ap, 6),
                "decision_bias": {
                    label: round(float(hierarchical_bias[index]), 4)
                    for index, label in enumerate(classes)
                },
                "validation_macro_f1_after_bias": round(
                    validation_hierarchical_macro_f1, 6
                ),
            },
            "hybrid_case_agentic": {
                "retrieval_weights": {
                    "lexical": 0.5,
                    "semantic": 0.3,
                    "issue_exact": 0.1,
                    "product_exact": 0.07,
                    "company_exact": 0.03,
                },
                "mean_validation_support": round(
                    float(validation_hybrid_support.mean()), 6
                ),
                "mean_test_support": round(float(test_hybrid_support.mean()), 6),
                "classwise_reliability": {
                    agent: {
                        label: round(
                            float(hybrid_reliability[agent_index, class_index]), 2
                        )
                        for class_index, label in enumerate(classes)
                    }
                    for agent_index, agent in enumerate(
                        ("joint_e5", "hybrid_cases", "rules")
                    )
                },
                "validation_macro_ap": round(validation_hybrid_macro_ap, 6),
                "decision_bias": {
                    label: round(float(hybrid_bias[index]), 4)
                    for index, label in enumerate(classes)
                },
                "validation_macro_f1_after_bias": round(
                    validation_hybrid_macro_f1, 6
                ),
            },
            "fixed_agent_set_ablation": candidate_agentic,
            "selected_agent_set_by_validation_macro_ap": selected_agent_set,
            "conservative_blend_ablation": blended_agentic,
            "selected_blend_by_validation_macro_ap": selected_blend,
        },
        "test_metrics": metrics,
        "headline": {
            "primary_metric": primary,
            "strongest_frozen_baseline": strongest,
            "selected_proposed": proposed_name,
            "relative_gain_vs_strongest_pct": round(
                100 * (proposed / metrics[strongest][primary] - 1), 3
            ),
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "seed": args.seed,
            "wall_seconds": round(time.perf_counter() - started, 3),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--model", default="intfloat/e5-small-v2")
    parser.add_argument("--cache-dir", type=Path, default=Path("runtime/e5-cache"))
    parser.add_argument("--device")
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--pca-dimensions", type=int, default=30)
    parser.add_argument("--neighbors", type=int, default=15)
    parser.add_argument("--text-features", type=int, default=50_000)
    parser.add_argument("--labels", type=int, default=4)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--min-narratives", type=int, default=1_000)
    parser.add_argument("--min-label-support", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args()
    if min(
        args.batch_size,
        args.pca_dimensions,
        args.neighbors,
        args.text_features,
        args.labels,
        args.max_rows,
        args.min_narratives,
        args.min_label_support,
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
