"""Benchmark BAF temporal fraud detection and unlabeled drift-gated adaptation."""

from __future__ import annotations

import argparse
import json
import platform
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score, roc_curve
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

TARGET = "fraud_bool"
MONTH = "month"
CATEGORICAL = (
    "payment_type",
    "employment_status",
    "housing_status",
    "source",
    "device_os",
)
OPAQUE_FEATURE = re.compile(r"^x\d+$", re.IGNORECASE)


def _read(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        frame = pd.read_csv(path)
        unnamed = [column for column in frame if str(column).startswith("Unnamed:")]
        return frame.drop(columns=unnamed)
    raise ValueError("BAF input must be CSV or Parquet")


def _tpr_at_fpr(truth: np.ndarray, scores: np.ndarray, limit: float = 0.05) -> tuple:
    fpr, tpr, thresholds = roc_curve(truth, scores)
    valid = np.flatnonzero(fpr < limit)
    if not len(valid):
        return 0.0, float("inf")
    best_fpr = fpr[valid].max()
    candidates = valid[fpr[valid] == best_fpr]
    index = candidates[np.argmax(tpr[candidates])]
    return float(tpr[index]), float(thresholds[index])


def _group_fpr_ratio(
    frame: pd.DataFrame, truth: np.ndarray, scores: np.ndarray, threshold: float
) -> dict:
    predictions = scores > threshold
    groups = np.asarray(frame["customer_age"] >= 50)
    rates = {}
    for name, selected in (("younger", ~groups), ("older", groups)):
        negative = selected & (truth == 0)
        rates[name] = float((predictions[negative] == 1).mean()) if negative.any() else 0.0
    maximum = max(rates.values())
    ratio = min(rates.values()) / maximum if maximum else 1.0
    return {"ratio": round(ratio, 6), **{key: round(value, 6) for key, value in rates.items()}}


def _metrics(frame: pd.DataFrame, truth: np.ndarray, scores: np.ndarray) -> dict:
    tpr, threshold = _tpr_at_fpr(truth, scores)
    return {
        "tpr_at_5pct_fpr": round(tpr, 6),
        "average_precision": round(float(average_precision_score(truth, scores)), 6),
        "roc_auc": round(float(roc_auc_score(truth, scores)), 6),
        "threshold": round(threshold, 8),
        "age_fpr": _group_fpr_ratio(frame, truth, scores, threshold),
    }


def _encode_categories(
    train: pd.DataFrame, test: pd.DataFrame, categorical: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = train.copy()
    test = test.copy()
    for field in categorical:
        categories = {value: index for index, value in enumerate(train[field].astype(str).unique())}
        train[field] = train[field].astype(str).map(categories).fillna(-1).astype("int32")
        test[field] = test[field].astype(str).map(categories).fillna(-1).astype("int32")
    return train, test


def _psi(train: pd.Series, test: pd.Series, bins: int = 10) -> float:
    train_values = pd.to_numeric(train, errors="coerce").fillna(-999).to_numpy()
    test_values = pd.to_numeric(test, errors="coerce").fillna(-999).to_numpy()
    edges = np.unique(np.quantile(train_values, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        train_counts = pd.Series(train_values).value_counts(normalize=True)
        test_counts = pd.Series(test_values).value_counts(normalize=True)
        values = train_counts.index.union(test_counts.index)
        expected = train_counts.reindex(values, fill_value=0).to_numpy()
        actual = test_counts.reindex(values, fill_value=0).to_numpy()
    else:
        edges[0], edges[-1] = -np.inf, np.inf
        expected = np.histogram(train_values, bins=edges)[0] / len(train_values)
        actual = np.histogram(test_values, bins=edges)[0] / len(test_values)
    expected = np.clip(expected, 1e-6, None)
    actual = np.clip(actual, 1e-6, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def _drift_scores(train: pd.DataFrame, test: pd.DataFrame, fields: list[str]) -> dict:
    encoded_train, encoded_test = _encode_categories(train, test, [
        field for field in CATEGORICAL if field in fields
    ])
    return {field: _psi(encoded_train[field], encoded_test[field]) for field in fields}


def _logistic(
    train: pd.DataFrame, test: pd.DataFrame, labels: np.ndarray, fields: list[str]
) -> np.ndarray:
    categorical = [field for field in CATEGORICAL if field in fields]
    numeric = [field for field in fields if field not in categorical]
    transform = ColumnTransformer([
        ("categorical", OneHotEncoder(handle_unknown="ignore"), categorical),
        ("numeric", StandardScaler(), numeric),
    ])
    model = make_pipeline(
        transform,
        LogisticRegression(
            class_weight="balanced", max_iter=500, solver="saga"
        ),
    )
    model.fit(train[fields], labels)
    return model.predict_proba(test[fields])[:, 1]


def _lightgbm(
    train: pd.DataFrame,
    test: pd.DataFrame,
    labels: np.ndarray,
    fields: list[str],
    seed: int,
) -> np.ndarray:
    from lightgbm import LGBMClassifier

    categorical = [field for field in CATEGORICAL if field in fields]
    encoded_train, encoded_test = _encode_categories(train[fields], test[fields], categorical)
    model = LGBMClassifier(
        n_estimators=700,
        learning_rate=0.04,
        num_leaves=48,
        max_depth=-1,
        min_child_samples=100,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=2.0,
        random_state=seed,
        n_jobs=-1,
        verbosity=-1,
    )
    model.fit(encoded_train, labels, categorical_feature=categorical)
    return model.predict_proba(encoded_test)[:, 1]


def _catboost(
    train: pd.DataFrame,
    test: pd.DataFrame,
    labels: np.ndarray,
    fields: list[str],
    seed: int,
) -> np.ndarray:
    from catboost import CatBoostClassifier

    categorical = [field for field in CATEGORICAL if field in fields]
    model = CatBoostClassifier(
        iterations=600,
        depth=8,
        learning_rate=0.06,
        loss_function="Logloss",
        random_seed=seed,
        verbose=False,
        allow_writing_files=False,
        thread_count=-1,
    )
    model.fit(train[fields], labels, cat_features=categorical, verbose=False)
    return model.predict_proba(test[fields])[:, 1]


def run_one(path: Path, args: argparse.Namespace) -> dict:
    started = time.perf_counter()
    frame = _read(path)
    if TARGET not in frame or MONTH not in frame:
        raise ValueError(f"{path.name} is missing {TARGET} or {MONTH}")
    train = frame[frame[MONTH] < 6].copy()
    test = frame[frame[MONTH] >= 6].copy()
    train_labels = train.pop(TARGET).to_numpy()
    test_labels = test.pop(TARGET).to_numpy()
    fields = list(train.columns)
    drift = _drift_scores(train, test, fields)
    stable_fields = [field for field in fields if drift[field] <= args.psi_threshold]
    untrusted_fields = [field for field in fields if OPAQUE_FEATURE.fullmatch(field)]
    governed_fields = [field for field in fields if field not in untrusted_fields]
    if not stable_fields:
        raise ValueError("drift gate removed every feature")
    logistic_scores = _logistic(train, test, train_labels, fields)
    lightgbm_scores = _lightgbm(train, test, train_labels, fields, args.seed)
    scores = {
        "b0_onehot_logistic": logistic_scores,
        "b1_lightgbm": lightgbm_scores,
        "b2_catboost": _catboost(train, test, train_labels, fields, args.seed),
        "proposed_unlabeled_drift_gated_lightgbm": _lightgbm(
            train, test, train_labels, stable_fields, args.seed
        ),
        "proposed_provenance_gated_lightgbm": (
            lightgbm_scores
            if governed_fields == fields
            else _lightgbm(train, test, train_labels, governed_fields, args.seed)
        ),
    }
    return {
        "file": path.name,
        "rows": len(frame),
        "fraud_rate": round(float(frame[TARGET].mean()), 8),
        "split": {"train": len(train), "test": len(test), "rule": "month<6 / month>=6"},
        "feature_audit": {
            "psi_threshold": args.psi_threshold,
            "all_features": fields,
            "stable_features": stable_fields,
            "dropped_features": sorted(set(fields) - set(stable_fields)),
            "provenance_policy": "reject anonymous x<number> fields without semantics",
            "untrusted_features": untrusted_fields,
            "psi": {key: round(value, 6) for key, value in sorted(drift.items())},
            "uses_test_labels": False,
        },
        "test_metrics": {
            name: _metrics(test, test_labels, prediction)
            for name, prediction in scores.items()
        },
        "wall_seconds": round(time.perf_counter() - started, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--psi-threshold", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=20261122)
    args = parser.parse_args()
    if args.psi_threshold <= 0:
        parser.error("PSI threshold must be positive")
    report = {
        "schema_version": 1,
        "experiment": "baf_temporal_dynamic_risk",
        "protocol": "official month<6 train, month>=6 test",
        "primary_metric": "tpr_at_5pct_fpr",
        "datasets": [run_one(path, args) for path in args.input],
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "seed": args.seed,
        },
    }
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
