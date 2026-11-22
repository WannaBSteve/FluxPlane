"""Fixed research protocols for FluxPlane algorithm experiments.

The registry keeps task, baseline and metric choices stable across scripts.  It is
intentionally independent of the runtime service so experiment iteration cannot
silently change the production API.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class MetricSpec:
    name: str
    direction: str
    role: str
    rationale: str

    def __post_init__(self) -> None:
        if self.direction not in {"maximize", "minimize"}:
            raise ValueError("metric direction must be maximize or minimize")
        if self.role not in {"primary", "secondary", "diagnostic"}:
            raise ValueError("metric role must be primary, secondary, or diagnostic")


@dataclass(frozen=True)
class BaselineSpec:
    identity: str
    family: str
    modalities: tuple[str, ...]
    description: str
    comparison_role: str = "required"

    def __post_init__(self) -> None:
        if self.comparison_role not in {"required", "sanity", "upper_bound"}:
            raise ValueError("invalid baseline comparison role")


@dataclass(frozen=True)
class BenchmarkSpec:
    identity: str
    research_role: str
    task: str
    target: str
    split: str
    metrics: tuple[MetricSpec, ...]
    baselines: tuple[BaselineSpec, ...]
    success_criteria: tuple[str, ...]
    public_anchor: str

    def __post_init__(self) -> None:
        metric_names = [item.name for item in self.metrics]
        baseline_names = [item.identity for item in self.baselines]
        if len(metric_names) != len(set(metric_names)):
            raise ValueError(f"duplicate metrics in {self.identity}")
        if len(baseline_names) != len(set(baseline_names)):
            raise ValueError(f"duplicate baselines in {self.identity}")
        if sum(item.role == "primary" for item in self.metrics) != 1:
            raise ValueError(f"{self.identity} must define exactly one primary metric")
        if not any(item.comparison_role == "required" for item in self.baselines):
            raise ValueError(f"{self.identity} requires at least one real baseline")

    @property
    def primary_metric(self) -> MetricSpec:
        return next(item for item in self.metrics if item.role == "primary")

    def manifest(self) -> dict:
        payload = asdict(self)
        payload["primary_metric"] = self.primary_metric.name
        return payload


CFPB_RESPONSE = BenchmarkSpec(
    identity="cfpb_response_agentic_fusion",
    research_role="text-tabular feature fusion and agentic decision quality",
    task=(
        "Predict the company's eventual complaint response from information available before "
        "that response is produced."
    ),
    target="Company response to consumer",
    split="chronological 60/20/20 train/validation/test over all selected records",
    metrics=(
        MetricSpec(
            "macro_average_precision",
            "maximize",
            "primary",
            "Risk-sensitive one-vs-rest ranking quality that does not hide rare response classes.",
        ),
        MetricSpec(
            "macro_ovr_auroc",
            "maximize",
            "secondary",
            "Comparable with the AUROC protocol used by recent text-tabular benchmarks.",
        ),
        MetricSpec(
            "macro_f1",
            "maximize",
            "secondary",
            "Measures the final hard decision equally across response classes.",
        ),
        MetricSpec(
            "expected_calibration_error",
            "minimize",
            "diagnostic",
            "Determines whether confidence is useful for selective LLM escalation.",
        ),
    ),
    baselines=(
        BaselineSpec(
            "b0_majority",
            "constant",
            ("label_prior",),
            "Most frequent training response; sanity check only.",
            "sanity",
        ),
        BaselineSpec(
            "b1_tfidf_lr",
            "linear",
            ("text",),
            "Word 1-2 gram TF-IDF with class-balanced logistic regression.",
        ),
        BaselineSpec(
            "b2_structured_lr",
            "linear",
            ("structured",),
            "One-hot pre-decision fields with class-balanced logistic regression.",
        ),
        BaselineSpec(
            "b3_early_fusion_lr",
            "linear",
            ("text", "structured"),
            "Single classifier over concatenated text and structured features.",
        ),
        BaselineSpec(
            "b4_late_fusion",
            "ensemble",
            ("text", "structured"),
            "Validation-selected probability fusion of the two unimodal agents.",
        ),
        BaselineSpec(
            "b5_structured_catboost",
            "gbdt",
            ("structured",),
            "Native categorical CatBoost over pre-response fields.",
        ),
        BaselineSpec(
            "b6_frozen_e5_catboost",
            "frozen_embedding",
            ("text",),
            "Field-level frozen e5-small-v2 embeddings with CatBoost.",
        ),
        BaselineSpec(
            "b7_joint_frozen_e5_catboost",
            "text_tabular",
            ("text", "structured"),
            "MulTaBench-style joint frozen E5 and categorical CatBoost; strongest required baseline.",
        ),
        BaselineSpec(
            "b8_ling_only",
            "large_model",
            ("text", "structured"),
            "Ling judges every record without retrieved examples.",
            "upper_bound",
        ),
        BaselineSpec(
            "b9_ling_rag",
            "large_model_retrieval",
            ("text", "structured", "retrieval"),
            "Ling judges every record with the same training-only hybrid retrieval context.",
            "upper_bound",
        ),
    ),
    success_criteria=(
        "The proposed method must beat both unimodal baselines and joint frozen E5 plus CatBoost.",
        "Removing text, structured evidence, or retrieval must each be reported.",
        (
            "The 30% headline uses relative macro-F1 gain over B1 and is also "
            "reported against the strongest required baseline."
        ),
        "All methods use the same complete chronological test partition.",
    ),
    public_anchor="MulTaBench additional/extended Consumer Complaints protocol (NeurIPS 2026)",
)


BAF = BenchmarkSpec(
    identity="baf_dynamic_risk",
    research_role="imbalanced fraud risk and temporal adaptation",
    task="Predict fraudulent bank-account applications under temporal and group shifts.",
    target="fraud_bool",
    split="official first-six-month train and later-month test protocol",
    metrics=(
        MetricSpec(
            "tpr_at_5pct_fpr",
            "maximize",
            "primary",
            "Official operating-point metric for the punitive fraud-screening setting.",
        ),
        MetricSpec(
            "average_precision",
            "maximize",
            "secondary",
            "Summarizes performance under the approximately 1.8% fraud prevalence.",
        ),
        MetricSpec(
            "fpr_group_ratio",
            "maximize",
            "diagnostic",
            "Official predictive-equality diagnostic; one is best.",
        ),
    ),
    baselines=(
        BaselineSpec("b1_logistic", "linear", ("structured",), "Balanced logistic regression."),
        BaselineSpec("b2_lightgbm", "gbdt", ("structured",), "Official strong tree baseline."),
        BaselineSpec("b3_catboost", "gbdt", ("structured",), "Categorical-aware strong baseline."),
    ),
    success_criteria=(
        "Beat the strongest GBDT on the official temporal test without degrading group FPR ratio.",
        "Report Base and Variant V separately.",
    ),
    public_anchor="Bank Account Fraud suite (NeurIPS 2022 Datasets and Benchmarks)",
)


PROTOCOLS = {item.identity: item for item in (CFPB_RESPONSE, BAF)}


def protocol(identity: str) -> BenchmarkSpec:
    try:
        return PROTOCOLS[identity]
    except KeyError as exc:
        raise ValueError(f"unknown benchmark protocol: {identity}") from exc
