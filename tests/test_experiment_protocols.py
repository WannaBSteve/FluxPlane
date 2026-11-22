"""The research contract must remain explicit and internally consistent."""

import pytest
from fluxplane.experiments.protocols import CFPB_RESPONSE, PROTOCOLS, protocol


def test_cfpb_response_protocol_uses_risk_sensitive_primary_metric() -> None:
    assert CFPB_RESPONSE.target == "Company response to consumer"
    assert CFPB_RESPONSE.primary_metric.name == "macro_average_precision"
    assert "complete chronological test partition" in " ".join(
        CFPB_RESPONSE.success_criteria
    )
    assert {
        "b1_tfidf_lr",
        "b7_joint_frozen_e5_catboost",
        "b8_ling_only",
        "b9_ling_rag",
    } <= {
        item.identity for item in CFPB_RESPONSE.baselines
    }


def test_protocol_registry_serializes_and_rejects_unknown_identity() -> None:
    assert set(PROTOCOLS) == {
        "cfpb_response_agentic_fusion",
        "baf_dynamic_risk",
    }
    manifest = protocol("cfpb_response_agentic_fusion").manifest()
    assert manifest["primary_metric"] == "macro_average_precision"
    with pytest.raises(ValueError, match="unknown benchmark"):
        protocol("missing")
