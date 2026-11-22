"""Checks for the cached low-confidence LLM router."""

from __future__ import annotations

import json

import httpx
import pytest
from fluxplane.experiments.llm_routing import CachedEvidenceRouter, RoutingItem


def test_router_validates_and_caches_predictions(tmp_path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url == "https://example.test/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer secret"
        body = json.loads(request.content)
        assert body["chat_template_kwargs"] == {"enable_thinking": False}
        assert body["temperature"] == 0
        result = {
            "items": [{
                "id": "1",
                "label": "billing",
                "confidence": 0.8,
                "evidence": ["charged twice"],
                "rationale": "Duplicate charge.",
            }]
        }
        return httpx.Response(200, json={
            "choices": [{"message": {"content": "```json\n" + json.dumps(result) + "\n```"}}]
        })

    router = CachedEvidenceRouter(
        base_url="https://example.test/v1",
        model="test-model",
        api_key="secret",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(handler),
    )
    item = RoutingItem(
        identity="1",
        text="I was charged twice",
        product="card",
        sub_product="credit card",
        local_label="cash advance",
        local_confidence=0.4,
        candidates=("billing", "cash advance"),
    )

    first, first_stats = router.classify([item], workers=2)
    second, second_stats = router.classify([item])

    assert first["1"].label == "billing"
    assert first_stats.api_calls == 1
    assert second["1"].cache_hit is True
    assert second_stats.cache_hits == 1
    assert second_stats.api_calls == 0
    assert calls == 1


def test_router_accepts_base_url_that_already_contains_v1(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "https://example.test/v1/chat/completions"
        return httpx.Response(200, json={
            "choices": [{"message": {"content": '{"items":[["1","yes",0.8]]}'}}]
        })

    router = CachedEvidenceRouter(
        base_url="https://example.test/v1",
        model="test-model",
        api_key="secret",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(handler),
    )
    item = RoutingItem("1", "text", "product", "sub", "", 0.0, ("yes", "no"))
    predictions, _ = router.classify([item])
    assert predictions["1"].label == "yes"


def test_router_flattens_nested_compact_predictions(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        result = {"items": [[["a", "yes", 0.7], ["b", "no", 0.8]]]}
        return httpx.Response(200, json={
            "choices": [{"message": {"content": json.dumps(result)}}]
        })

    router = CachedEvidenceRouter(
        base_url="https://example.test/v1",
        model="test-model",
        api_key="secret",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(handler),
    )
    items = [
        RoutingItem(str(index), "text", "product", "sub", "", 0.0, ("yes", "no"))
        for index in ("a", "b")
    ]
    predictions, _ = router.classify(items, batch_size=2)
    assert {key: value.label for key, value in predictions.items()} == {
        "a": "yes",
        "b": "no",
    }


def test_router_reports_provider_error_without_key_error(tmp_path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={
            "error": {"code": "THROTTLED", "message": "slow down"}
        })

    router = CachedEvidenceRouter(
        base_url="https://example.test/v1",
        model="test-model",
        api_key="secret",
        cache_dir=tmp_path,
        transport=httpx.MockTransport(handler),
    )
    item = RoutingItem("1", "text", "product", "sub", "", 0.0, ("yes", "no"))
    with pytest.raises(RuntimeError, match="provider error THROTTLED: slow down"):
        router.classify([item])
    assert calls == 3


def test_router_sanitizes_links_and_email_from_untrusted_evidence() -> None:
    item = RoutingItem(
        "1",
        "See https://bad.example.com/path and me@example.com",
        "card",
        "credit",
        "yes",
        0.8,
        ("yes", "no"),
        structured_evidence=(("site", "www.example.org/a"),),
        retrieved_cases=("source=identitytheft.gov",),
    )
    prompt = CachedEvidenceRouter._prompt([item])
    assert "bad.example.com" not in prompt
    assert "me@example.com" not in prompt
    assert "www.example.org" not in prompt
    assert "identitytheft.gov" not in prompt
    assert "<redacted-link>" in prompt
    assert "<redacted-email>" in prompt
