"""Browsing and natural-language plans use published data and normal query execution."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from fluxplane.api.app import create_app
from fluxplane.api.exploration import ModelPlan
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.scheduling.jobs import claim


def work(platform, lane):
    with platform.db.session() as session:
        job = claim(session, lane, "exploration-test", 120)
        identity, generation = job.id, job.lease_generation
    return platform.process(identity, generation)


@pytest.fixture
def catalog(tmp_path: Path):
    config = Settings(database_url=f"sqlite:///{tmp_path / 'metadata.db'}",
                      source_root=tmp_path / "sources", artifact_root=tmp_path / "artifacts",
                      api_key="admin", operator_api_key="operator", viewer_api_key="viewer")
    platform = Platform(config)
    platform.initialize()
    (config.source_root / "sales.csv").write_text("category,amount\nA,12\nB,8\nA,7\n")
    source = platform.add_source("sales-file", "file", {})
    dataset = platform.add_dataset(source.id, "sales", "sales.csv")
    platform.queue_sync(dataset.id)
    assert work(platform, "background")["status"] == "succeeded"
    with TestClient(create_app(config), headers={"X-API-Key": "admin"}) as client:
        yield platform, client, dataset


def test_preview_pages_are_version_pinned_and_roles_apply(catalog):
    platform, client, dataset = catalog
    path = f"/api/v1/catalog/datasets/{dataset.id}/preview"
    page = client.get(path, params={"limit": 2}).json()
    assert page["row_count"] == 3 and len(page["rows"]) == 2 and page["has_more"]
    (platform.settings.source_root / "sales.csv").write_text("category,amount\nC,100\n")
    platform.queue_sync(dataset.id)
    work(platform, "background")
    old = client.get(path, params={"offset": 2, "version_id": page["version_id"]}).json()
    assert old["rows"] == [{"category": "A", "amount": 7}]
    assert client.get(path).json()["row_count"] == 1
    assert client.get(path, params={"offset": -1}).status_code == 422
    assert client.get(path, params={"version_id": "missing"}).status_code == 404
    assert client.get(path, headers={"X-API-Key": "viewer"}).status_code == 200
    assert client.get(path, headers={"X-API-Key": "wrong"}).status_code == 401


def test_unconfigured_model_and_viewer_cannot_plan(catalog):
    _, client, _ = catalog
    assert client.get("/api/v1/analysis/status").json()["configured"] is False
    body = {"question": "汇总销售额", "relations": ["sales"]}
    assert client.post("/api/v1/analysis/plan", json=body).status_code == 503
    assert client.post("/api/v1/analysis/plan", json=body,
                       headers={"X-API-Key": "viewer"}).status_code == 403


def mock_plan(monkeypatch, sql, clarification=None):
    async def respond(platform, question, catalog):
        assert catalog[0]["fields"] == [
            {"name": "category", "type": "string"}, {"name": "amount", "type": "int64"}]
        assert "rows" not in catalog[0]
        return ModelPlan(summary="按类别汇总销售额", sql=sql, clarification=clarification)
    monkeypatch.setattr("fluxplane.api.exploration.request_plan", respond)


def test_plan_executes_through_normal_worker_and_rejects_changed_inputs(catalog, monkeypatch):
    platform, client, dataset = catalog
    mock_plan(monkeypatch, "SELECT category, SUM(amount) AS total FROM sales GROUP BY category ORDER BY category")
    response = client.post("/api/v1/analysis/plan", json={"question": "各类别销售额？", "relations": ["sales"]})
    assert response.status_code == 200, response.text
    plan = response.json()
    assert "分组" in plan["operations"] and "计算汇总指标" in plan["operations"]
    assert plan["relations"] == ["sales"]
    job = client.post("/api/v1/queries", json=plan)
    assert job.status_code == 202
    assert work(platform, "query")["status"] == "succeeded"
    result = client.get(f"/api/v1/runs/{job.json()['id']}/rows").json()
    assert result["rows"] == [{"category": "A", "total": "19"}, {"category": "B", "total": "8"}] or result["rows"] == [{"category": "A", "total": 19}, {"category": "B", "total": 8}]
    (platform.settings.source_root / "sales.csv").write_text("category,amount\nA,20\n")
    platform.queue_sync(dataset.id)
    work(platform, "background")
    assert client.post("/api/v1/queries", json=plan).status_code == 400


@pytest.mark.parametrize("sql", ["DELETE FROM sales", "SELECT * FROM hidden",
                                 "SELECT invented FROM sales",
                                 "SELECT * FROM read_csv('/etc/passwd')", "SELECT 1"])
def test_invalid_plans_never_reach_execution(catalog, monkeypatch, sql):
    _, client, _ = catalog
    mock_plan(monkeypatch, sql)
    result = client.post("/api/v1/analysis/plan", json={"question": "帮我分析", "relations": ["sales"]})
    assert result.status_code == 422
    assert not any(j["kind"] == "query" for j in client.get("/api/v1/runs").json())


def test_clarification_has_no_executable_sql(catalog, monkeypatch):
    _, client, _ = catalog
    mock_plan(monkeypatch, "SELECT * FROM sales", "销售额是否需要扣除退款？")
    result = client.post("/api/v1/analysis/plan", json={"question": "净收入？", "relations": ["sales"]}).json()
    assert result["clarification"] and result["sql"] == "" and not result["expected_versions"]


@pytest.mark.parametrize("valid", [True, False])
def test_provider_request_and_errors_do_not_expose_credentials(catalog, monkeypatch, valid):
    import asyncio
    import json

    import httpx
    from fastapi import HTTPException
    from fluxplane.api.exploration import request_plan

    platform, _, _ = catalog
    platform.settings.analysis_base_url = "https://model.example/v1"
    platform.settings.analysis_model = "analysis-test-model"
    platform.settings.analysis_api_key = "private-provider-key"
    original_client = httpx.AsyncClient

    def provider(request):
        assert request.url.path == "/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer private-provider-key"
        sent = json.loads(request.content)
        assert "response_format" not in sent
        context = json.loads(sent["messages"][1]["content"])
        assert set(context["catalog"][0]) == {"name", "fields", "definition"}
        assert "private-provider-key" not in request.content.decode()
        if not valid:
            return httpx.Response(401, text="private-provider-key rejected")
        content = json.dumps({
            "summary": "总销售额", "sql": "SELECT SUM(amount) FROM sales", "assumptions": []
        })
        return httpx.Response(200, json={"choices": [{"message": {
            "content": f"```json\n{content}\n```",
        }}]})

    monkeypatch.setattr("fluxplane.api.exploration.httpx.AsyncClient", lambda **kwargs:
                        original_client(transport=httpx.MockTransport(provider), **kwargs))
    context = [{"name": "sales", "fields": [{"name": "amount", "type": "int64"}],
                "definition": None}]
    if valid:
        assert asyncio.run(request_plan(platform, "总销售额？", context)).sql.startswith("SELECT")
    else:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(request_plan(platform, "总销售额？", context))
        assert exc.value.status_code == 502
        assert "private-provider-key" not in str(exc.value.detail)
