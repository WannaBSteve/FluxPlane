"""API roles use the same permissions as the web console."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from fluxplane.api.app import create_app
from fluxplane.config import Settings


@pytest.fixture
def secured_client(tmp_path: Path):
    config = Settings(
        database_url=f"sqlite:///{tmp_path / 'metadata.sqlite3'}",
        artifact_root=tmp_path / "artifacts",
        source_root=tmp_path / "sources",
        api_key="admin-secret",
        operator_api_key="operator-secret",
        viewer_api_key="viewer-secret",
    )
    with TestClient(create_app(config)) as client:
        yield client


def test_role_keys_gate_mutations_and_allow_reads(secured_client: TestClient) -> None:
    client = secured_client
    keys = {role: {"X-API-Key": f"{role}-secret"}
            for role in ("admin", "operator", "viewer")}
    assert client.get("/api/v1/auth/me").status_code == 401
    assert client.get("/api/v1/runs/unknown/rows").status_code == 401
    assert client.get("/api/v1/auth/me", headers={"X-API-Key": "wrong"}).status_code == 401
    for role, headers in keys.items():
        assert client.get("/api/v1/auth/me", headers=headers).json() == {"role": role}
        assert client.get("/api/v1/datasets", headers=headers).status_code == 200
        assert client.get("/api/v1/events/cursor", headers=headers).status_code == 200
        assert client.get("/api/v1/runs/unknown/rows", headers=headers).status_code == 404

    source = client.post("/api/v1/sources", headers=keys["admin"],
                         json={"name": "files", "kind": "file", "config": {}})
    assert source.status_code == 201
    dataset = client.post("/api/v1/datasets", headers=keys["admin"], json={
        "source_id": source.json()["id"], "name": "orders", "source_object": "orders.csv"
    })
    assert dataset.status_code == 201
    assert client.post(f"/api/v1/datasets/{dataset.json()['id']}/sync",
                       headers=keys["operator"]).status_code == 202

    administrative = [
        ("/sources", {"name": "blocked", "kind": "file", "config": {}}),
        ("/datasets", {"source_id": source.json()["id"], "name": "blocked",
                       "source_object": "orders.csv"}),
        ("/features", {"name": "blocked", "sql": "SELECT 1", "dependencies": [],
                       "entity_keys": []}),
        ("/releases/unknown/restore", None),
    ]
    for path, body in administrative:
        assert client.post("/api/v1" + path, headers=keys["operator"],
                           json=body).status_code == 403
    operational = [
        (f"/datasets/{dataset.json()['id']}/sync", None),
        ("/features/unknown/refresh", None),
        ("/queries", {"sql": "SELECT 1"}),
        ("/workflows", {"name": "blocked", "steps": []}),
        ("/runs/unknown/cancel", None),
        ("/runs/unknown/retry", None),
    ]
    for path, body in administrative + operational:
        assert client.post("/api/v1" + path, headers=keys["viewer"],
                           json=body).status_code == 403


def test_role_keys_must_be_distinct_and_have_an_admin() -> None:
    with pytest.raises(ValueError, match="distinct"):
        Settings(api_key="same", operator_api_key="same")
    with pytest.raises(ValueError, match="administrator API key"):
        Settings(viewer_api_key="viewer-secret")
