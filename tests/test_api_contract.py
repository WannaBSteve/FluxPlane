"""The console's generated types require concrete OpenAPI response schemas."""

from fluxplane.api.app import create_app


def test_console_read_endpoints_publish_response_schemas() -> None:
    schema = create_app().openapi()
    paths = (
        "/api/v1/auth/me", "/api/v1/system/metrics", "/api/v1/storage/summary",
        "/api/v1/sources", "/api/v1/datasets", "/api/v1/datasets/{dataset_id}/versions",
        "/api/v1/datasets/{dataset_id}/profile", "/api/v1/features",
        "/api/v1/features/{feature_id}/snapshots", "/api/v1/layouts",
        "/api/v1/releases", "/api/v1/workflows", "/api/v1/workflows/{workflow_id}",
        "/api/v1/materializations", "/api/v1/runs", "/api/v1/runs/{job_id}",
        "/api/v1/runs/{job_id}/rows", "/api/v1/events/cursor",
    )
    for path in paths:
        response = schema["paths"][path]["get"]["responses"]["200"]
        assert response["content"]["application/json"]["schema"], path
