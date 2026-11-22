"""Check an OpenAI-compatible chat endpoint without printing response content or secrets."""

from __future__ import annotations

import json
import time
import uuid

import httpx
from fluxplane.config import settings


def main() -> None:
    config = settings()
    if not (config.analysis_base_url and config.analysis_model and config.analysis_api_key):
        raise SystemExit("set FLUXPLANE_ANALYSIS_BASE_URL, MODEL, and API_KEY first")
    trace_id = uuid.uuid4().hex
    started = time.perf_counter()
    try:
        with httpx.Client(timeout=config.analysis_timeout_seconds) as client:
            response = client.post(
                config.analysis_base_url.rstrip("/") + "/chat/completions",
                headers={
                    "Authorization": f"Bearer {config.analysis_api_key}",
                    "Content-Type": "application/json",
                    "SOFA-TraceId": trace_id,
                },
                json={
                    "model": config.analysis_model,
                    "messages": [{"role": "user", "content": "只回复 OK"}],
                },
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            if not isinstance(content, str):
                raise TypeError("provider returned non-text content")
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as exc:
        status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
        print(json.dumps({"ok": False, "error_type": type(exc).__name__, "status": status}))
        raise SystemExit(1) from None
    print(json.dumps({
        "ok": True,
        "model": config.analysis_model,
        "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        "trace_id": trace_id,
        "response_characters": len(content),
    }))


if __name__ == "__main__":
    main()
