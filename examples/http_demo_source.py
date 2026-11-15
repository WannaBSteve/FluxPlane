"""Internal HTTP JSON source for the reproducible Compose demo."""

from __future__ import annotations

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

REFUNDS = Path("/data/sources/full_demo/http/refunds.json")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/refunds.json":
            self.send_error(404)
            return
        try:
            rows = json.loads(REFUNDS.read_text(encoding="utf-8"))
            since = parse_qs(parsed.query).get("updated_after", [None])[0]
            if since:
                cutoff = datetime.fromisoformat(since)
                rows = [row for row in rows if datetime.fromisoformat(row["updated_at"]) >= cutoff]
            payload = json.dumps({"data": rows}).encode()
        except (OSError, ValueError, KeyError, TypeError):
            self.send_error(400, "source data is unavailable or invalid")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8091), Handler).serve_forever()
