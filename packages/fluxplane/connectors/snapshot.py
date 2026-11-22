"""Read supported sources into Arrow tables with an explicit source fingerprint."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
import pyarrow as pa
import pyarrow.json as pajson
import pyarrow.parquet as pq
from pyarrow import csv
from sqlalchemy import create_engine, text

from fluxplane.config import Settings

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SourceSnapshot:
    table: pa.Table
    fingerprint: str
    source_cursor: dict | None = None
    mode: str = "snapshot"


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_file_state(root: Path, relative: str) -> tuple[int, int, str] | None:
    """Return a file identity only if size and mtime remain fixed while hashing."""
    path = _source_file(root, relative)
    before = path.stat()
    digest = _hash_file(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        return None
    return after.st_size, after.st_mtime_ns, digest


def _source_file(root: Path, relative: str) -> Path:
    path = (root.resolve() / relative).resolve(strict=True)
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise ValueError("source path is outside the configured source directory")
    return path


def _rows_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({"_empty": pa.array([], type=pa.string())})
    return pa.Table.from_pylist(rows)


def _sqlite_snapshot(path: Path, table_name: str) -> SourceSnapshot:
    if not IDENTIFIER.fullmatch(table_name):
        raise ValueError("invalid source table name")
    uri = f"{path.as_uri()}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        con.execute("PRAGMA query_only=ON")
        cursor = con.execute(f'SELECT * FROM "{table_name}"')
        names = [column[0] for column in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
    finally:
        con.close()
    table = _rows_table(rows)
    # The database file hash misses uncheckpointed WAL changes.
    digest = hashlib.sha256(repr(table.to_pylist()).encode()).hexdigest()
    return SourceSnapshot(table, digest)


def _postgres_engine(env_name: str):
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", env_name):
        raise ValueError("invalid credential reference")
    dsn = os.environ.get(env_name)
    if not dsn:
        raise ValueError("source credential is unavailable")
    return create_engine(dsn)


def _postgres_snapshot(env_name: str, table_name: str, config: dict) -> SourceSnapshot:
    if not IDENTIFIER.fullmatch(table_name):
        raise ValueError("invalid source table name")
    engine = _postgres_engine(env_name)
    try:
        with engine.connect() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ"))
            result = connection.execute(text(f'SELECT * FROM "{table_name}"'))
            names = list(result.keys())
            rows = [dict(zip(names, row)) for row in result.fetchall()]
            connection.rollback()
    finally:
        engine.dispose()
    cursor = _postgres_cursor(rows, config) if config.get("updated_at") else None
    if config.get("deleted_column"):
        rows = [row for row in rows if not row[config["deleted_column"]]]
    table = _rows_table(rows)
    digest = hashlib.sha256(repr(table.to_pylist()).encode()).hexdigest()
    return SourceSnapshot(table, digest, cursor)


def _postgres_cursor(rows: list[dict], config: dict) -> dict | None:
    values = [row[config["updated_at"]] for row in rows]
    if not values:
        return None
    if any(not isinstance(value, datetime) for value in values):
        raise ValueError("PostgreSQL update marker must be a timestamp")
    return {"watermark": max(values).isoformat()}


def read_postgres_changes(config: dict, table_name: str, previous: pa.Table,
                          cursor: dict) -> SourceSnapshot:
    """Merge timestamp-marked rows into a prior immutable snapshot."""
    if not IDENTIFIER.fullmatch(table_name):
        raise ValueError("invalid source table name")
    key, marker = config["primary_key"], config["updated_at"]
    if any(not IDENTIFIER.fullmatch(name) for name in (key, marker)):
        raise ValueError("invalid PostgreSQL cursor column")
    watermark = datetime.fromisoformat(cursor["watermark"])
    cutoff = watermark - timedelta(seconds=60)
    engine = _postgres_engine(str(config["database_url_env"]))
    try:
        with engine.connect() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY, ISOLATION LEVEL REPEATABLE READ"))
            result = connection.execute(text(
                f'SELECT * FROM "{table_name}" WHERE "{marker}" >= :cutoff '
                f'ORDER BY "{marker}", "{key}"'
            ), {"cutoff": cutoff})
            names = list(result.keys())
            changes = [dict(zip(names, row)) for row in result.fetchall()]
            connection.rollback()
    finally:
        engine.dispose()
    if set(names) != set(previous.column_names):
        return _postgres_snapshot(str(config["database_url_env"]), table_name, config)
    if any(row.get(key) is None for row in changes):
        raise ValueError("PostgreSQL primary key cannot be null")
    by_key = {row[key]: row for row in previous.to_pylist()}
    if len(by_key) != previous.num_rows:
        raise ValueError("PostgreSQL primary key is not unique")
    deleted = config.get("deleted_column")
    for row in changes:
        if deleted and row[deleted]:
            by_key.pop(row[key], None)
        else:
            by_key[row[key]] = row
    merged = pa.Table.from_pylist(list(by_key.values()), schema=previous.schema)
    newest = _postgres_cursor(changes, config)
    next_cursor = (newest if newest and datetime.fromisoformat(newest["watermark"]) > watermark
                   else cursor)
    digest = hashlib.sha256(repr(merged.to_pylist()).encode()).hexdigest()
    return SourceSnapshot(merged, digest, next_cursor, "cursor")


def _http_value(payload: object, path: str, *, required: bool) -> object:
    value = payload
    for field in path.split("."):
        if not isinstance(value, dict) or field not in value:
            if required:
                raise ValueError(f"HTTP response is missing {path}")
            return None
        value = value[field]
    return value


def _http_rows(url: str, allowed_hosts: list[str], config: dict) -> tuple[list[dict], str]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password:
        raise ValueError("invalid HTTP source URL")
    if not parsed.hostname or parsed.hostname not in allowed_hosts:
        raise ValueError("HTTP source host is not allowed")
    rows: list[dict] = []
    digest = hashlib.sha256()
    seen: set[str] = set()
    next_url: str | None = url
    # Source URLs are allow-listed and every pagination hop is checked below.  Do not
    # inherit workstation proxy settings: a proxy can both break same-host sources
    # (notably local development endpoints) and make the validated destination differ
    # from the network peer that actually receives the request.
    with httpx.Client(timeout=20, follow_redirects=False, trust_env=False) as client:
        while next_url is not None:
            target = urlparse(next_url)
            if (target.scheme != parsed.scheme or target.hostname != parsed.hostname or
                    target.port != parsed.port or target.username or target.password):
                raise ValueError("HTTP pagination must stay on the source origin")
            if next_url in seen or len(seen) >= 100:
                raise ValueError("HTTP pagination repeated or exceeded 100 pages")
            seen.add(next_url)
            response = client.get(next_url)
            response.raise_for_status()
            payload = response.content
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
            response_data = json.loads(payload)
            records_path = config.get("records_path")
            next_path = config.get("next_path")
            if records_path:
                values = _http_value(response_data, records_path, required=True)
            elif isinstance(response_data, dict):
                values = response_data.get("data", response_data.get("items", response_data))
            else:
                values = response_data
            if next_path:
                follow = _http_value(response_data, next_path, required=False)
            else:
                follow = (response_data.get("next") if isinstance(response_data, dict) and
                          ("data" in response_data or "items" in response_data) else None)
            if isinstance(values, dict):
                values = [values]
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise ValueError("HTTP source must return a list of records")
            rows.extend(values)
            if follow is not None and (not isinstance(follow, str) or not follow):
                raise ValueError("HTTP pagination next must be a URL string")
            next_url = urljoin(next_url, follow) if follow else None
    return rows, digest.hexdigest()


def _http_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError("HTTP update marker must be an ISO timestamp string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("HTTP update marker must be an ISO timestamp string") from exc
    if parsed.tzinfo is None:
        raise ValueError("HTTP update marker must include a timezone")
    return parsed.astimezone(UTC)


def _http_mapped_rows(rows: list[dict], config: dict) -> list[dict]:
    field_map = config.get("field_map") or {}
    if not field_map:
        return rows
    mapped = []
    for row in rows:
        missing = set(field_map) - set(row)
        if missing:
            raise ValueError(f"HTTP source is missing mapped fields: {', '.join(sorted(missing))}")
        renamed = {field_map.get(name, name): value for name, value in row.items()}
        if len(renamed) != len(row):
            raise ValueError("HTTP field mapping collides with another field")
        mapped.append(renamed)
    return mapped


def _http_cursor(rows: list[dict], config: dict) -> dict | None:
    marker = config["updated_at"]
    if not rows:
        return None
    values = [_http_time(row[marker]) for row in rows]
    return {"watermark": max(values).isoformat()}


def _http_snapshot(config: dict, allowed_hosts: list[str]) -> SourceSnapshot:
    rows, payload_hash = _http_rows(str(config["url"]), allowed_hosts, config)
    rows = _http_mapped_rows(rows, config)
    if not config.get("updated_at"):
        return SourceSnapshot(_rows_table(rows), payload_hash)
    key = config["primary_key"]
    if any(row.get(key) is None for row in rows) or len({row[key] for row in rows}) != len(rows):
        raise ValueError("HTTP primary key must be present and unique")
    cursor = _http_cursor(rows, config)
    deleted = config.get("deleted_column")
    if deleted:
        rows = [row for row in rows if not row[deleted]]
    table = _rows_table(rows)
    digest = hashlib.sha256(repr(table.to_pylist()).encode()).hexdigest()
    return SourceSnapshot(table, digest, cursor)


def read_http_changes(config: dict, previous: pa.Table, cursor: dict,
                      allowed_hosts: list[str]) -> SourceSnapshot:
    """Read timestamp-filtered API changes and merge by a stable record key."""
    key, marker = config["primary_key"], config["updated_at"]
    if any(not IDENTIFIER.fullmatch(name) for name in (key, marker)):
        raise ValueError("invalid HTTP cursor field")
    watermark = _http_time(cursor["watermark"])
    cutoff = watermark - timedelta(seconds=60)
    url = urlparse(str(config["url"]))
    parameter = str(config.get("cursor_param", "updated_after"))
    if not IDENTIFIER.fullmatch(parameter):
        raise ValueError("invalid HTTP cursor parameter")
    query = [(name, value) for name, value in parse_qsl(url.query)
             if name != parameter]
    query.append((parameter, cutoff.isoformat()))
    target = urlunparse(url._replace(query=urlencode(query)))
    changes, _ = _http_rows(target, allowed_hosts, config)
    changes = _http_mapped_rows(changes, config)
    if any(_http_time(row[marker]) < cutoff for row in changes):
        raise ValueError("HTTP source returned records older than the requested cursor")
    if any(row.get(key) is None for row in changes):
        raise ValueError("HTTP primary key cannot be null")
    if any(set(row) != set(previous.column_names) for row in changes):
        return _http_snapshot(config, allowed_hosts)
    by_key = {row[key]: row for row in previous.to_pylist()}
    if len(by_key) != previous.num_rows:
        raise ValueError("HTTP primary key is not unique")
    deleted = config.get("deleted_column")
    for row in changes:
        if deleted and row[deleted]:
            by_key.pop(row[key], None)
        else:
            by_key[row[key]] = row
    table = pa.Table.from_pylist(list(by_key.values()), schema=previous.schema)
    newest = _http_cursor(changes, config)
    next_cursor = (newest if newest and _http_time(newest["watermark"]) > watermark
                   else cursor)
    digest = hashlib.sha256(repr(table.to_pylist()).encode()).hexdigest()
    return SourceSnapshot(table, digest, next_cursor, "cursor")


def read_snapshot(kind: str, config: dict, source_object: str, settings: Settings) -> SourceSnapshot:
    if kind == "file":
        path = _source_file(settings.source_root, source_object)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            table = csv.read_csv(path)
        elif suffix == ".parquet":
            table = pq.read_table(path)
        elif suffix == ".jsonl":
            table = pajson.read_json(path)
        elif suffix == ".json":
            values = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(values, list) or any(not isinstance(row, dict) for row in values):
                raise ValueError("JSON source must be a list of records")
            table = _rows_table(values)
        else:
            raise ValueError("unsupported tabular file type")
        return SourceSnapshot(table, _hash_file(path))
    if kind == "sqlite":
        return _sqlite_snapshot(_source_file(settings.source_root, str(config["path"])), source_object)
    if kind == "postgres":
        return _postgres_snapshot(str(config["database_url_env"]), source_object, config)
    if kind == "http":
        allowed_hosts = [value.strip() for value in settings.http_allowed_hosts.split(",") if value.strip()]
        return _http_snapshot(config, allowed_hosts)
    if kind == "document":
        path = _source_file(settings.source_root, source_object)
        pages: list[tuple[int | None, str]]
        if path.suffix.lower() in {".md", ".txt"}:
            pages = [(None, path.read_text(encoding="utf-8"))]
        elif path.suffix.lower() == ".pdf":
            import pymupdf

            with pymupdf.open(path) as doc:
                pages = [(number, page.get_text()) for number, page in enumerate(doc, start=1)]
        else:
            raise ValueError("unsupported document type")
        rows = [
            (path.name, page_number, start, min(start + 2000, len(content)),
             content[start : start + 2000])
            for page_number, content in pages
            for start in range(0, len(content), 2000)
        ]
        table = pa.table({
            "source": pa.array((row[0] for row in rows), type=pa.string()),
            "chunk_index": pa.array(range(len(rows)), type=pa.int64()),
            "page_number": pa.array((row[1] for row in rows), type=pa.int32()),
            "char_start": pa.array((row[2] for row in rows), type=pa.int64()),
            "char_end": pa.array((row[3] for row in rows), type=pa.int64()),
            "text": pa.array((row[4] for row in rows), type=pa.string()),
        })
        return SourceSnapshot(table, _hash_file(path))
    raise ValueError("unsupported source kind")
