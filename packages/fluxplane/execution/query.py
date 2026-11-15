"""Read-only SQL over pinned dataset versions."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import sqlglot
from sqlglot import exp

from fluxplane.storage.artifacts import ArtifactStore

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_query(sql: str, available: set[str]) -> set[str]:
    statements = sqlglot.parse(sql, read="duckdb")
    if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.Union)):
        raise ValueError("only one SELECT query is allowed")
    expression = statements[0]
    ctes = {cte.alias for cte in expression.find_all(exp.CTE)}
    referenced = {table.name for table in expression.find_all(exp.Table)} - ctes
    if referenced - available:
        raise ValueError(f"unknown dataset: {', '.join(sorted(referenced - available))}")
    for node in expression.walk():
        if isinstance(node, (exp.Command, exp.Into, exp.Create, exp.Insert, exp.Update, exp.Delete)):
            raise ValueError("query contains an unsupported operation")  # noqa: TRY004
        if isinstance(node, exp.Anonymous):
            name = node.name.lower()
            if name.startswith(("read_", "sqlite_", "postgres_", "http_")) or name in {
                "query_table", "query", "pragma", "range", "glob", "getenv", "install_extension",
                "load_extension", "read_blob", "read_text", "write_file",
            }:
                raise ValueError("external access is not allowed in queries")
    return referenced


def equality_filter_columns(sql: str) -> set[str]:
    """Find simple literal equality filters that can prune a partitioned input."""
    expression = sqlglot.parse_one(sql, read="duckdb")
    columns = set()
    for predicate in expression.find_all(exp.EQ):
        if isinstance(predicate.left, exp.Column) and isinstance(predicate.right, exp.Literal):
            columns.add(predicate.left.name)
        elif isinstance(predicate.right, exp.Column) and isinstance(predicate.left, exp.Literal):
            columns.add(predicate.right.name)
    for predicate in expression.find_all(exp.In):
        if isinstance(predicate.this, exp.Column) and predicate.expressions and all(
            isinstance(value, exp.Literal) for value in predicate.expressions
        ):
            columns.add(predicate.this.name)
    return columns


def equality_filters_by_source(sql: str, available: set[str]) -> dict[str, set[str]]:
    """Attribute simple filters to their input table, conservatively for joins."""
    expression = sqlglot.parse_one(sql, read="duckdb")
    tables = [table for table in expression.find_all(exp.Table) if table.name in available]
    aliases = {table.alias_or_name: table.name for table in tables}
    sole = tables[0].name if len(tables) == 1 else None
    matched: dict[str, set[str]] = {}
    predicates = list(expression.find_all(exp.EQ)) + list(expression.find_all(exp.In))
    for predicate in predicates:
        if predicate.find_ancestor(exp.Where, exp.Join) is None:
            continue
        column = None
        if isinstance(predicate, exp.EQ):
            if isinstance(predicate.left, exp.Column) and isinstance(predicate.right, exp.Literal):
                column = predicate.left
            elif isinstance(predicate.right, exp.Column) and isinstance(predicate.left, exp.Literal):
                column = predicate.right
        elif (isinstance(predicate.this, exp.Column) and predicate.expressions and
              all(isinstance(value, exp.Literal) for value in predicate.expressions)):
            column = predicate.this
        if column is None:
            continue
        source = aliases.get(column.table) if column.table else sole
        if source is not None:
            matched.setdefault(source, set()).add(column.name)
    return matched


MATERIALIZABLE_FUNCTIONS = {
    "ABS", "AVG", "CAST", "COALESCE", "COUNT", "DATE_TRUNC", "IF", "IFNULL",
    "LENGTH", "LOWER", "MAX", "MIN", "NULLIF", "ROUND", "SUBSTRING", "SUM",
    "TRIM", "TRY_CAST", "UPPER",
}


def materialization_signature(sql: str, versions: dict[str, str]) -> tuple[str, dict[str, str]] | None:
    """Identify a conservative, deterministic query over pinned table versions."""
    expression = sqlglot.parse_one(sql, read="duckdb")
    referenced = validate_query(sql, set(versions))
    if not referenced or any(function.sql_name().upper() not in MATERIALIZABLE_FUNCTIONS
                             for function in expression.find_all(exp.Func)):
        return None
    inputs = {name: versions[name] for name in sorted(referenced)}
    payload = json.dumps({"sql": sql.strip(), "inputs": inputs}, sort_keys=True)
    return sha256(payload.encode()).hexdigest(), inputs


def run_sql(sql: str, sources: dict[str, str | pa.Table | ds.Scanner],
            store: ArtifactStore) -> pa.Table:
    validate_query(sql, set(sources))
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET threads=2")
        con.execute("SET TimeZone='UTC'")
        for name, artifact in sources.items():
            if not IDENTIFIER.fullmatch(name):
                raise ValueError("invalid dataset name")
            con.register(name, artifact if isinstance(artifact, (pa.Table, ds.Scanner))
                         else store.open_scanner(artifact))
        con.execute("SET enable_external_access=false")
        return con.execute(sql).to_arrow_table()
    finally:
        con.close()


def equivalent_results(sql: str, first: pa.Table, second: pa.Table,
                       store: ArtifactStore) -> bool:
    """Compare unordered SQL results as multisets; honor explicit result ordering."""
    if first.schema != second.schema or first.num_rows != second.num_rows:
        return False
    if sqlglot.parse_one(sql, read="duckdb").args.get("order") is not None:
        return first.equals(second)
    sources = {"first_result": first, "second_result": second}
    for left, right in (("first_result", "second_result"),
                        ("second_result", "first_result")):
        difference = run_sql(
            f"SELECT COUNT(*) AS n FROM (SELECT * FROM {left} EXCEPT ALL "
            f"SELECT * FROM {right})", sources, store,
        ).column("n")[0].as_py()
        if difference:
            return False
    return True


def table_preview(table: pa.Table, limit: int = 100) -> dict:
    def portable(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, bytes):
            return value.hex()
        if isinstance(value, dict):
            return {str(key): portable(child) for key, child in value.items()}
        if isinstance(value, list):
            return [portable(child) for child in value]
        return value

    return {
        "columns": table.column_names,
        "rows": portable(table.slice(0, limit).to_pylist()),
        "row_count": table.num_rows,
    }

