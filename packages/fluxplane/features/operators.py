"""Versioned, built-in Python operators for feature computation."""

from __future__ import annotations

import pyarrow as pa

from fluxplane.execution.query import IDENTIFIER

OPERATORS = [{
    "name": "text_match_count",
    "version": "1",
    "description": "Count configured text matches in each input row",
    "input_columns": ["text"],
}]


def validate_operator(config: dict, entity_keys: list[str],
                      columns: set[str] | None = None) -> None:
    if not isinstance(config, dict) or set(config) != {
        "operator", "version", "text_column", "terms", "output_column"
    }:
        raise ValueError("invalid Python operator definition")
    if (config["operator"], config["version"]) != ("text_match_count", "1"):
        raise ValueError("unknown Python operator or version")
    names = [config["text_column"], config["output_column"], *entity_keys]
    if any(not isinstance(name, str) or not IDENTIFIER.fullmatch(name) for name in names):
        raise ValueError("Python operator columns must be plain identifiers")
    if config["output_column"] in entity_keys or len(set(entity_keys)) != len(entity_keys):
        raise ValueError("Python operator output column must differ from entity keys")
    terms = config["terms"]
    if (not isinstance(terms, list) or not 1 <= len(terms) <= 10 or
            any(not isinstance(term, str) or not term.strip() or len(term) > 100
                for term in terms)):
        raise ValueError("Python operator needs one to ten nonempty text matches")
    if columns is not None and (missing := set(entity_keys + [config["text_column"]]) - columns):
        raise ValueError(f"Python operator input columns are missing: {', '.join(sorted(missing))}")


def run_operator(config: dict, entity_keys: list[str], source: pa.Table) -> pa.Table:
    validate_operator(config, entity_keys, set(source.column_names))
    terms = [term.casefold() for term in config["terms"]]
    text_column = config["text_column"]
    output_column = config["output_column"]
    if not pa.types.is_string(source.schema.field(text_column).type) and not pa.types.is_large_string(
        source.schema.field(text_column).type
    ):
        raise ValueError("Python operator text column must contain strings")
    counts = [sum((value or "").casefold().count(term) for term in terms)
              for value in source.column(text_column).to_pylist()]
    return pa.table({**{key: source.column(key) for key in entity_keys},
                     output_column: pa.array(counts, type=pa.int64())})
