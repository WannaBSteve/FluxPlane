"""Append-safe sum and count feature definition."""

from __future__ import annotations

from fluxplane.execution.query import IDENTIFIER


def aggregate_sql(source: str, keys: list[str], measure: str,
                  sum_alias: str, count_alias: str) -> str:
    names = [source, measure, sum_alias, count_alias, *keys]
    if not keys or any(not isinstance(name, str) or not IDENTIFIER.fullmatch(name)
                       for name in names):
        raise ValueError("aggregate feature needs plain SQL identifiers")
    if len({name.lower() for name in keys + [sum_alias, count_alias]}) != len(keys) + 2:
        raise ValueError("aggregate output names must be distinct")
    key_sql = ", ".join(f'"{key}"' for key in keys)
    return (f'SELECT {key_sql}, SUM("{measure}") AS "{sum_alias}", '
            f'COUNT(*) AS "{count_alias}" FROM "{source}" GROUP BY {key_sql}')


def merge_sql(keys: list[str], sum_alias: str, count_alias: str,
              delta_sql: str) -> str:
    key_sql = ", ".join(f'"{key}"' for key in keys)
    return (f'SELECT {key_sql}, SUM("{sum_alias}") AS "{sum_alias}", '
            f'CAST(SUM("{count_alias}") AS BIGINT) AS "{count_alias}" '
            f'FROM (SELECT * FROM prior UNION ALL {delta_sql}) GROUP BY {key_sql}')
