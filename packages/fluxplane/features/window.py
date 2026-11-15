"""SQL for an event-time rolling sum and its next membership change."""

from __future__ import annotations

from datetime import datetime

import sqlglot
from sqlglot import exp

from fluxplane.execution.query import IDENTIFIER


def _columns(source: str, keys: list[str], measure: str, event_time: str,
             sum_alias: str, count_alias: str, window_seconds: int) -> tuple[str, str]:
    names = [source, measure, event_time, sum_alias, count_alias, *keys]
    if (not keys or any(not isinstance(name, str) or not IDENTIFIER.fullmatch(name)
                        for name in names) or not isinstance(window_seconds, int) or
            isinstance(window_seconds, bool) or window_seconds < 1):
        raise ValueError("rolling feature needs valid columns and a positive window")
    if len({name.lower() for name in [*keys, sum_alias, count_alias]}) != len(keys) + 2:
        raise ValueError("rolling feature output names must be distinct")
    return ", ".join(f'"{key}"' for key in keys), f'"{event_time}"'


def _anchor(as_of: datetime | None) -> str:
    return "CURRENT_TIMESTAMP" if as_of is None else f"TIMESTAMPTZ '{as_of.isoformat()}'"


def rolling_sum_sql(source: str, keys: list[str], measure: str, event_time: str,
                    window_seconds: int, sum_alias: str, count_alias: str,
                    as_of: datetime | None = None) -> str:
    key_sql, event_sql = _columns(source, keys, measure, event_time, sum_alias,
                                  count_alias, window_seconds)
    anchor = _anchor(as_of)
    return (f'SELECT {key_sql}, SUM("{measure}") AS "{sum_alias}", '
            f'COUNT(*) AS "{count_alias}" FROM "{source}" '
            f'WHERE CAST({event_sql} AS TIMESTAMPTZ) > '
            f"{anchor} - INTERVAL '{window_seconds} seconds' "
            f'AND CAST({event_sql} AS TIMESTAMPTZ) <= {anchor} '
            f'GROUP BY {key_sql}')


def rolling_next_due_sql(source: str, keys: list[str], measure: str, event_time: str,
                         window_seconds: int, sum_alias: str, count_alias: str,
                         as_of: datetime) -> str:
    _, event_sql = _columns(source, keys, measure, event_time, sum_alias,
                             count_alias, window_seconds)
    anchor = _anchor(as_of)
    return (f"SELECT MIN(CASE WHEN event_ts > {anchor} THEN event_ts "
            f"ELSE event_ts + INTERVAL '{window_seconds} seconds' END) AS next_due "
            f'FROM (SELECT CAST({event_sql} AS TIMESTAMPTZ) AS event_ts FROM "{source}") '
            f"WHERE event_ts > {anchor} - INTERVAL '{window_seconds} seconds'")


def validate_window_rules(windows: dict, dependencies: list[str],
                          columns: dict[str, set[str]] | None = None) -> None:
    if not isinstance(windows, dict) or not windows or set(windows) - set(dependencies):
        raise ValueError("window rules must refer to input datasets")
    for source, spec in windows.items():
        if (not isinstance(spec, dict) or set(spec) != {"event_time", "window_seconds"} or
                not isinstance(spec["event_time"], str) or
                not IDENTIFIER.fullmatch(spec["event_time"]) or
                not isinstance(spec["window_seconds"], int) or
                isinstance(spec["window_seconds"], bool) or spec["window_seconds"] < 1):
            raise ValueError("window rule needs an event-time column and positive duration")
        if columns is not None and source in columns and spec["event_time"] not in columns[source]:
            raise ValueError(f"window event-time column is missing from {source}")


def validate_window_query(sql: str) -> None:
    expression = sqlglot.parse_one(sql, read="duckdb")
    volatile = {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NOW",
                "RANDOM", "RAND", "UUID", "GEN_RANDOM_UUID"}
    if any((node.name if isinstance(node, exp.Anonymous) else node.sql_name()).upper()
           in volatile for node in expression.find_all(exp.Func)):
        raise ValueError("window SQL must use the declared input windows for time-dependent results")


def window_filter_sql(source: str, event_time: str, window_seconds: int,
                      as_of: datetime) -> str:
    validate_window_rules({source: {"event_time": event_time,
                                    "window_seconds": window_seconds}}, [source])
    anchor = _anchor(as_of)
    event_sql = f'CAST("{event_time}" AS TIMESTAMPTZ)'
    return (f'SELECT * FROM "{source}" WHERE {event_sql} > '
            f"{anchor} - INTERVAL '{window_seconds} seconds' "
            f'AND {event_sql} <= {anchor}')


def window_next_due_sql(source: str, event_time: str, window_seconds: int,
                        as_of: datetime) -> str:
    validate_window_rules({source: {"event_time": event_time,
                                    "window_seconds": window_seconds}}, [source])
    anchor = _anchor(as_of)
    return (f"SELECT MIN(CASE WHEN event_ts > {anchor} THEN event_ts "
            f"ELSE event_ts + INTERVAL '{window_seconds} seconds' END) AS next_due "
            f'FROM (SELECT CAST("{event_time}" AS TIMESTAMPTZ) AS event_ts '
            f'FROM "{source}") '
            f"WHERE event_ts > {anchor} - INTERVAL '{window_seconds} seconds'")
