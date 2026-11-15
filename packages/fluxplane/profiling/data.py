"""Compact field summaries saved alongside each immutable dataset version."""

from __future__ import annotations

from math import isfinite
from random import Random

import pyarrow as pa
import pyarrow.compute as pc


def profile_table(table: pa.Table, sample_limit: int = 5000) -> dict:
    if sample_limit < 1:
        raise ValueError("profile sample limit must be positive")
    sample = (table.take(pa.array(sorted(Random(0).sample(range(table.num_rows), sample_limit)),
                                  type=pa.int64()))
              if table.num_rows > sample_limit else table)
    fields: list[dict] = []
    for field in table.schema:
        column = table.column(field.name)
        sampled = sample.column(field.name)
        try:
            distinct = int(pc.count_distinct(sampled).as_py())
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            distinct = None
        distribution = None
        if (pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or
                pa.types.is_decimal(field.type)):
            try:
                quantiles = pc.quantile(sampled, q=[0, 0.25, 0.5, 0.75, 1]).to_pylist()
                if all(value is not None and isfinite(value) for value in quantiles):
                    distribution = dict(zip(("min", "p25", "median", "p75", "max"), quantiles))
                    distribution["kind"] = "numeric"
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass
        elif (distinct is not None and distinct <= 20 and
              (pa.types.is_string(field.type) or pa.types.is_large_string(field.type) or
               pa.types.is_boolean(field.type))):
            try:
                counts = pc.value_counts(sampled).to_pylist()
                common = sorted((entry for entry in counts if entry["values"] is not None),
                                key=lambda entry: (-entry["counts"], str(entry["values"])))[:3]
                distribution = {"kind": "categorical", "top_values": [
                    {"value": str(entry["values"]), "count": entry["counts"]}
                    for entry in common
                ]}
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass
        fields.append({
            "name": field.name,
            "type": str(field.type),
            "null_count": column.null_count,
            "distinct_in_sample": distinct,
            "distribution": distribution,
        })
    return {"sampled_rows": sample.num_rows, "fields": fields}
