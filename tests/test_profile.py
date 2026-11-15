"""Dataset profiles summarize the full version with bounded representative samples."""

import pyarrow as pa
from fluxplane.profiling.data import profile_table


def test_profile_samples_across_the_table_and_summarizes_numeric_values() -> None:
    table = pa.table({"amount": list(range(10000))})
    profile = profile_table(table)
    field = profile["fields"][0]
    assert profile["sampled_rows"] == 5000
    assert field["null_count"] == 0
    assert field["distribution"]["kind"] == "numeric"
    assert 4900 <= field["distribution"]["median"] <= 5100
    assert field["distribution"]["min"] <= 10
    assert field["distribution"]["max"] >= 9990


def test_profile_reports_common_categories_without_exposing_high_cardinality_values() -> None:
    table = pa.table({
        "category": ["A", "A", "B", None],
        "identifier": ["id_1", "id_2", "id_3", "id_4"],
    })
    profile = profile_table(table, sample_limit=4)
    category, identifier = profile["fields"]
    assert category["null_count"] == 1
    assert category["distribution"] == {"kind": "categorical", "top_values": [
        {"value": "A", "count": 2}, {"value": "B", "count": 1},
    ]}
    assert identifier["distribution"] is not None
    assert profile_table(pa.table({"identifier": [f"id_{i}" for i in range(30)]}))[
        "fields"][0]["distribution"] is None
