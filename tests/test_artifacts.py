"""Physical layouts must preserve the logical table while compacting files."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fluxplane.execution.query import equality_filters_by_source, equivalent_results, run_sql
from fluxplane.storage.artifacts import ArtifactStore


def test_partitioned_layout_compacts_fragments_and_preserves_rows(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    table = pa.table({
        "row_id": list(range(18)),
        "region": ["east", "west", "north"] * 6,
        "amount": list(range(18, 36)),
    })
    path, manifest = store.write_layout(table, "region", fragment_rows=2)
    assert store.available_layout(path)
    assert store.verify_layout(path)
    assert manifest["fragment_count"] > manifest["file_count"] == 3
    scanner = store.open_layout(path)
    assert scanner.projected_schema == table.schema
    rebuilt = scanner.to_table()
    assert equivalent_results("SELECT * FROM source", table, rebuilt, store)
    result = run_sql("SELECT SUM(amount) AS total FROM source WHERE region = 'east'",
                     {"source": scanner}, store)
    assert result.column("total")[0].as_py() == sum(range(18, 36, 3))
    with pytest.raises(ValueError, match="outside managed store"):
        store.open_layout(str(tmp_path))
    parquet = next(Path(path).rglob("*.parquet"))
    with parquet.open("ab") as handle:
        handle.write(b"corrupted")
    assert store.available_layout(path)
    assert not store.verify_layout(path)
    parquet.unlink()
    parquet.symlink_to(tmp_path / "outside.parquet")
    assert not store.available_layout(path)
    assert not store.verify_layout(path)
    with pytest.raises(ValueError, match="outside managed store"):
        store.open_layout(path)


def test_partitioned_layout_rejects_high_cardinality_key(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    table = pa.table({"key": list(range(65)), "value": list(range(65))})
    with pytest.raises(ValueError, match="at most 64"):
        store.write_layout(table, "key")


def test_sql_reads_snapshot_through_scanner(tmp_path: Path, monkeypatch) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    path, _ = store.write_table("datasets", pa.table({
        "row_id": [1, 2, 3], "amount": [10, 20, 30],
    }))

    def eager_read(_path: str) -> pa.Table:
        raise AssertionError("query loaded the full artifact before execution")

    monkeypatch.setattr(store, "read_table", eager_read)
    result = run_sql("SELECT SUM(amount) AS total FROM source WHERE row_id >= 2",
                     {"source": path}, store)
    assert result.column("total")[0].as_py() == 50
    outside = tmp_path / "outside.parquet"
    pq.write_table(pa.table({"row_id": [1]}), outside)
    with pytest.raises(ValueError, match="outside managed store"):
        store.open_scanner(str(outside))


def test_result_page_reads_across_parquet_row_groups(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path / "artifacts")
    folder = store.root / "queries"
    folder.mkdir()
    path = folder / "paged.parquet"
    pq.write_table(pa.table({"row_id": list(range(10))}), path, row_group_size=3)
    middle, total = store.read_page(str(path), 4, 4)
    assert total == 10
    assert middle.column("row_id").to_pylist() == [4, 5, 6, 7]
    empty, total = store.read_page(str(path), 10, 4)
    assert total == 10 and empty.num_rows == 0
    assert empty.column_names == ["row_id"]
    pq.write_table(pa.table({"row_id": [99]}), tmp_path / "outside.parquet")
    with pytest.raises(ValueError, match="outside managed store"):
        store.read_page(str(tmp_path / "outside.parquet"), 0, 4)


def test_equality_filters_are_attributed_to_the_filtered_source() -> None:
    sql = ("SELECT * FROM orders o JOIN products p ON o.product_id = p.id "
           "WHERE p.region = 'east' AND o.status IN ('paid', 'open')")
    assert equality_filters_by_source(sql, {"orders", "products"}) == {
        "orders": {"status"}, "products": {"region"},
    }
    assert equality_filters_by_source(
        "SELECT * FROM orders o JOIN products p ON o.product_id = p.id "
        "WHERE region = 'east'", {"orders", "products"},
    ) == {}
