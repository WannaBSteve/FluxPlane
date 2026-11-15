"""Immutable Parquet artifacts written before catalog publication."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq


class ArtifactStore:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def write_table(self, kind: str, table: pa.Table) -> tuple[str, str]:
        if kind not in {"datasets", "deltas", "features", "queries", "materializations"}:
            raise ValueError("unknown artifact kind")
        folder = self.root / kind
        folder.mkdir(parents=True, exist_ok=True)
        identity = uuid4().hex
        temp = folder / f".{identity}.partial"
        target = folder / f"{identity}.parquet"
        try:
            pq.write_table(table, temp, compression="zstd")
            with temp.open("rb") as handle:
                os.fsync(handle.fileno())
            sha = hashlib.sha256()
            with temp.open("rb") as source:
                for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
                    sha.update(chunk)
            digest = sha.hexdigest()
            os.replace(temp, target)
            directory_fd = os.open(folder, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temp.unlink(missing_ok=True)
        return str(target), digest

    def read_table(self, path: str) -> pa.Table:
        resolved = Path(path).resolve(strict=True)
        if not resolved.is_relative_to(self.root) or resolved.suffix != ".parquet":
            raise ValueError("artifact outside managed store")
        return pq.read_table(resolved)

    def open_scanner(self, path: str) -> ds.Scanner:
        """Expose a published Parquet snapshot without loading its full table."""
        resolved = Path(path).resolve(strict=True)
        if not resolved.is_relative_to(self.root) or resolved.suffix != ".parquet":
            raise ValueError("artifact outside managed store")
        return ds.dataset(resolved, format="parquet").scanner()

    def read_page(self, path: str, offset: int, limit: int) -> tuple[pa.Table, int]:
        """Read a bounded result page, skipping whole Parquet row groups when possible."""
        if offset < 0 or limit < 1:
            raise ValueError("result page offset and limit are invalid")
        resolved = Path(path).resolve(strict=True)
        if not resolved.is_relative_to(self.root) or resolved.suffix != ".parquet":
            raise ValueError("artifact outside managed store")
        parquet = pq.ParquetFile(resolved)
        total = parquet.metadata.num_rows
        batches = []
        remaining_offset = offset
        remaining_limit = limit
        for group in range(parquet.num_row_groups):
            group_size = parquet.metadata.row_group(group).num_rows
            if remaining_offset >= group_size:
                remaining_offset -= group_size
                continue
            for batch in parquet.iter_batches(batch_size=min(1024, limit), row_groups=[group]):
                if remaining_offset >= batch.num_rows:
                    remaining_offset -= batch.num_rows
                    continue
                page_batch = batch.slice(remaining_offset, remaining_limit)
                batches.append(page_batch)
                remaining_limit -= page_batch.num_rows
                remaining_offset = 0
                if remaining_limit == 0:
                    break
            if remaining_limit == 0:
                break
        return pa.Table.from_batches(batches, schema=parquet.schema_arrow), total

    def available(self, path: str) -> bool:
        resolved = Path(path).resolve()
        return resolved.is_relative_to(self.root) and resolved.suffix == ".parquet" and resolved.is_file()

    def verify_artifact(self, path: str, expected_sha256: str) -> bool:
        if not self.available(path):
            return False
        digest = hashlib.sha256()
        try:
            with Path(path).open("rb") as handle:
                for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                    digest.update(chunk)
        except OSError:
            return False
        return digest.hexdigest() == expected_sha256

    def write_layout(self, table: pa.Table, partition_key: str,
                     *, fragment_rows: int = 10000) -> tuple[str, dict]:
        """Create an immutable hive-partitioned layout and compact its fragments."""
        if partition_key not in table.column_names or fragment_rows < 1:
            raise ValueError("layout needs a valid partition key and fragment size")
        field = table.schema.field(partition_key)
        if not (pa.types.is_string(field.type) or pa.types.is_integer(field.type)):
            raise ValueError("layout partition key must be a string or integer")
        if table.num_rows == 0 or len(pc.unique(table.column(partition_key))) > 64:
            raise ValueError("layout needs rows and at most 64 key values")
        folder = self.root / "layouts"
        folder.mkdir(parents=True, exist_ok=True)
        identity = uuid4().hex
        temporary = folder / f".{identity}.partial"
        target = folder / identity
        temporary.mkdir()
        try:
            partitioning = ds.partitioning(pa.schema([field]), flavor="hive")
            ds.write_dataset(table, temporary, format="parquet", partitioning=partitioning,
                             basename_template="fragment-{i}.parquet",
                             max_rows_per_file=fragment_rows,
                             max_rows_per_group=fragment_rows)
            fragments = list(temporary.rglob("*.parquet"))
            for partition in sorted({path.parent for path in fragments}):
                parts = sorted(partition.glob("*.parquet"))
                if len(parts) < 2:
                    continue
                merged = ds.dataset([str(path) for path in parts], format="parquet")
                compacted = partition / "compact.parquet"
                with pq.ParquetWriter(compacted, merged.schema, compression="zstd") as writer:
                    for batch in merged.to_batches(batch_size=fragment_rows):
                        writer.write_batch(batch)
                for path in parts:
                    path.unlink()
            files = sorted(temporary.rglob("*.parquet"))
            digest = hashlib.sha256()
            total_bytes = 0
            for path in files:
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
                    digest.update(str(path.relative_to(temporary)).encode())
                    for payload in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                        total_bytes += len(payload)
                        digest.update(payload)
            manifest = {
                "partition_key": partition_key,
                "schema": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
                "row_count": table.num_rows,
                "partition_count": len({path.parent for path in files}),
                "fragment_count": len(fragments),
                "file_count": len(files),
                "files": [str(path.relative_to(temporary)) for path in files],
                "bytes": total_bytes,
                "sha256": digest.hexdigest(),
            }
            manifest_path = temporary / "_manifest.json"
            with manifest_path.open("w", encoding="utf-8") as handle:
                json.dump(manifest, handle, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            directory_fd = os.open(folder, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return str(target), manifest
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    def open_layout(self, path: str) -> ds.Scanner:
        resolved = Path(path).resolve(strict=True)
        if not self.available_layout(path):
            raise ValueError("layout outside managed store")
        manifest = json.loads((resolved / "_manifest.json").read_text(encoding="utf-8"))
        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(manifest["schema"])))
        key = manifest["partition_key"]
        partitioning = ds.partitioning(pa.schema([schema.field(key)]), flavor="hive")
        return ds.dataset(resolved, format="parquet", partitioning=partitioning).scanner(
            columns=schema.names,
        )

    def available_layout(self, path: str) -> bool:
        original = Path(path)
        resolved = original.resolve()
        manifest_path = resolved / "_manifest.json"
        if not (not original.is_symlink() and
                resolved.is_relative_to(self.root / "layouts") and resolved.is_dir() and
                manifest_path.is_file() and not manifest_path.is_symlink()):
            return False
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            contents = list(resolved.rglob("*"))
            if any(item.is_symlink() or not item.resolve().is_relative_to(resolved)
                   for item in contents):
                return False
            files = sorted(str(item.relative_to(resolved)) for item in contents
                           if item.is_file() and item.suffix == ".parquet")
            return files == sorted(manifest["files"]) and len(files) == manifest["file_count"] > 0
        except (OSError, ValueError, KeyError, TypeError):
            return False

    def verify_layout(self, path: str) -> bool:
        if not self.available_layout(path):
            return False
        root = Path(path).resolve()
        try:
            manifest = json.loads((root / "_manifest.json").read_text(encoding="utf-8"))
            digest = hashlib.sha256()
            total_bytes = 0
            for relative in manifest["files"]:
                digest.update(relative.encode())
                with (root / relative).open("rb") as handle:
                    for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
                        total_bytes += len(chunk)
                        digest.update(chunk)
            return digest.hexdigest() == manifest["sha256"] and total_bytes == manifest["bytes"]
        except (OSError, ValueError, KeyError, TypeError):
            return False

