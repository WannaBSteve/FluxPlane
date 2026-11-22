"""Dataset preparation and semantic constraints for multimodal experiments."""

from __future__ import annotations

import csv
import hashlib
import heapq
import io
import math
import zipfile
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import TextIO

CFPB_FIELDS = {
    "date": "Date received",
    "text": "Consumer complaint narrative",
    "id": "Complaint ID",
    "issue": "Issue",
    "response": "Company response to consumer",
    "product": "Product",
}

ISSUE_STRUCTURED_FIELDS = (
    "Product",
    "Sub-product",
    "State",
    "Tags",
    "Submitted via",
)

# These fields are available before the company response is known.  In particular,
# ``Timely response?`` and ``Company public response`` are deliberately excluded:
# both are produced after the decision we want to predict.
RESPONSE_STRUCTURED_FIELDS = (
    "Product",
    "Sub-product",
    "State",
    "Tags",
    "Submitted via",
)

RESPONSE_SEMANTIC_FIELDS = (
    "Issue",
    "Sub-issue",
    "Company",
)


@dataclass(frozen=True)
class FusionRecord:
    identity: str
    received: date
    text: str
    label: str
    product: str
    structured: dict[str, str]
    semantic: dict[str, str] = field(default_factory=dict)
    narrative: str = ""


@dataclass(frozen=True)
class DatasetAudit:
    total_rows: int
    narrative_rows: int
    label_counts: dict[str, int]

    @property
    def narrative_rate(self) -> float:
        return self.narrative_rows / self.total_rows if self.total_rows else 0.0


@contextmanager
def open_csv(path: Path) -> Iterator[TextIO]:
    """Open a plain CSV or a ZIP containing exactly one CSV file."""
    if path.suffix.lower() != ".zip":
        with path.open(encoding="utf-8-sig", newline="") as stream:
            yield stream
        return
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError("dataset ZIP must contain exactly one CSV file")
        with archive.open(names[0]) as raw, io.TextIOWrapper(
            raw, encoding="utf-8-sig", newline=""
        ) as stream:
            yield stream


def _label_field(target: str) -> str:
    try:
        return CFPB_FIELDS[target]
    except KeyError as exc:
        raise ValueError(f"unsupported target: {target}") from exc


def audit_dataset(path: Path, target: str) -> DatasetAudit:
    """Count usable narratives and labels without loading text into memory."""
    label_field = _label_field(target)
    total = narratives = 0
    counts: Counter[str] = Counter()
    with open_csv(path) as stream:
        for row in csv.DictReader(stream):
            total += 1
            text = (row.get(CFPB_FIELDS["text"]) or "").strip()
            label = (row.get(label_field) or "").strip()
            if text and label:
                narratives += 1
                counts[label] += 1
    return DatasetAudit(total, narratives, dict(counts))


def validate_audit(audit: DatasetAudit, *, min_narratives: int,
                   min_label_support: int, label_count: int) -> list[str]:
    """Return the supported top labels or fail early on an unusable slice."""
    if audit.narrative_rows < min_narratives:
        raise ValueError(
            f"dataset has only {audit.narrative_rows} usable narratives; "
            f"at least {min_narratives} are required"
        )
    supported = [
        label for label, count in sorted(
            audit.label_counts.items(), key=lambda item: (-item[1], item[0])
        ) if count >= min_label_support
    ][:label_count]
    if len(supported) < 2:
        raise ValueError("dataset does not contain at least two sufficiently supported labels")
    return supported


def _sample_score(identity: str, seed: int) -> int:
    payload = f"{seed}:{identity}".encode()
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def load_records(path: Path, target: str, labels: list[str], *,
                 max_rows: int, seed: int) -> list[FusionRecord]:
    """Take a deterministic uniform sample, independent of source row order."""
    label_field = _label_field(target)
    selected = set(labels)
    heap: list[tuple[int, str, FusionRecord]] = []
    with open_csv(path) as stream:
        for row_number, row in enumerate(csv.DictReader(stream), start=2):
            text = (row.get(CFPB_FIELDS["text"]) or "").strip()
            narrative = text
            label = (row.get(label_field) or "").strip()
            if not text or label not in selected:
                continue
            identity = (row.get(CFPB_FIELDS["id"]) or str(row_number)).strip()
            try:
                received = date.fromisoformat((row.get(CFPB_FIELDS["date"]) or "").strip())
            except ValueError as exc:
                raise ValueError(f"invalid Date received at CSV row {row_number}") from exc
            structured_fields = (
                RESPONSE_STRUCTURED_FIELDS if target == "response" else ISSUE_STRUCTURED_FIELDS
            )
            structured = {
                field: (row.get(field) or "<missing>").strip() or "<missing>"
                for field in structured_fields
            }
            semantic = {
                field: (row.get(field) or "<missing>").strip() or "<missing>"
                for field in RESPONSE_SEMANTIC_FIELDS
            } if target == "response" else {}
            if target == "response":
                structured["Received year"] = str(received.year)
                structured["Received month"] = f"{received.month:02d}"
                # Field-labelled serialization preserves semantic boundaries while
                # keeping the raw narrative as the dominant text signal.
                text = "\n".join([
                    f"Complaint narrative: {text}",
                    *(f"{name}: {value}" for name, value in semantic.items()),
                ])
            record = FusionRecord(
                identity=identity,
                received=received,
                text=text,
                label=label,
                product=structured[CFPB_FIELDS["product"]],
                structured=structured,
                semantic=semantic,
                narrative=narrative,
            )
            score = _sample_score(identity, seed)
            item = (-score, identity, record)
            if len(heap) < max_rows:
                heapq.heappush(heap, item)
            elif score < -heap[0][0]:
                heapq.heapreplace(heap, item)
    records = [item[2] for item in heap]
    records.sort(key=lambda item: (item.received, item.identity))
    if len(records) < 3:
        raise ValueError("dataset sample is too small for train/validation/test splits")
    return records


def chronological_split(records: list[FusionRecord], train_fraction: float = 0.6,
                        validation_fraction: float = 0.2
                        ) -> tuple[list[FusionRecord], list[FusionRecord], list[FusionRecord]]:
    """Use later records for validation/test to expose temporal drift."""
    if not (0 < train_fraction < 1 and 0 < validation_fraction < 1 and
            train_fraction + validation_fraction < 1):
        raise ValueError("invalid split fractions")
    train_end = max(1, math.floor(len(records) * train_fraction))
    validation_end = max(train_end + 1, math.floor(
        len(records) * (train_fraction + validation_fraction)
    ))
    validation_end = min(validation_end, len(records) - 1)
    return records[:train_end], records[train_end:validation_end], records[validation_end:]


def product_label_map(records: list[FusionRecord]) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for record in records:
        result.setdefault(record.product, set()).add(record.label)
    return result


def apply_taxonomy_mask(probabilities, classes: list[str], products: list[str],
                        allowed: dict[str, set[str]]):
    """Mask issue predictions that were never valid for the product in training."""
    import numpy as np

    masked = probabilities.copy()
    for row_index, product in enumerate(products):
        valid = allowed.get(product)
        if not valid:
            continue
        keep = np.array([label in valid for label in classes], dtype=bool)
        if keep.any():
            masked[row_index, ~keep] = 0.0
            total = masked[row_index].sum()
            masked[row_index] = masked[row_index] / total if total else keep / keep.sum()
    return masked


def semantic_consistency(predictions: list[str], products: list[str],
                         allowed: dict[str, set[str]]) -> float:
    checks = [prediction in allowed[product] for prediction, product in zip(predictions, products)
              if product in allowed]
    return sum(checks) / len(checks) if checks else 1.0
