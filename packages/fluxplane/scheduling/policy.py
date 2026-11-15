"""Small, deterministic scheduling policy driven by observed task durations."""

from __future__ import annotations

from datetime import UTC, datetime

from fluxplane.storage.models import Job


def order_ready(jobs: list[Job], estimated_ms: dict[str, float], moment: datetime) -> list[Job]:
    """Age waiting work into higher priority; prefer shorter work within a tier."""
    def rank(job: Job) -> tuple:
        created = job.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        waited = max(0.0, (moment - created).total_seconds())
        effective_priority = job.priority + int(waited // 30)
        return (-effective_priority, estimated_ms.get(job.kind, 1000.0), created, job.id)

    return sorted(jobs, key=rank)
