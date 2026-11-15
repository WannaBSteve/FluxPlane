"""Queue submission, leasing, and fenced completion."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy import exists, func, select
from sqlalchemy.orm import Session, aliased

from fluxplane.scheduling.policy import order_ready
from fluxplane.storage.models import (
    CatalogState,
    DatasetLayout,
    Event,
    Job,
    JobAttempt,
    JobDependency,
    Materialization,
    Observation,
    now,
)


def _settle_background_candidate(session: Session, job: Job, reason: str) -> None:
    if job.kind not in {"materialize", "organize"} or not job.target_id:
        return
    model = Materialization if job.kind == "materialize" else DatasetLayout
    candidate = session.get(model, job.target_id, with_for_update=True)
    if candidate is not None and candidate.status == "building":
        candidate.status = "rejected"
        candidate.error = reason


def submit(
    session: Session,
    kind: str,
    lane: str,
    *,
    target_id: str | None = None,
    payload: dict | None = None,
    idempotency_key: str | None = None,
    priority: int = 0,
    cpu_millicores: int = 500,
    memory_mb: int = 512,
    workflow_id: str | None = None,
    node_key: str | None = None,
) -> Job:
    if cpu_millicores < 1 or memory_mb < 1:
        raise ValueError("task resource reservations must be positive")
    if idempotency_key:
        existing = session.scalar(select(Job).where(Job.idempotency_key == idempotency_key))
        if existing:
            return existing
    job = Job(
        kind=kind,
        lane=lane,
        target_id=target_id,
        payload=payload or {},
        idempotency_key=idempotency_key,
        priority=priority,
        cpu_millicores=cpu_millicores,
        memory_mb=memory_mb,
        workflow_id=workflow_id,
        node_key=node_key,
    )
    session.add(job)
    session.flush()
    session.add(Event(kind="job.submitted", subject_id=job.id, data={"kind": kind}))
    return job


def claim(session: Session, lane: str, worker_id: str, lease_seconds: int,
          max_running: int = 1, *, cpu_budget: int = 2000,
          memory_budget: int = 2048) -> Job | None:
    if min(max_running, cpu_budget, memory_budget) < 1:
        raise ValueError("execution pool size and resource budgets must be positive")
    if session.get(CatalogState, 1, with_for_update=True) is None:
        raise RuntimeError("catalog is not initialized")
    recover_expired(session, lane)
    failed_parent = aliased(Job)
    failed_ids = session.scalars(
        select(JobDependency.job_id)
        .join(failed_parent, failed_parent.id == JobDependency.depends_on_id)
        .join(Job, Job.id == JobDependency.job_id)
        .where(Job.lane == lane, Job.status == "pending",
               failed_parent.status.in_(("failed", "cancelled")))
        .distinct()
    ).all()
    for child_id in failed_ids:
        child = session.get(Job, child_id, with_for_update=True)
        if child.status == "pending":
            child.status = "failed"
            child.error = "dependency_failed: a prerequisite did not succeed"
            session.add(Event(kind="job.failed", subject_id=child.id,
                              data={"reason": "dependency_failed"}))
    active = session.scalar(
        select(func.count(Job.id)).where(Job.lane == lane,
                                         Job.status.in_(("running", "cancelling")))
    ) or 0
    if active >= max_running:
        return None
    reserved_cpu, reserved_memory = session.execute(
        select(func.coalesce(func.sum(Job.cpu_millicores), 0),
               func.coalesce(func.sum(Job.memory_mb), 0))
        .where(Job.status.in_(("running", "cancelling")))
    ).one()
    free_cpu = cpu_budget - reserved_cpu
    free_memory = memory_budget - reserved_memory
    prerequisite = aliased(Job)
    waiting = exists(
        select(JobDependency.job_id)
        .join(prerequisite, prerequisite.id == JobDependency.depends_on_id)
        .where(JobDependency.job_id == Job.id, prerequisite.status != "succeeded")
    )
    candidates = session.scalars(
        select(Job).where(Job.lane == lane, Job.status == "pending", ~waiting)
        .order_by(Job.created_at.asc()).limit(100)
    ).all()
    recent = session.execute(
        select(Job.kind, Observation.duration_ms)
        .join(Observation, Observation.job_id == Job.id)
        .where(Job.lane == lane)
        .order_by(Observation.created_at.desc()).limit(200)
    ).all()
    durations: dict[str, list[float]] = {}
    for kind, duration in recent:
        durations.setdefault(kind, []).append(duration)
    estimates = {kind: sum(values) / len(values) for kind, values in durations.items()}
    job = None
    for candidate in order_ready(candidates, estimates, now()):
        if candidate.cpu_millicores > cpu_budget or candidate.memory_mb > memory_budget:
            locked = session.scalar(
                select(Job).where(Job.id == candidate.id, Job.status == "pending")
                .with_for_update(skip_locked=True)
            )
            if locked is not None:
                locked.status = "failed"
                locked.error = "resource_unschedulable: task reservation exceeds instance budget"
                session.add(Event(kind="job.failed", subject_id=locked.id,
                                  data={"reason": "resource_unschedulable"}))
            continue
        if candidate.cpu_millicores > free_cpu or candidate.memory_mb > free_memory:
            continue
        locked = session.scalar(
            select(Job).where(Job.id == candidate.id, Job.status == "pending")
            .with_for_update(skip_locked=True)
        )
        if locked is not None:
            job = locked
            break
    if job is None:
        return None
    job.status = "running"
    job.attempt += 1
    job.lease_generation += 1
    job.worker_id = worker_id
    job.lease_until = now() + timedelta(seconds=lease_seconds)
    session.add(JobAttempt(job_id=job.id, number=job.attempt, worker_id=worker_id, status="running"))
    session.add(Event(kind="job.started", subject_id=job.id, data={"attempt": job.attempt}))
    session.flush()
    return job


def claim_for_worker(session: Session, worker_lane: str, worker_id: str,
                     lease_seconds: int, *, query_workers: int,
                     background_workers: int, cpu_budget: int = 2000,
                     memory_budget: int = 2048,
                     allow_borrow: bool = True) -> Job | None:
    """Keep query workers dedicated and lend idle background workers to queries."""
    if worker_lane not in {"query", "background"}:
        raise ValueError("unknown worker lane")
    if min(query_workers, background_workers) < 1:
        raise ValueError("execution pool sizes must be positive")
    limits = {"cpu_budget": cpu_budget, "memory_budget": memory_budget}
    if worker_lane == "background":
        job = claim(session, "background", worker_id, lease_seconds,
                    background_workers, **limits)
        if job is not None or not allow_borrow:
            return job
    return claim(session, "query", worker_id, lease_seconds,
                 query_workers + background_workers, **limits)


def recover_expired(session: Session, lane: str, max_attempts: int = 3) -> int:
    """Requeue lost work and fence any worker still holding an old generation."""
    expired = session.scalars(
        select(Job)
        .where(Job.lane == lane, Job.status.in_(("running", "cancelling")),
               Job.lease_until < now())
        .with_for_update(skip_locked=True)
    ).all()
    for job in expired:
        previous_status = job.status
        job.lease_generation += 1
        job.lease_until = None
        job.worker_id = None
        job.status = ("cancelled" if previous_status == "cancelling" else
                      "failed" if job.attempt >= max_attempts else "pending")
        if job.status == "failed":
            job.error = "worker lease expired after the final attempt"
        if job.status in {"failed", "cancelled"}:
            _settle_background_candidate(session, job, job.error or "build was cancelled")
        attempt = session.scalar(select(JobAttempt).where(
            JobAttempt.job_id == job.id, JobAttempt.number == job.attempt
        ))
        if attempt is not None:
            attempt.status = "expired"
            attempt.error = "worker lease expired"
            attempt.completed_at = now()
        session.add(Event(kind=f"job.{job.status}", subject_id=job.id,
                          data={"reason": "lease_expired", "attempt": job.attempt}))
    return len(expired)


def heartbeat(session: Session, job_id: str, generation: int, lease_seconds: int) -> bool:
    job = session.scalar(select(Job).where(Job.id == job_id, Job.lease_until >= now())
                         .with_for_update())
    if job is None or job.status != "running" or job.lease_generation != generation:
        return False
    job.lease_until = now() + timedelta(seconds=lease_seconds)
    return True


def complete(
    session: Session,
    job: Job,
    generation: int,
    *,
    result: dict | None = None,
    error: str | None = None,
) -> bool:
    current = session.scalar(select(Job).where(Job.id == job.id, Job.lease_until >= now())
                             .with_for_update())
    if current is None or current.status not in {"running", "cancelling"} or current.lease_generation != generation:
        return False
    if current.status == "cancelling":
        current.status = "cancelled"
        current.result = None
        current.error = None
    else:
        current.status = "failed" if error else "succeeded"
        current.result = result
        current.error = error
    current.lease_until = None
    attempt = session.scalar(
        select(JobAttempt).where(JobAttempt.job_id == job.id, JobAttempt.number == current.attempt)
    )
    if attempt is not None:
        attempt.status = current.status
        attempt.error = error
        attempt.completed_at = now()
    session.add(Event(kind=f"job.{current.status}", subject_id=job.id, data=current.result or {}))
    return True


def cancel(session: Session, job_id: str) -> Job | None:
    job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None:
        return None
    if job.status == "pending":
        job.status = "cancelled"
        _settle_background_candidate(session, job, "build was cancelled")
        session.add(Event(kind="job.cancelled", subject_id=job.id, data={}))
    elif job.status == "running":
        job.status = "cancelling"
        session.add(Event(kind="job.cancelling", subject_id=job.id, data={}))
    return job


def retry(session: Session, job_id: str) -> Job | None:
    job = session.scalar(select(Job).where(Job.id == job_id).with_for_update())
    if job is None:
        return None
    if job.status != "failed":
        raise ValueError("only a failed task can be retried")
    if job.kind == "materialize" and job.target_id:
        candidate = session.get(Materialization, job.target_id, with_for_update=True)
        if candidate is None:
            raise ValueError("materialization candidate is unavailable")
        if candidate.status != "rejected":
            raise ValueError("materialization candidate cannot be retried")
        candidate.status = "building"
        candidate.error = None
    if job.kind == "organize" and job.target_id:
        candidate = session.get(DatasetLayout, job.target_id, with_for_update=True)
        if candidate is None or candidate.status != "rejected":
            raise ValueError("layout candidate cannot be retried")
        candidate.status = "building"
        candidate.error = None
    job.status = "pending"
    job.error = None
    session.add(Event(kind="job.retried", subject_id=job.id, data={"attempt": job.attempt}))
    return job

