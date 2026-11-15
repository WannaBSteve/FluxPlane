"""Live PostgreSQL source contract; enabled with FLUXPLANE_TEST_PG_DSN."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.scheduling.jobs import claim
from fluxplane.storage.models import Job
from sqlalchemy import create_engine, select, text


@pytest.mark.skipif(not os.getenv("FLUXPLANE_TEST_PG_DSN"), reason="PostgreSQL test DSN not set")
def test_cursor_sync_and_full_reconciliation(tmp_path: Path, monkeypatch) -> None:
    dsn = os.environ["FLUXPLANE_TEST_PG_DSN"]
    monkeypatch.setenv("FLUXPLANE_TEST_SOURCE_DSN", dsn)
    engine = create_engine(dsn)
    table_name = f"fluxplane_test_{uuid4().hex[:12]}"
    old = datetime.now(UTC) - timedelta(hours=2)
    updated = datetime.now(UTC)

    def work(platform: Platform, lane: str) -> dict:
        with platform.db.session() as session:
            job = claim(session, lane, "pg-test", 120)
            assert job is not None
            job_id, generation = job.id, job.lease_generation
        return platform.process(job_id, generation)

    try:
        with engine.begin() as connection:
            connection.execute(text(
                f'CREATE TABLE "{table_name}" (id integer PRIMARY KEY, amount integer NOT NULL, '
                'updated_at timestamptz NOT NULL, is_deleted boolean NOT NULL DEFAULT false)'
            ))
            connection.execute(text(
                f'INSERT INTO "{table_name}" VALUES (1,10,:marker,false),(2,20,:marker,false)'
            ), {"marker": old})
        platform = Platform(Settings(
            database_url=f"sqlite:///{tmp_path / 'metadata.sqlite3'}",
            artifact_root=tmp_path / "artifacts", source_root=tmp_path / "sources",
        ))
        platform.initialize()
        source = platform.add_source("pg", "postgres", {
            "database_url_env": "FLUXPLANE_TEST_SOURCE_DSN", "primary_key": "id",
            "updated_at": "updated_at", "deleted_column": "is_deleted",
        })
        dataset = platform.add_dataset(source.id, "orders", table_name)
        platform.queue_sync(dataset.id)
        assert work(platform, "background")["result"]["source_read"] == "snapshot"

        with engine.begin() as connection:
            connection.execute(text(
                f'UPDATE "{table_name}" SET amount=15, updated_at=:marker WHERE id=1'
            ), {"marker": updated})
            connection.execute(text(
                f'UPDATE "{table_name}" SET is_deleted=true, updated_at=:marker WHERE id=2'
            ), {"marker": updated})
            connection.execute(text(
                f'INSERT INTO "{table_name}" VALUES (3,30,:marker,false)'
            ), {"marker": updated})
        platform.queue_sync(dataset.id)
        assert work(platform, "background")["result"]["source_read"] == "cursor"
        platform.queue_query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders")
        assert work(platform, "query")["result"]["rows"] == [{"n": 2, "total": "45"}]

        with engine.begin() as connection:
            connection.execute(text(f'DELETE FROM "{table_name}" WHERE id=3'))
        platform.queue_sync(dataset.id, force_full=True)
        assert work(platform, "background")["result"]["source_read"] == "snapshot"
        platform.queue_query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders")
        assert work(platform, "query")["result"]["rows"] == [{"n": 1, "total": "15"}]

        with platform.db.session() as session:
            for job in session.scalars(select(Job).where(Job.kind == "sync")).all():
                job.updated_at = datetime.now(UTC) - timedelta(days=2)
        scheduled = platform.poll_remote_sources()
        assert len(scheduled) == 1
        with platform.db.session() as session:
            assert session.get(Job, scheduled[0]).payload["force_full"] is True
    finally:
        with engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        engine.dispose()
