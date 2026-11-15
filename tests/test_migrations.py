"""Catalog startup must work for fresh and pre-migration installations."""

import sqlite3
from pathlib import Path

import pytest
from fluxplane.application.service import Platform
from fluxplane.config import Settings
from fluxplane.storage.models import Base
from sqlalchemy import create_engine, inspect, text


def _platform(tmp_path: Path) -> Platform:
    return Platform(Settings(
        database_url=f"sqlite:///{tmp_path / 'catalog.sqlite3'}",
        artifact_root=tmp_path / "artifacts",
        source_root=tmp_path / "sources",
    ))


@pytest.mark.parametrize("legacy", [False, True])
def test_fresh_and_legacy_catalogs_receive_version_stamp(tmp_path: Path, legacy: bool) -> None:
    platform = _platform(tmp_path)
    if legacy:
        engine = create_engine(platform.settings.database_url)
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE dataset_layouts"))
            connection.execute(text("DROP TABLE materializations"))
            connection.execute(text("DROP TABLE job_dependencies"))
            connection.execute(text("DROP TABLE workflows"))
            connection.execute(text("DROP INDEX ix_jobs_workflow"))
            connection.execute(text("ALTER TABLE jobs DROP COLUMN workflow_id"))
            connection.execute(text("ALTER TABLE jobs DROP COLUMN node_key"))
            connection.execute(text("ALTER TABLE dataset_versions DROP COLUMN source_cursor"))
            connection.execute(text("ALTER TABLE jobs DROP COLUMN cpu_millicores"))
            connection.execute(text("ALTER TABLE jobs DROP COLUMN memory_mb"))
            connection.execute(text("ALTER TABLE features DROP COLUMN refresh_interval_seconds"))
            connection.execute(text("ALTER TABLE feature_snapshots DROP COLUMN next_refresh_at"))
            connection.execute(text("ALTER TABLE observations DROP COLUMN cpu_ms"))
            connection.execute(text("ALTER TABLE observations DROP COLUMN peak_rss_mb"))
        engine.dispose()
    platform.initialize()
    platform.initialize()
    with platform.db.engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "0011_dataset_layouts"
        )
        assert "source_cursor" in {
            column["name"] for column in inspect(connection).get_columns("dataset_versions")
        }
        assert {"cpu_millicores", "memory_mb"}.issubset({
            column["name"] for column in inspect(connection).get_columns("jobs")
        })
        assert "refresh_interval_seconds" in {
            column["name"] for column in inspect(connection).get_columns("features")
        }
        assert next(column for column in inspect(connection).get_columns("features")
                    if column["name"] == "sql")["nullable"]
        assert "next_refresh_at" in {
            column["name"] for column in inspect(connection).get_columns("feature_snapshots")
        }
        assert {"workflows", "job_dependencies"}.issubset(inspect(connection).get_table_names())
        assert "materializations" in inspect(connection).get_table_names()
        assert "dataset_layouts" in inspect(connection).get_table_names()
        assert {"cpu_ms", "peak_rss_mb"}.issubset({
            column["name"] for column in inspect(connection).get_columns("observations")
        })
        assert "reuse_total_ms" in {
            column["name"] for column in inspect(connection).get_columns("materializations")
        }


def test_unknown_existing_catalog_is_not_stamped(tmp_path: Path) -> None:
    with sqlite3.connect(tmp_path / "catalog.sqlite3") as connection:
        connection.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
    with pytest.raises(RuntimeError, match="does not match"):
        _platform(tmp_path).initialize()
