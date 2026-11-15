"""Database sessions and schema initialization."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from fluxplane.config import Settings
from fluxplane.migrations.legacy import INITIAL_TABLE_COLUMNS


class Database:
    def __init__(self, config: Settings) -> None:
        self.engine = create_engine(config.database_url, pool_pre_ping=True, future=True)
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)

    def create_schema(self) -> None:
        migration_dir = Path(__file__).resolve().parents[1] / "migrations"
        config = Config()
        config.set_main_option("script_location", str(migration_dir))
        with self.engine.begin() as connection:
            if self.engine.dialect.name == "postgresql":
                connection.execute(text("SELECT pg_advisory_xact_lock(154309, 1)"))
            config.attributes["connection"] = connection
            tables = set(inspect(connection).get_table_names())
            if tables and "alembic_version" not in tables:
                # Catalogs created before migrations existed can be adopted only
                # when their tables and columns match the frozen initial schema.
                expected = set(INITIAL_TABLE_COLUMNS)
                if expected != tables:
                    raise RuntimeError("existing catalog does not match the initial schema")
                for name, columns in INITIAL_TABLE_COLUMNS.items():
                    existing = {column["name"] for column in inspect(connection).get_columns(name)}
                    if existing != set(columns):
                        raise RuntimeError(f"existing catalog table {name} has unexpected columns")
                command.stamp(config, "0001_initial")
            command.upgrade(config, "head")

    @contextmanager
    def session(self) -> Iterator[Session]:
        with self.sessions.begin() as session:
            yield session

