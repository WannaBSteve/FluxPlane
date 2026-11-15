"""Alembic environment shared by CLI and application startup."""

from alembic import context
from sqlalchemy import create_engine

from fluxplane.config import settings
from fluxplane.storage.models import Base

target_metadata = Base.metadata


def run_migrations_online() -> None:
    connection = context.config.attributes.get("connection")
    if connection is not None:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(settings().database_url)
    try:
        with engine.connect() as owned:
            context.configure(connection=owned, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


run_migrations_online()
