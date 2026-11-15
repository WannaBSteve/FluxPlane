"""Persist task resource reservations.

Revision ID: 0003_job_resources
Revises: 0002_source_cursor
"""

import sqlalchemy as sa
from alembic import op

revision = "0003_job_resources"
down_revision = "0002_source_cursor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("cpu_millicores", sa.Integer(), nullable=False,
                                    server_default="500"))
    op.add_column("jobs", sa.Column("memory_mb", sa.Integer(), nullable=False,
                                    server_default="512"))


def downgrade() -> None:
    op.drop_column("jobs", "memory_mb")
    op.drop_column("jobs", "cpu_millicores")
