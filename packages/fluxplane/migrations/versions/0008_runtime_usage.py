"""Record observed worker CPU time and resident memory.

Revision ID: 0008_runtime_usage
Revises: 0007_materializations
"""

import sqlalchemy as sa
from alembic import op

revision = "0008_runtime_usage"
down_revision = "0007_materializations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("observations", sa.Column("cpu_ms", sa.Float()))
    op.add_column("observations", sa.Column("peak_rss_mb", sa.Float()))


def downgrade() -> None:
    op.drop_column("observations", "peak_rss_mb")
    op.drop_column("observations", "cpu_ms")
