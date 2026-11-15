"""Record measured time spent serving materialized results.

Revision ID: 0010_materialization_benefit
Revises: 0009_python_features
"""

import sqlalchemy as sa
from alembic import op

revision = "0010_materialization_benefit"
down_revision = "0009_python_features"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("materializations", sa.Column("reuse_total_ms", sa.Float(),
                                               nullable=False, server_default="0"))


def downgrade() -> None:
    op.drop_column("materializations", "reuse_total_ms")
