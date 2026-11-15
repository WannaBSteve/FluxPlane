"""Store the next event-time boundary for rolling features.

Revision ID: 0006_window_refresh
Revises: 0005_workflows
"""

import sqlalchemy as sa
from alembic import op

revision = "0006_window_refresh"
down_revision = "0005_workflows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("feature_snapshots", sa.Column("next_refresh_at", sa.DateTime(timezone=True)))


def downgrade() -> None:
    op.drop_column("feature_snapshots", "next_refresh_at")
