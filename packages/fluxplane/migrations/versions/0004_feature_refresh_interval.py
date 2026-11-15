"""Add optional time-based feature refresh.

Revision ID: 0004_feature_refresh_interval
Revises: 0003_job_resources
"""

import sqlalchemy as sa
from alembic import op

revision = "0004_feature_refresh_interval"
down_revision = "0003_job_resources"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("features", sa.Column("refresh_interval_seconds", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("features", "refresh_interval_seconds")
