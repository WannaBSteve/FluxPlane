"""Persist workload-driven query materializations.

Revision ID: 0007_materializations
Revises: 0006_window_refresh
"""

import sqlalchemy as sa
from alembic import op

revision = "0007_materializations"
down_revision = "0006_window_refresh"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "materializations",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("signature", sa.String(64), nullable=False, unique=True),
        sa.Column("sql", sa.Text(), nullable=False),
        sa.Column("input_versions", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("observed_queries", sa.Integer(), nullable=False),
        sa.Column("observed_total_ms", sa.Float(), nullable=False),
        sa.Column("reuse_count", sa.Integer(), nullable=False),
        sa.Column("artifact_path", sa.Text()),
        sa.Column("sha256", sa.String(64)),
        sa.Column("preview", sa.JSON()),
        sa.Column("row_count", sa.Integer()),
        sa.Column("build_duration_ms", sa.Float()),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("materializations")
