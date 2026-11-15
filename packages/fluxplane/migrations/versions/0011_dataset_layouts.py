"""Track verified secondary physical layouts for dataset versions.

Revision ID: 0011_dataset_layouts
Revises: 0010_materialization_benefit
"""

import sqlalchemy as sa
from alembic import op

revision = "0011_dataset_layouts"
down_revision = "0010_materialization_benefit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_layouts",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("version_id", sa.String(32), sa.ForeignKey("dataset_versions.id"), nullable=False),
        sa.Column("partition_key", sa.String(160), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("artifact_dir", sa.Text()),
        sa.Column("row_count", sa.Integer()),
        sa.Column("partition_count", sa.Integer()),
        sa.Column("fragment_count", sa.Integer()),
        sa.Column("artifact_bytes", sa.Integer()),
        sa.Column("build_ms", sa.Float()),
        sa.Column("baseline_ms", sa.Float()),
        sa.Column("organized_ms", sa.Float()),
        sa.Column("query_count", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("version_id", "partition_key"),
    )


def downgrade() -> None:
    op.drop_table("dataset_layouts")
