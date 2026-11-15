"""Persist multi-step workflows and task dependencies.

Revision ID: 0005_workflows
Revises: 0004_feature_refresh_interval
"""

import sqlalchemy as sa
from alembic import op

revision = "0005_workflows"
down_revision = "0004_feature_refresh_interval"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("name", sa.String(160), nullable=False),
        sa.Column("idempotency_key", sa.String(200), unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.add_column("jobs", sa.Column("workflow_id", sa.String(32), nullable=True))
    op.add_column("jobs", sa.Column("node_key", sa.String(80), nullable=True))
    op.create_index("ix_jobs_workflow", "jobs", ["workflow_id"])
    op.create_table(
        "job_dependencies",
        sa.Column("job_id", sa.String(32), sa.ForeignKey("jobs.id"), primary_key=True),
        sa.Column("depends_on_id", sa.String(32), sa.ForeignKey("jobs.id"), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("job_dependencies")
    op.drop_index("ix_jobs_workflow", table_name="jobs")
    op.drop_column("jobs", "node_key")
    op.drop_column("jobs", "workflow_id")
    op.drop_table("workflows")
