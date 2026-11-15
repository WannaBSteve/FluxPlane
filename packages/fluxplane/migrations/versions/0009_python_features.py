"""Allow registered Python operators to define features without SQL.

Revision ID: 0009_python_features
Revises: 0008_runtime_usage
"""

import sqlalchemy as sa
from alembic import op

revision = "0009_python_features"
down_revision = "0008_runtime_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("features") as batch:
        batch.alter_column("sql", existing_type=sa.Text(), nullable=True)


def downgrade() -> None:
    with op.batch_alter_table("features") as batch:
        batch.alter_column("sql", existing_type=sa.Text(), nullable=False)
