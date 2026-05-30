"""add is_replay to delivery_records

Revision ID: 0009
Revises: 0008
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("delivery_records") as batch:
        batch.add_column(
            sa.Column("is_replay", sa.Boolean(), nullable=False, server_default=sa.false())
        )


def downgrade() -> None:
    with op.batch_alter_table("delivery_records") as batch:
        batch.drop_column("is_replay")
