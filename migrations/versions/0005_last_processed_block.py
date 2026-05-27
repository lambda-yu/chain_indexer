"""add last_processed_block + delivery_record improvements

Revision ID: 0005
Revises: 0004
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.add_column(sa.Column("last_processed_block", sa.BigInteger(), nullable=True))
    # Make error nullable + add success status for delivery records
    with op.batch_alter_table("failed_deliveries") as batch:
        batch.alter_column("error", nullable=True)


def downgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.drop_column("last_processed_block")
    with op.batch_alter_table("failed_deliveries") as batch:
        batch.alter_column("error", nullable=False)
