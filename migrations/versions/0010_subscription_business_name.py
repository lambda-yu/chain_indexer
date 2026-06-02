"""add business_name to subscriptions

Revision ID: 0010
Revises: 0009
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.add_column(sa.Column("business_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.drop_column("business_name")