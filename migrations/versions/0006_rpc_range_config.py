"""add log_query_range_blocks and slot_query_range_blocks to chains

Revision ID: 0006
Revises: 0005
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(
            sa.Column(
                "log_query_range_blocks",
                sa.Integer(),
                nullable=False,
                server_default="100",
            )
        )
        batch.add_column(
            sa.Column(
                "slot_query_range_blocks",
                sa.Integer(),
                nullable=False,
                server_default="1000",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("slot_query_range_blocks")
        batch.drop_column("log_query_range_blocks")
