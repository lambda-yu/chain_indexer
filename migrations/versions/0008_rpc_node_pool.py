"""add rpc_http_fallbacks and rpc_timeout_ms to chains

Revision ID: 0008
Revises: 0007
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(
            sa.Column("rpc_http_fallbacks", sa.JSON(), nullable=False, server_default="[]")
        )
        batch.add_column(
            sa.Column("rpc_timeout_ms", sa.Integer(), nullable=False, server_default="10000")
        )


def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("rpc_timeout_ms")
        batch.drop_column("rpc_http_fallbacks")
