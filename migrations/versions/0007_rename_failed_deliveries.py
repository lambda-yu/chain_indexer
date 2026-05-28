"""rename failed_deliveries to delivery_records

The table records both successful and failed deliveries; the old name is
misleading. Pure rename — no data shape change.

Revision ID: 0007
Revises: 0006
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.rename_table("failed_deliveries", "delivery_records")


def downgrade() -> None:
    op.rename_table("delivery_records", "failed_deliveries")
