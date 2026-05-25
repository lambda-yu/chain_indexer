import pytest
from sqlalchemy import text

from core.config.db import Database


@pytest.mark.asyncio
async def test_database_session_executes() -> None:
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
    async with db.session() as s:
        result = await s.execute(text("SELECT 1"))
        assert result.scalar() == 1
    await db.disconnect()
