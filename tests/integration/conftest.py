from collections.abc import AsyncIterator

import pytest_asyncio

from core.config.db import Database
from core.config.models import Base


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()
