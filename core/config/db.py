from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    def __init__(self, url: str, echo: bool = False) -> None:
        self._url = url
        self._echo = echo
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    async def connect(self) -> None:
        self._engine = create_async_engine(self._url, echo=self._echo, future=True)
        self._sessionmaker = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

    async def disconnect(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a session. Caller is responsible for commit/rollback.

        Sessions auto-close on context exit (uncommitted writes are discarded by
        SQLAlchemy at session close). Repositories never commit on their own —
        callers (routers, worker) decide transaction boundaries.
        """
        assert self._sessionmaker is not None, "Database not connected"
        async with self._sessionmaker() as s:
            yield s

    @property
    def engine(self) -> AsyncEngine:
        assert self._engine is not None, "Database not connected"
        return self._engine
