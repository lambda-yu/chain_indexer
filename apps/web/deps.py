"""FastAPI dependency providers.

These read from `app.state` (populated by `lifespan` in `apps/web/main.py`).
Tests override via `app.dependency_overrides[get_db] = ...`.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from core.bus.redis_bus import RedisBus
from core.config.db import Database


def get_db(request: Request) -> Database:
    db: Database = request.app.state.db
    return db


def get_bus(request: Request) -> RedisBus:
    bus: RedisBus = request.app.state.bus
    return bus


async def get_session(
    db: Database = Depends(get_db),  # noqa: B008
) -> AsyncIterator[AsyncSession]:
    async with db.session() as s:
        yield s
