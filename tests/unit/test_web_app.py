from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base


class _FakeBus:
    """Stand-in for RedisBus exposing the methods deps/healthz touch."""

    def __init__(self, ping_ok: bool = True) -> None:
        self.ping_ok = ping_ok
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def ping(self) -> bool:
        return self.ping_ok

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        self.published.append((channel, payload))


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db: Database, bus: _FakeBus) -> TestClient:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def test_healthz_ok(db: Database) -> None:
    bus = _FakeBus(ping_ok=True)
    with _client(db, bus) as c:
        r = c.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body == {"db": "ok", "redis": "ok"}


def test_healthz_reports_redis_failure(db: Database) -> None:
    bus = _FakeBus(ping_ok=False)
    with _client(db, bus) as c:
        r = c.get("/healthz")
    assert r.status_code == 503
    assert r.json()["redis"] == "fail"
