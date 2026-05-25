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
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def ping(self) -> bool:
        return True

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


def test_create_http_channel(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/channels", json={
            "name": "hook1",
            "type": "http",
            "config": {"url": "https://example.com/webhook", "method": "POST"},
        })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["type"] == "http"
    assert body["config"]["url"] == "https://example.com/webhook"
    assert len(bus.published) == 1
    assert bus.published[0][0] == "config_changed"


def test_list_channels(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        c.post("/api/channels", json={"name": "a", "type": "http", "config": {"url": "x"}})
        c.post("/api/channels", json={"name": "b", "type": "mq",   "config": {"driver": "rabbitmq", "url": "amqp://"}})
        r = c.get("/api/channels")
    assert r.status_code == 200
    names = sorted(x["name"] for x in r.json())
    assert names == ["a", "b"]


def test_get_channel_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.get("/api/channels/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


def test_create_channel_invalid_type_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/channels", json={"name": "x", "type": "telegram", "config": {}})
    assert r.status_code == 422
