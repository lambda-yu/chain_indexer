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


def test_create_chain_persists_and_publishes(db: Database) -> None:
    bus = _FakeBus()
    payload = {
        "id": "eth-mainnet",
        "kind": "evm",
        "rpc_http": "http://localhost:8545",
        "rpc_ws": None,
        "confirmations": 12,
        "poll_interval_ms": 3000,
        "enabled": True,
    }
    with _client(db, bus) as c:
        r = c.post("/api/chains", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["id"] == "eth-mainnet"
    assert body["kind"] == "evm"
    assert len(bus.published) == 1
    ch, msg = bus.published[0]
    assert ch == "config_changed"
    assert msg["entity"] == "chain"
    assert msg["id"] == "eth-mainnet"


def test_list_chains_returns_all(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        c.post("/api/chains", json={
            "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
            "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
        })
        c.post("/api/chains", json={
            "id": "bsc", "kind": "evm", "rpc_http": "y", "rpc_ws": None,
            "confirmations": 15, "poll_interval_ms": 3000, "enabled": False,
        })
        r = c.get("/api/chains")
    assert r.status_code == 200
    ids = sorted(x["id"] for x in r.json())
    assert ids == ["bsc", "eth-mainnet"]


def test_get_chain_by_id_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.get("/api/chains/nope")
    assert r.status_code == 404


def test_create_chain_invalid_kind_400(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/chains", json={
            "id": "x", "kind": "doge", "rpc_http": "z", "rpc_ws": None,
            "confirmations": 1, "poll_interval_ms": 1000, "enabled": True,
        })
    assert r.status_code == 422  # pydantic validation
