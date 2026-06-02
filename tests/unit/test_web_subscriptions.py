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


def _seed_chain_and_channel(c: TestClient) -> tuple[str, str]:
    c.post("/api/chains", json={
        "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
        "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
    })
    r = c.post("/api/channels", json={"name": "hook", "type": "http", "config": {"url": "http://h"}})
    return "eth-mainnet", r.json()["id"]


def test_create_subscription_and_bind_channel(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, channel_id = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "wallet1",
            "chain_id": chain_id,
            "address": "0xabc",
            "abi_id": None,
            "match_kind": "native_transfer",
            "match_name": None,
            "arg_filters": {},
            "enabled": True,
        })
        assert r.status_code == 201, r.text
        sub_id = r.json()["id"]

        b = c.post(f"/api/subscriptions/{sub_id}/channels",
                   json={"channel_id": channel_id})
        assert b.status_code == 204, b.text

        d = c.get(f"/api/subscriptions/{sub_id}")
        assert d.status_code == 200
        assert d.json()["channel_ids"] == [channel_id]

    # Four writes: chain, channel, subscription, bind — four config_changed pubs.
    assert len(bus.published) == 4
    assert {p[1]["entity"] for p in bus.published} == {"chain", "channel", "subscription"}


def test_create_subscription_unknown_chain_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/subscriptions", json={
            "name": "wallet1", "chain_id": "no-such",
            "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
    assert r.status_code == 404


def test_bind_channel_to_missing_subscription_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, channel_id = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions/00000000-0000-0000-0000-000000000000/channels",
                   json={"channel_id": channel_id})
    assert r.status_code == 404


def test_bind_unknown_channel_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        sub = c.post("/api/subscriptions", json={
            "name": "x", "chain_id": chain_id, "address": "0x1",
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        }).json()
        r = c.post(f"/api/subscriptions/{sub['id']}/channels",
                   json={"channel_id": "00000000-0000-0000-0000-000000000000"})
    assert r.status_code == 404


def test_invalid_match_kind_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "x", "chain_id": "eth-mainnet", "address": "0x1",
            "abi_id": None, "match_kind": "telepathy", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
    assert r.status_code == 422


def test_rebind_same_channel_409(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, channel_id = _seed_chain_and_channel(c)
        sub = c.post("/api/subscriptions", json={
            "name": "x", "chain_id": chain_id, "address": "0x1",
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        }).json()
        first = c.post(f"/api/subscriptions/{sub['id']}/channels",
                       json={"channel_id": channel_id})
        assert first.status_code == 204
        second = c.post(f"/api/subscriptions/{sub['id']}/channels",
                        json={"channel_id": channel_id})
    assert second.status_code == 409


def test_subscription_business_name_roundtrip(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "trading-team",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201, r.text
        sub_id = r.json()["id"]
        assert r.json()["business_name"] == "trading-team"

        d = c.get(f"/api/subscriptions/{sub_id}").json()
        assert d["business_name"] == "trading-team"


def test_subscription_business_name_defaults_to_null(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201
        assert r.json()["business_name"] is None


def test_subscription_business_name_whitespace_normalized_to_null(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "   ",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201
        assert r.json()["business_name"] is None


def test_subscription_business_name_put_updates_and_clears(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        sub_id = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "old",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        }).json()["id"]

        # Update to a new value
        r = c.put(f"/api/subscriptions/{sub_id}", json={
            "name": "w", "business_name": "new",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 200
        assert r.json()["business_name"] == "new"

        # Update to null (clear)
        r = c.put(f"/api/subscriptions/{sub_id}", json={
            "name": "w", "business_name": None,
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 200
        assert r.json()["business_name"] is None


def test_subscription_business_name_over_length_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "x" * 256,
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 422
