from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# Mirrors `_FakeBus` from tests/unit/test_web_chains.py — captures
# (channel, payload) tuples so the test can assert publish side effects.
class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def ping(self) -> bool:
        return True

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        self.published.append((channel, payload))


# `db` is provided by tests/integration/conftest.py (M1) — file-backed memory SQLite
# with Base.metadata.create_all already run.


@pytest_asyncio.fixture
async def fake_bus() -> _FakeBus:
    return _FakeBus()


@pytest.fixture
def erc20_body() -> list[dict[str, Any]]:
    return [
        {
            "name": "Transfer", "type": "event",
            "inputs": [
                {"name": "from", "type": "address", "indexed": True},
                {"name": "to",   "type": "address", "indexed": True},
                {"name": "value","type": "uint256", "indexed": False},
            ],
        }
    ]


async def test_abi_crud_round_trip(db: Database, fake_bus: _FakeBus, erc20_body) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # CREATE
        r = await c.post("/api/abis", json={
            "name": "erc20", "kind": "evm_abi", "body": erc20_body,
        })
        assert r.status_code == 201, r.text
        abi_id = r.json()["id"]
        assert r.json()["name"] == "erc20"
        assert r.json()["kind"] == "evm_abi"

        # GET single
        r = await c.get(f"/api/abis/{abi_id}")
        assert r.status_code == 200
        assert r.json()["body"] == erc20_body

        # GET 404 for unknown
        r = await c.get("/api/abis/no-such-id")
        assert r.status_code == 404

        # LIST
        r = await c.get("/api/abis")
        assert r.status_code == 200
        ids = [x["id"] for x in r.json()]
        assert abi_id in ids

        # DELETE
        r = await c.delete(f"/api/abis/{abi_id}")
        assert r.status_code == 204

        r = await c.get(f"/api/abis/{abi_id}")
        assert r.status_code == 404

    # Verify config_version bumped via fake_bus's recorded publishes.
    # `bump_and_publish` posts to the "config_changed" channel with an
    # {"entity": "abi", "id": ..., "action": "create|delete"} payload.
    actions = [payload["action"] for _, payload in fake_bus.published]
    assert "create" in actions
    assert "delete" in actions


async def test_abi_create_accepts_empty_body(db: Database, fake_bus: _FakeBus) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/abis", json={"name": "x", "kind": "evm_abi", "body": []})
        # Empty body is allowed at API level; downstream decoder cache will be a no-op.
        # This test just ensures we don't fail-fast on empty arrays.
        assert r.status_code == 201


async def test_abi_create_rejects_invalid_kind(db: Database, fake_bus: _FakeBus) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/abis", json={"name": "x", "kind": "yaml", "body": {}})
        assert r.status_code == 422
