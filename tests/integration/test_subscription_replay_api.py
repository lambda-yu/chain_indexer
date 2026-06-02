from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind, MatchKind
from core.config.repositories import ChainRepo, SubscriptionRepo

pytestmark = pytest.mark.integration


async def _seed(db: Database) -> str:
    async with db.session() as s:
        await ChainRepo(s).create(
            id="eth", kind=ChainKind.evm, rpc_http="x", rpc_ws=None,
            confirmations=1, poll_interval_ms=1000, enabled=True,
        )
        sub = await SubscriptionRepo(s).create(
            name="t", chain_id="eth", address=None, abi_id=None,
            match_kind=MatchKind.native_transfer, match_name=None,
            arg_filters={}, enabled=True, start_block=None,
            business_name="trading-team",
        )
        await s.commit()
        return sub.id


@pytest.mark.asyncio
async def test_replay_publishes_request(db: Database, redis_url: str) -> None:
    bus_writer = RedisBus(url=redis_url)
    bus_reader = RedisBus(url=redis_url)
    await bus_writer.connect()
    await bus_reader.connect()
    drain: asyncio.Task | None = None
    try:
        sub_id = await _seed(db)
        received: list[dict] = []
        ready = asyncio.Event()

        async def _drain() -> None:
            async for msg in bus_reader.subscribe("replay_request", ready=ready):
                received.append(msg)
                return
        drain = asyncio.create_task(_drain())
        await asyncio.wait_for(ready.wait(), timeout=5.0)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus_writer
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/replay",
                             json={"from_block": 100, "to_block": 200})
            assert r.status_code == 202
            body = r.json()
            assert body["status"] == "accepted"
            assert "request_id" in body and body["chain_id"] == "eth"

        await asyncio.wait_for(drain, timeout=5.0)
        assert len(received) == 1
        msg = received[0]
        assert msg["chain_id"] == "eth"
        assert msg["subscription"]["id"] == sub_id
        assert msg["subscription"]["start_block"] is None
        assert msg["subscription"]["enabled"] is True
        assert msg["from_block"] == 100 and msg["to_block"] == 200
        assert msg["subscription"]["business_name"] == "trading-team"
    finally:
        if drain and not drain.done():
            drain.cancel()
        await bus_writer.disconnect()
        await bus_reader.disconnect()


@pytest.mark.asyncio
async def test_replay_validation(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        sub_id = await _seed(db)
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            assert (await c.post("/api/subscriptions/nope/replay",
                                 json={"from_block": 1, "to_block": 2})).status_code == 404
            assert (await c.post(f"/api/subscriptions/{sub_id}/replay",
                                 json={"from_block": 200, "to_block": 100})).status_code == 422
            assert (await c.post(f"/api/subscriptions/{sub_id}/replay",
                                 json={"from_block": 0, "to_block": 100000})).status_code == 422
    finally:
        await bus.disconnect()
