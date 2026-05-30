from __future__ import annotations

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
        )
        await s.commit()
        return sub.id


@pytest.mark.asyncio
async def test_pause_then_resume_flips_enabled(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        sub_id = await _seed(db)
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/pause")
            assert r.status_code == 200
            assert r.json()["status"] == "paused"
        async with db.session() as s:
            row = await SubscriptionRepo(s).get(sub_id)
            assert row is not None and row.enabled is False

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/resume")
            assert r.status_code == 200
            assert r.json()["status"] == "resumed"
        async with db.session() as s:
            row = await SubscriptionRepo(s).get(sub_id)
            assert row is not None and row.enabled is True
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_pause_unknown_sub_404(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post("/api/subscriptions/nope/pause")
        assert r.status_code == 404
    finally:
        await bus.disconnect()
