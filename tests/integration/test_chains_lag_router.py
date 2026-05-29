from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind
from core.config.repositories import ChainRepo, CheckpointRepo

pytestmark = pytest.mark.integration


async def _seed_chain(db: Database, chain_id: str = "eth-mainnet") -> None:
    async with db.session() as s:
        await ChainRepo(s).create(
            id=chain_id, kind=ChainKind.evm,
            rpc_http="x", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
        )
        await s.commit()


@pytest.mark.asyncio
async def test_lag_normal_path(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        async with db.session() as s:
            await CheckpointRepo(s).upsert(
                "eth-mainnet", last_block=100, last_block_hash="0xabc",
            )
            await s.commit()
        await bus.client.set("chain:eth-mainnet:tip", 123, ex=60)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        assert r.status_code == 200
        body = r.json()
        assert body["tip_block"] == 123
        assert body["last_processed_block"] == 100
        assert body["lag_blocks"] == 23
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_missing_tip(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        async with db.session() as s:
            await CheckpointRepo(s).upsert(
                "eth-mainnet", last_block=100, last_block_hash="0xabc",
            )
            await s.commit()
        await bus.client.delete("chain:eth-mainnet:tip")

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        body = r.json()
        assert body["tip_block"] is None
        assert body["lag_blocks"] is None
        assert body["last_processed_block"] == 100
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_missing_checkpoint(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        await bus.client.set("chain:eth-mainnet:tip", 999, ex=60)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        body = r.json()
        assert body["tip_block"] == 999
        assert body["last_processed_block"] is None
        assert body["lag_blocks"] is None
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_unknown_chain(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/no-such-chain/lag")
        assert r.status_code == 404
    finally:
        await bus.disconnect()
