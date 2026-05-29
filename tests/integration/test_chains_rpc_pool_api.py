from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_create_chain_with_fallbacks(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "eth-api-pool", "kind": "evm", "rpc_http": "http://a",
                "rpc_ws": None, "confirmations": 12, "poll_interval_ms": 3000,
                "rpc_http_fallbacks": ["http://b", "http://a"],  # dup primary
                "rpc_timeout_ms": 7000, "enabled": True,
            })
            assert r.status_code == 201
            body = r.json()
            assert body["rpc_http_fallbacks"] == ["http://b"]  # primary deduped out
            assert body["rpc_timeout_ms"] == 7000
    finally:
        await bus.disconnect()
