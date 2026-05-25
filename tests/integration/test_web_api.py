from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.repositories import ConfigVersionRepo

pytestmark = pytest.mark.integration

EXPECTED_PUBLISHES = 4


@pytest.mark.asyncio
async def test_full_create_flow_bumps_version_and_publishes(
    db: Database, redis_url: str
) -> None:
    """db, redis_url come from tests/integration/conftest.py.

    Subscribes to `config_changed` on a separate RedisBus instance, then drives
    create+bind through the API, and asserts every write produced a publish AND
    bumped the global version counter.
    """
    bus_writer = RedisBus(url=redis_url)
    bus_reader = RedisBus(url=redis_url)
    await bus_writer.connect()
    await bus_reader.connect()
    drain_task: asyncio.Task[None] | None = None
    try:
        received: list[dict[str, Any]] = []
        ready = asyncio.Event()

        async def _drain() -> None:
            async for msg in bus_reader.subscribe("config_changed", ready=ready):
                received.append(msg)
                if len(received) >= EXPECTED_PUBLISHES:
                    return

        drain_task = asyncio.create_task(_drain())
        await asyncio.wait_for(ready.wait(), timeout=5.0)

        async with db.session() as s:
            v0 = await ConfigVersionRepo(s).get()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r1 = await c.post("/api/chains", json={
                "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
                "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
            })
            assert r1.status_code == 201

            r2 = await c.post("/api/channels", json={
                "name": "hook", "type": "http", "config": {"url": "http://h"},
            })
            assert r2.status_code == 201
            channel_id = r2.json()["id"]

            r3 = await c.post("/api/subscriptions", json={
                "name": "wallet1", "chain_id": "eth-mainnet", "address": "0x1",
                "abi_id": None, "match_kind": "native_transfer", "match_name": None,
                "arg_filters": {}, "enabled": True,
            })
            assert r3.status_code == 201
            sub_id = r3.json()["id"]

            r4 = await c.post(f"/api/subscriptions/{sub_id}/channels",
                              json={"channel_id": channel_id})
            assert r4.status_code == 204

        await asyncio.wait_for(drain_task, timeout=3.0)

        async with db.session() as s:
            v_final = await ConfigVersionRepo(s).get()

        assert v_final == v0 + EXPECTED_PUBLISHES
        assert [m["entity"] for m in received] == ["chain", "channel", "subscription", "subscription"]
        assert received[-1]["action"] == "bind_channel"
    finally:
        if drain_task is not None and not drain_task.done():
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await drain_task
        await bus_reader.disconnect()
        await bus_writer.disconnect()
