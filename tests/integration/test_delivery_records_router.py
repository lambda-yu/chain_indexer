from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.repositories import DeliveryRecordRepo

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_list_filters_by_status_server_side(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        async with db.session() as s:
            repo = DeliveryRecordRepo(s)
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"ok": 1}, status="success",
            )
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"bad": 1}, error="err", status="failed",
            )
            await s.commit()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/delivery-records?status=failed")
            assert r.status_code == 200
            rows = r.json()
            assert len(rows) == 1
            assert rows[0]["status"] == "failed"

            r = await c.get("/api/delivery-records?status=success")
            assert r.status_code == 200
            assert len(r.json()) == 1

            r = await c.get("/api/delivery-records")
            assert len(r.json()) == 2
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_list_rejects_invalid_status(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/delivery-records?status=garbage")
            assert r.status_code == 422
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_retry_failure_bumps_attempts(db: Database, redis_url: str) -> None:
    """A manual retry that itself fails increments attempts and updates error."""
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        from core.config.models import ChannelType
        from core.config.repositories import ChannelRepo

        async with db.session() as s:
            # ChannelRepo.create auto-generates the id (uuid). Capture it from the
            # returned row and use it for the delivery record's channel_id.
            ch = await ChannelRepo(s).create(
                name="bad-webhook",
                type=ChannelType.http,
                config={"url": "http://127.0.0.1:1"},  # connection refused
            )
            row = await DeliveryRecordRepo(s).create(
                subscription_id="sub", channel_id=ch.id, chain_id="eth",
                event_payload={"x": 1}, error="initial", attempts=2, status="failed",
            )
            await s.commit()
            delivery_id = row.id

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.post(f"/api/delivery-records/{delivery_id}/retry", json={})
            assert r.status_code == 502

        async with db.session() as s:
            row = await DeliveryRecordRepo(s).get(delivery_id)
            assert row is not None
            assert row.attempts == 3
            assert row.error != "initial"  # overwritten by new error
            assert row.status == "failed"  # still failed, not resolved
    finally:
        await bus.disconnect()
