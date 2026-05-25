from __future__ import annotations

import asyncio

import pytest

from apps.worker.config_watcher import ConfigWatcher
from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo
from core.config.snapshot import ConfigSnapshot, load_snapshot


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_bump_reloads_snapshot(db, redis_url) -> None:
    """db fixture: connected `Database` from tests/integration/conftest.py.
    redis_url: testcontainer URL fixture from same conftest.
    """
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
        watcher = ConfigWatcher(
            bus=bus,
            session_factory=db.session,
            load_snapshot=load_snapshot,  # type: ignore[arg-type]
            out_queue=out,
            poll_interval_s=30.0,  # rely on redis, not poll
        )
        await watcher.start()
        try:
            first = await asyncio.wait_for(out.get(), timeout=2.0)
            v0 = first.version

            async with db.session() as s:
                await ConfigVersionRepo(s).bump()
                await s.commit()
            await bus.publish("config_changed", {"reason": "bump"})

            second = await asyncio.wait_for(out.get(), timeout=2.0)
            assert second.version == v0 + 1
        finally:
            await watcher.stop()
    finally:
        await bus.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poll_fallback_reloads_when_redis_silent(db, redis_url) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
        watcher = ConfigWatcher(
            bus=bus,
            session_factory=db.session,
            load_snapshot=load_snapshot,  # type: ignore[arg-type]
            out_queue=out,
            poll_interval_s=0.2,  # poll fast
        )
        await watcher.start()
        try:
            first = await asyncio.wait_for(out.get(), timeout=2.0)
            v0 = first.version

            async with db.session() as s:
                await ConfigVersionRepo(s).bump()
                await s.commit()
            # Do NOT publish — poll must pick it up.

            second = await asyncio.wait_for(out.get(), timeout=2.0)
            assert second.version == v0 + 1
        finally:
            await watcher.stop()
    finally:
        await bus.disconnect()
