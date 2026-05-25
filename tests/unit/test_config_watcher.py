from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from apps.worker.config_watcher import ConfigWatcher
from core.config.snapshot import ConfigSnapshot


def _snap(version: int) -> ConfigSnapshot:
    return ConfigSnapshot(version=version, subscriptions=[], channels=[], chains=[])


class _FakeBus:
    """Minimal RedisBus stand-in. `subscribe` yields whatever the test pushes.

    Mirrors the real bus shape: `subscribe()` is NOT awaited by callers — it is
    an async-generator function, so calling it returns the generator directly.
    The real bus yields JSON-decoded dicts (Chunk 3); ConfigWatcher does not
    inspect the payload, so this fake yields strings to keep the test small.
    """

    def __init__(self) -> None:
        self._q: asyncio.Queue[object] = asyncio.Queue()
        self.subscribed_channels: list[str] = []

    async def push(self, msg: object) -> None:
        await self._q.put(msg)

    def subscribe(
        self, channel: str, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[object]:
        self.subscribed_channels.append(channel)

        async def _gen() -> AsyncIterator[object]:
            if ready is not None:
                ready.set()
            while True:
                yield await self._q.get()

        return _gen()


class _FakeSessionFactory:
    """Returns the same session-context manager each time. The loader inspects
    a shared `versions` list to decide what version to emit."""

    def __init__(self, versions: list[int]) -> None:
        self.versions = versions
        self.load_calls = 0

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[AsyncSession]:
        # The loader never touches the session in these unit tests; cast lets
        # the fake satisfy the SessionFactory[AsyncSession] type without a
        # real DB engine.
        yield cast(AsyncSession, self)

    async def load_snapshot_fn(self, _session: AsyncSession) -> ConfigSnapshot:
        self.load_calls += 1
        v = self.versions[min(self.load_calls - 1, len(self.versions) - 1)]
        return _snap(v)


@pytest.mark.asyncio
async def test_watcher_emits_initial_snapshot_on_start() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus,
        session_factory=factory,
        load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0,
        out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        assert factory.load_calls == 1
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_reloads_on_redis_message() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus,
        session_factory=factory,
        load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0,
        out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        # Wait for the subscribe loop to be live before pushing.
        # The fake's `subscribe` always sets `ready` immediately, so a tiny yield is enough.
        await asyncio.sleep(0)
        await bus.push("bump")
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_skips_emit_when_version_unchanged() -> None:
    bus = _FakeBus()
    # Two reloads at v=1 — the watcher must NOT emit the second.
    factory = _FakeSessionFactory(versions=[1, 1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus,
        session_factory=factory,
        load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0,
        out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1

        await asyncio.sleep(0)
        await bus.push("bump")  # triggers reload → still v=1 → no emit
        await bus.push("bump")  # triggers reload → v=2 → emit
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
        assert out.qsize() == 0
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_polls_when_bus_is_quiet() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus,
        session_factory=factory,
        load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=0.05,
        out_queue=out,  # poll fast
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        # No Redis traffic; the 50-ms poll must pick up v=2 within ~200ms.
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_continues_on_load_error() -> None:
    bus = _FakeBus()

    calls = {"n": 0}

    async def flaky_loader(_session: AsyncSession) -> ConfigSnapshot:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("db blip")
        # v=1 then (raises) then v=2
        return _snap(1 if calls["n"] == 1 else 2)

    factory = _FakeSessionFactory(versions=[1, 1, 2])  # versions unused; loader overridden
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
    watcher = ConfigWatcher(
        bus=bus,
        session_factory=factory,
        load_snapshot=flaky_loader,
        poll_interval_s=0.05,
        out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2  # the flaky middle reload was logged & swallowed
    finally:
        await watcher.stop()
