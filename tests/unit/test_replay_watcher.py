from __future__ import annotations

import asyncio

import pytest


class _FakeBus:
    def __init__(self, messages):
        self._messages = messages

    async def subscribe(self, channel, *, ready=None):
        if ready is not None:
            ready.set()
        for m in self._messages:
            yield m


@pytest.mark.asyncio
async def test_replay_watcher_forwards_messages() -> None:
    from apps.worker.replay_watcher import ReplayWatcher
    seen: list[dict] = []

    async def on_replay(msg):
        seen.append(msg)

    w = ReplayWatcher(bus=_FakeBus([{"a": 1}, {"a": 2}]), on_replay=on_replay)
    await w.start()
    await asyncio.sleep(0.05)
    await w.stop()
    assert seen == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_replay_watcher_survives_callback_error() -> None:
    from apps.worker.replay_watcher import ReplayWatcher
    seen: list[dict] = []

    async def on_replay(msg):
        if msg["a"] == 1:
            raise RuntimeError("boom")
        seen.append(msg)

    w = ReplayWatcher(bus=_FakeBus([{"a": 1}, {"a": 2}]), on_replay=on_replay)
    await w.start()
    await asyncio.sleep(0.05)
    await w.stop()
    assert seen == [{"a": 2}]


@pytest.mark.asyncio
async def test_on_replay_request_routes_to_runner() -> None:
    from unittest.mock import AsyncMock, MagicMock
    from apps.worker.main import _Worker
    from core.settings import Settings

    worker = _Worker(Settings())
    runner = MagicMock()
    runner.replay = AsyncMock()
    worker._runners["eth"] = (runner, MagicMock())
    await worker._on_replay_request({"chain_id": "eth", "request_id": "r1"})
    await asyncio.sleep(0.01)  # let the created task run
    runner.replay.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_replay_request_no_runner_is_noop() -> None:
    from apps.worker.main import _Worker
    from core.settings import Settings

    worker = _Worker(Settings())
    await worker._on_replay_request({"chain_id": "eth", "request_id": "r1"})  # must not raise
