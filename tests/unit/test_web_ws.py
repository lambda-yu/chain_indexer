from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.web.routers import ws as ws_module
from core.config.db import Database
from core.config.models import Base, ChannelType
from core.config.repositories import ChannelRepo


class _FakeBus:
    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[dict[str, Any] | None]] = {}
        self._loops: dict[str, asyncio.AbstractEventLoop] = {}

    async def ping(self) -> bool:
        return True

    async def subscribe(
        self, channel: str, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._queues[channel] = q
        self._loops[channel] = loop
        if ready is not None:
            ready.set()
        try:
            while True:
                msg = await q.get()
                if msg is None:
                    return
                yield msg
        finally:
            self._queues.pop(channel, None)
            self._loops.pop(channel, None)

    def feed_threadsafe(self, channel: str, payload: dict[str, Any]) -> None:
        for _ in range(200):
            if channel in self._loops and channel in self._queues:
                break
            time.sleep(0.01)
        else:
            raise RuntimeError(f"no subscriber registered for {channel}")
        loop = self._loops[channel]
        queue = self._queues[channel]
        loop.call_soon_threadsafe(queue.put_nowait, payload)

    def stop_threadsafe(self, channel: str) -> None:
        loop = self._loops.get(channel)
        q = self._queues.get(channel)
        if loop is None or q is None:
            return
        loop.call_soon_threadsafe(q.put_nowait, None)


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db: Database, bus: _FakeBus) -> TestClient:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def _stub_resolver(mapping: dict[str, str | None]):  # type: ignore[type-arg]
    async def _resolver(channel_id: str, _db: Database) -> str | None:
        return mapping.get(channel_id)
    return _resolver


def test_ws_client_receives_messages_from_fanout_channel(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ws_module,
        "_resolve_fanout_channel",
        _stub_resolver({"valid": "fanout-recv"}),
    )
    bus = _FakeBus()
    with _client(db, bus) as c, c.websocket_connect("/ws?channel_id=valid") as ws:
        bus.feed_threadsafe("fanout-recv", {"hello": "world"})
        text = ws.receive_text()
    assert json.loads(text) == {"hello": "world"}


def test_unknown_channel_id_closes_with_policy_violation(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from starlette.websockets import WebSocketDisconnect

    monkeypatch.setattr(
        ws_module, "_resolve_fanout_channel", _stub_resolver({})
    )
    bus = _FakeBus()
    with _client(db, bus) as c, pytest.raises(WebSocketDisconnect) as exc_info:  # noqa: SIM117
        with c.websocket_connect("/ws?channel_id=missing") as ws:
            ws.receive_text()
    assert exc_info.value.code == 1008


def test_slow_consumer_does_not_crash_when_queue_is_full(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ws_module, "_QUEUE_SIZE", 4)
    monkeypatch.setattr(
        ws_module,
        "_resolve_fanout_channel",
        _stub_resolver({"X": "fanout-drop"}),
    )

    bus = _FakeBus()
    with _client(db, bus) as c, c.websocket_connect("/ws?channel_id=X") as ws:
        for i in range(20):
            bus.feed_threadsafe("fanout-drop", {"i": i})
        received: list[dict[str, Any]] = []
        for _ in range(4):
            received.append(json.loads(ws.receive_text()))
        bus.stop_threadsafe("fanout-drop")
    assert len(received) == 4
    nums = [m["i"] for m in received]
    assert nums == sorted(nums)


@pytest.mark.asyncio
async def test_resolver_returns_fanout_for_ws_channel(db: Database) -> None:
    async with db.session() as s:
        row = await ChannelRepo(s).create(
            name="wsx",
            type=ChannelType.ws,
            config={"ws_fanout_channel": "fanout-direct"},
        )
        await s.commit()
        channel_id = row.id
    result = await ws_module._resolve_fanout_channel(channel_id, db)
    assert result == "fanout-direct"


@pytest.mark.asyncio
async def test_resolver_returns_none_for_unknown_channel(db: Database) -> None:
    result = await ws_module._resolve_fanout_channel(
        "00000000-0000-0000-0000-000000000000", db
    )
    assert result is None


@pytest.mark.asyncio
async def test_resolver_returns_none_for_non_ws_type(db: Database) -> None:
    async with db.session() as s:
        row = await ChannelRepo(s).create(
            name="webhook",
            type=ChannelType.http,
            config={"url": "http://example.com/hook"},
        )
        await s.commit()
        channel_id = row.id
    result = await ws_module._resolve_fanout_channel(channel_id, db)
    assert result is None
