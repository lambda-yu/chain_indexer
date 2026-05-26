from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from core.bus.redis_bus import RedisBus
from core.notifier.retry import RetryExhausted
from core.notifier.websocket import WebSocketChannel


def _fake_bus_with_client(client: AsyncMock) -> AsyncMock:
    bus = AsyncMock(spec=RedisBus)
    bus.client = client
    return bus


@pytest.mark.asyncio
async def test_publish_sends_json_payload_to_configured_fanout_channel() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus)
    await ch.start()
    try:
        await ch.send({"k": 1, "subscription_id": "s1"})
    finally:
        await ch.stop()

    client.publish.assert_awaited_once()
    args, _kwargs = client.publish.call_args
    assert args[0] == "fanout-x"
    assert json.loads(args[1]) == {"k": 1, "subscription_id": "s1"}


@pytest.mark.asyncio
async def test_transient_redis_error_is_retried_then_succeeds() -> None:
    client = AsyncMock()
    client.publish.side_effect = [RedisError("temporary"), 1]
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.publish.await_count == 2


@pytest.mark.asyncio
async def test_persistent_redis_error_raises_retry_exhausted() -> None:
    client = AsyncMock()
    client.publish.side_effect = RedisError("hard down")
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        with pytest.raises(RetryExhausted):
            await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.publish.await_count == 3


@pytest.mark.asyncio
async def test_missing_fanout_channel_config_raises_at_construction() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    with pytest.raises(KeyError):
        WebSocketChannel(config={}, bus=bus)


def test_type_attribute_matches_db_enum_ws_slot() -> None:
    assert WebSocketChannel.type == "ws"
