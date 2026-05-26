from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from core.notifier.redis_streams import RedisStreamsChannel
from core.notifier.retry import RetryExhausted


def _fake_bus_with_client(client: AsyncMock) -> AsyncMock:
    bus = AsyncMock()
    bus.client = client
    return bus


@pytest.mark.asyncio
async def test_xadd_sends_json_payload_to_configured_stream() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus)
    await ch.start()
    try:
        payload: dict[str, Any] = {"k": 1, "subscription_id": "s1"}
        await ch.send(payload)
    finally:
        await ch.stop()

    client.xadd.assert_awaited_once()
    args, kwargs = client.xadd.call_args
    assert args[0] == "events"
    fields = args[1]
    assert json.loads(fields["data"]) == payload
    assert "maxlen" not in kwargs


@pytest.mark.asyncio
async def test_xadd_forwards_maxlen_when_configured() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events", "maxlen": 1000}, bus=bus)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()

    args, kwargs = client.xadd.call_args
    assert kwargs.get("maxlen") == 1000
    assert kwargs.get("approximate") is True


@pytest.mark.asyncio
async def test_transient_redis_error_is_retried_then_succeeds() -> None:
    client = AsyncMock()
    client.xadd.side_effect = [RedisError("temporary"), b"1700000000000-0"]
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.xadd.await_count == 2


@pytest.mark.asyncio
async def test_persistent_redis_error_raises_retry_exhausted() -> None:
    client = AsyncMock()
    client.xadd.side_effect = RedisError("hard down")
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        with pytest.raises(RetryExhausted):
            await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.xadd.await_count == 3


@pytest.mark.asyncio
async def test_missing_stream_config_raises_at_construction() -> None:
    bus = _fake_bus_with_client(AsyncMock())
    with pytest.raises(KeyError):
        RedisStreamsChannel(config={"maxlen": 100}, bus=bus)


def test_type_attribute_matches_db_enum_mq_slot() -> None:
    assert RedisStreamsChannel.type == "mq"
