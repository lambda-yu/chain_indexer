import asyncio
from typing import Any

import pytest
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

from core.bus.redis_bus import RedisBus

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_publish_subscribe_round_trip() -> None:
    with RedisContainer("redis:7-alpine") as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        pub = RedisBus(url)
        sub = RedisBus(url)
        await pub.connect()
        await sub.connect()
        received: list[dict[str, Any]] = []
        ready = asyncio.Event()

        gen = sub.subscribe("test_channel", ready=ready)

        async def consume() -> None:
            async for msg in gen:
                received.append(msg)
                if len(received) >= 2:
                    return

        task = asyncio.create_task(consume())
        await ready.wait()  # subscriber is attached
        await pub.publish("test_channel", {"k": 1})
        await pub.publish("test_channel", {"k": 2})
        await asyncio.wait_for(task, timeout=2.0)
        await gen.aclose()  # type: ignore[attr-defined]  # explicit cleanup, runs finally block

        assert received == [{"k": 1}, {"k": 2}]
        await pub.disconnect()
        await sub.disconnect()
