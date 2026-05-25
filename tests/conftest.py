from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]


@pytest_asyncio.fixture(scope="function")
async def redis_url() -> AsyncIterator[str]:
    """Redis testcontainer URL.

    Lives at `tests/conftest.py` (not `tests/integration/conftest.py`) so
    `tests/e2e/` can consume it without duplicate fixture definitions.
    """
    with RedisContainer("redis:7-alpine") as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"
