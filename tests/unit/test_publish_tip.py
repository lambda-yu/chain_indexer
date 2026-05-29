from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.settings import Settings


def _build_worker_with_mock_bus() -> tuple[Any, AsyncMock]:
    """Return a _Worker with its bus.client.set replaced by an AsyncMock."""
    from apps.worker.main import _Worker

    worker = _Worker(Settings())
    set_mock = AsyncMock()
    fake_client = MagicMock()
    fake_client.set = set_mock
    worker._bus._client = fake_client  # bypass the connect() guard
    return worker, set_mock


@pytest.mark.asyncio
async def test_publish_tip_writes_redis_key_with_ttl() -> None:
    worker, set_mock = _build_worker_with_mock_bus()
    await worker._publish_tip("eth-mainnet", 12345)
    set_mock.assert_awaited_once_with("chain:eth-mainnet:tip", 12345, ex=60)


@pytest.mark.asyncio
async def test_publish_tip_swallows_redis_errors() -> None:
    """A Redis hiccup must not propagate (or the chain runner would die)."""
    worker, set_mock = _build_worker_with_mock_bus()
    set_mock.side_effect = RuntimeError("redis down")
    # Should not raise:
    await worker._publish_tip("eth-mainnet", 12345)
