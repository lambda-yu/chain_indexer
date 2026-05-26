from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from core.chains.evm import EvmAdapter

pytestmark = pytest.mark.asyncio


async def test_poll_interval_ms_is_respected() -> None:
    """With poll_interval_ms=100, the unconditional end-of-loop sleep in
    `_poll_heads` should be 0.1s, not 1.0s."""
    a = EvmAdapter(
        chain_id="x", rpc_http="http://stub", rpc_ws=None,
        confirmations=0, poll_interval_ms=100,
    )

    fake_eth = AsyncMock()

    class _BlockNumberProp:
        def __get__(self, obj: object, objtype: type | None = None) -> Any:
            async def _coro() -> int:
                return 1
            return _coro()
    type(fake_eth).block_number = _BlockNumberProp()
    fake_eth.get_block = AsyncMock(return_value={
        "number": 1, "hash": "0xaa", "parentHash": "0xbb", "timestamp": 1700000000,
    })

    class _FakeW3:
        eth = fake_eth
    object.__setattr__(a, "_w3", _FakeW3())

    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def _record_sleep(d: float) -> None:
        sleeps.append(d)
        await real_sleep(0)

    with patch("core.chains.evm.asyncio.sleep", side_effect=_record_sleep):
        gen = a._poll_heads()
        await asyncio.wait_for(anext(gen), timeout=1.0)
        task: asyncio.Task[Any] = asyncio.create_task(anext(gen))  # type: ignore[arg-type]
        try:
            await real_sleep(0.01)
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                await task

    assert sleeps, "expected at least one sleep call"
    assert sleeps[0] == pytest.approx(0.1, abs=1e-6), \
        f"expected sleep ~0.1s for poll_interval_ms=100, got {sleeps[0]}"
