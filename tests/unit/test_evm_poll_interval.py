from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

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

    # Inject a fake AsyncWeb3-like that always reports block 1.
    fake_eth = AsyncMock()
    # `_poll_heads` does `await self._w3.eth.block_number`, so this attribute
    # must be a fresh awaitable each access (web3.py exposes it as a coro property).
    class _BlockNumberProp:
        def __get__(self, obj: object, objtype: type | None = None) -> Any:
            async def _coro() -> int:
                return 1
            return _coro()
    type(fake_eth).block_number = _BlockNumberProp()  # type: ignore[assignment]
    fake_eth.get_block = AsyncMock(return_value={
        "number": 1, "hash": "0xaa", "parentHash": "0xbb", "timestamp": 1700000000,
    })
    class _FakeW3:
        eth = fake_eth
    a._w3 = _FakeW3()  # type: ignore[assignment]

    # Patch asyncio.sleep at the evm module's namespace so the cadence inside
    # `_poll_heads` is observable without actually sleeping.
    sleeps: list[float] = []
    import core.chains.evm as evm_mod
    orig_sleep = evm_mod.asyncio.sleep

    async def _record_sleep(d: float) -> None:
        sleeps.append(d)
        await orig_sleep(0)  # don't actually wait

    evm_mod.asyncio.sleep = _record_sleep  # type: ignore[assignment]
    try:
        gen = a._poll_heads()
        # First `anext` flushes the first head (yield happens before the sleep).
        await asyncio.wait_for(anext(gen), timeout=1.0)
        # Second `anext` drives the loop past the yield into `await asyncio.sleep(...)`;
        # because `_record_sleep` doesn't actually wait and block_number stays at 1,
        # the loop spins (no new head) but our recorder captures the cadence.
        # Use create_task + a tiny real sleep to let the generator advance.
        task = asyncio.create_task(anext(gen))
        try:
            await orig_sleep(0.01)
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, StopAsyncIteration):
                pass
    finally:
        evm_mod.asyncio.sleep = orig_sleep  # type: ignore[assignment]

    assert sleeps, "expected at least one sleep call"
    assert sleeps[0] == pytest.approx(0.1, abs=1e-6), \
        f"expected sleep ~0.1s for poll_interval_ms=100, got {sleeps[0]}"
