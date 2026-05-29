from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.metrics import RPC_REQUESTS_TOTAL


@pytest.mark.asyncio
async def test_get_latest_block_number_records_rpc_metric() -> None:
    from core.chains.evm import EvmAdapter
    adapter = EvmAdapter(
        chain_id="test-eth", rpc_http="http://x", rpc_ws=None, confirmations=1,
    )
    # Bypass connect; stub _w3 with the minimal shape this method touches.
    adapter._w3 = MagicMock()
    # block_number is an awaitable property in web3.py 6+.
    async def _bn() -> int:
        return 12345
    type(adapter._w3.eth).block_number = property(lambda self: _bn())

    before = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "success")._value.get()
    result = await adapter.get_latest_block_number()
    after = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "success")._value.get()
    assert result == 12345
    assert after - before == 1


@pytest.mark.asyncio
async def test_rpc_error_records_error_status() -> None:
    """If the underlying RPC raises, track_rpc bumps the error counter."""
    from core.chains.evm import EvmAdapter
    adapter = EvmAdapter(
        chain_id="test-eth", rpc_http="http://x", rpc_ws=None, confirmations=1,
    )
    adapter._w3 = MagicMock()
    async def _bn_fail():
        raise RuntimeError("rpc down")
    type(adapter._w3.eth).block_number = property(lambda self: _bn_fail())

    before = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    with pytest.raises(RuntimeError):
        await adapter.get_latest_block_number()
    after = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    assert after - before == 1
