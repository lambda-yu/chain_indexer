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
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("test-eth", [adapter._w3])  # type: ignore[arg-type]

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
    from core.chains.rpc_pool import AllEndpointsFailed, EndpointPool
    adapter._pool = EndpointPool("test-eth", [adapter._w3])  # type: ignore[arg-type]

    before = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    with pytest.raises(AllEndpointsFailed):
        await adapter.get_latest_block_number()
    after = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_fetch_block_fails_over_transparently() -> None:
    from unittest.mock import AsyncMock, MagicMock
    from core.chains.evm import EvmAdapter
    from core.chains.rpc_pool import EndpointPool

    adapter = EvmAdapter(
        chain_id="fo-eth", rpc_http="http://a", rpc_http_fallbacks=["http://b"],
        rpc_ws=None, confirmations=1,
    )
    raw_block = {
        "number": 7, "hash": "0xabc", "parentHash": "0xdef", "timestamp": 100,
        "transactions": [],
    }
    h0 = MagicMock()
    h0.eth.get_block = AsyncMock(side_effect=RuntimeError("node 0 down"))
    h1 = MagicMock()
    h1.eth.get_block = AsyncMock(return_value=raw_block)
    adapter._pool = EndpointPool("fo-eth", [h0, h1])

    block = await adapter.fetch_block(7)
    assert block.header.number == 7
    assert block.header.hash == "0xabc"
    h1.eth.get_block.assert_awaited_once()
