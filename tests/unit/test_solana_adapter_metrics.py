from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.metrics import RPC_REQUESTS_TOTAL


@pytest.mark.asyncio
async def test_get_latest_slot_records_rpc_metric(monkeypatch) -> None:
    from core.chains.solana import SolanaAdapter
    adapter = SolanaAdapter(
        chain_id="test-sol", rpc_http="http://x", commitment="confirmed",
    )
    adapter._client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.text = '{"jsonrpc":"2.0","result":12345,"id":1}'
    adapter._client.post = AsyncMock(return_value=fake_resp)

    # solders parsing of getSlot is finicky; mock GetSlotResp.from_json directly.
    parsed = MagicMock()
    parsed.value = 12345
    from solders.rpc.responses import GetSlotResp
    monkeypatch.setattr(GetSlotResp, "from_json", lambda _text: parsed)

    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("test-sol", ["http://x"])

    before = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    result = await adapter.get_latest_slot()
    after = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    assert result == 12345
    assert after - before == 1


@pytest.mark.asyncio
async def test_get_blocks_fails_over_transparently() -> None:
    from core.chains.rpc_pool import EndpointPool
    from core.chains.solana import SolanaAdapter

    adapter = SolanaAdapter(
        chain_id="fo-sol", rpc_http="http://a", rpc_http_fallbacks=["http://b"],
        commitment="confirmed",
    )
    good = MagicMock()
    good.raise_for_status = MagicMock()
    good.json = MagicMock(return_value={"jsonrpc": "2.0", "result": [10, 11], "id": 1})

    async def post(url, **kwargs):
        if url == "http://a":
            raise RuntimeError("node a down")
        return good

    adapter._client = MagicMock()
    adapter._client.post = AsyncMock(side_effect=post)
    adapter._pool = EndpointPool("fo-sol", ["http://a", "http://b"])

    result = await adapter.get_blocks(10, 11)
    assert result == [10, 11]
