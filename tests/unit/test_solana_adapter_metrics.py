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

    before = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    result = await adapter.get_latest_slot()
    after = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    assert result == 12345
    assert after - before == 1
