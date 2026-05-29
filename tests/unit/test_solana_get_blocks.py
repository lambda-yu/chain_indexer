"""Verify SolanaAdapter.get_blocks calls getBlocks RPC and returns the slot list."""
from __future__ import annotations

import pytest

from core.chains.solana import SolanaAdapter


class _FakeResp:
    def __init__(self, payload): self._p = payload
    def raise_for_status(self): pass
    def json(self): return self._p


@pytest.mark.asyncio
async def test_get_blocks_returns_valid_slots():
    adapter = SolanaAdapter(
        chain_id="sol", rpc_http="http://x", commitment="confirmed",
        poll_interval_ms=2000,
    )
    captured: dict = {}

    async def fake_post(url, json=None, headers=None):
        captured["json"] = json
        return _FakeResp({"jsonrpc": "2.0", "id": 1, "result": [10, 12, 15]})

    adapter._client = type("C", (), {"post": staticmethod(fake_post)})()
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("sol", ["http://x"])
    out = await adapter.get_blocks(10, 20)
    assert out == [10, 12, 15]
    assert captured["json"]["method"] == "getBlocks"
    assert captured["json"]["params"][:2] == [10, 20]
    assert captured["json"]["params"][2]["commitment"] == "finalized"


@pytest.mark.asyncio
async def test_get_blocks_returns_empty_list_when_result_null():
    adapter = SolanaAdapter(
        chain_id="sol", rpc_http="http://x", commitment="confirmed",
        poll_interval_ms=2000,
    )

    async def fake_post(url, json=None, headers=None):
        return _FakeResp({"jsonrpc": "2.0", "id": 1, "result": None})

    adapter._client = type("C", (), {"post": staticmethod(fake_post)})()
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("sol", ["http://x"])
    out = await adapter.get_blocks(10, 20)
    assert out == []
