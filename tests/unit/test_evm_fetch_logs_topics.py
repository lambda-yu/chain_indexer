"""Verify EvmAdapter.fetch_logs threads addresses + topics into eth_getLogs."""
from __future__ import annotations

import pytest

from core.chains.evm import EvmAdapter


class _StubEthLogs:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_logs(self, params):
        self.calls.append(dict(params))
        return []


class _StubEth:
    def __init__(self) -> None:
        self._logs = _StubEthLogs()

    async def get_logs(self, params):
        return await self._logs.get_logs(params)


class _StubW3:
    def __init__(self) -> None:
        self.eth = _StubEth()


@pytest.mark.asyncio
async def test_fetch_logs_passes_addresses_and_topics():
    adapter = EvmAdapter(
        chain_id="x", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    adapter._w3 = _StubW3()  # type: ignore[assignment]
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("x", [adapter._w3])  # type: ignore[arg-type]
    await adapter.fetch_logs(
        from_block=10, to_block=20,
        addresses=["0xaaa", "0xbbb"],
        topics=[["0xt0a", "0xt0b"]],
    )
    call = adapter._w3.eth._logs.calls[0]  # type: ignore[attr-defined]
    assert call["fromBlock"] == 10
    assert call["toBlock"] == 20
    assert call["address"] == ["0xaaa", "0xbbb"]
    assert call["topics"] == [["0xt0a", "0xt0b"]]


@pytest.mark.asyncio
async def test_fetch_logs_omits_topics_when_none():
    adapter = EvmAdapter(
        chain_id="x", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    adapter._w3 = _StubW3()  # type: ignore[assignment]
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("x", [adapter._w3])  # type: ignore[arg-type]
    await adapter.fetch_logs(from_block=1, to_block=1, addresses=None, topics=None)
    call = adapter._w3.eth._logs.calls[0]  # type: ignore[attr-defined]
    assert "address" not in call
    assert "topics" not in call
