"""Verify _catchup_evm issues one fetch_logs per window, N fetch_block calls."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.chains.types import Block, BlockHeader
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)


class _Adapter:
    chain_id = "evm-1"

    def __init__(self, tip: int) -> None:
        self._tip = tip
        self.fetch_block_calls = 0
        self.fetch_logs_calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass

    async def get_latest_block_number(self): return self._tip

    async def fetch_block(self, n):
        self.fetch_block_calls += 1
        return Block(
            header=BlockHeader(number=n, hash="0x" + format(n, "064x"),
                               parent_hash="0x" + format(n-1, "064x"), timestamp=0),
            txs=[], logs=[],
        )

    async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
        self.fetch_logs_calls.append((from_block, to_block))
        return []

    def subscribe_heads(self): ...


class _NullChannel:
    """Minimal Channel stand-in so Notifier.start doesn't crash."""
    type = "http"
    async def start(self): pass
    async def stop(self): pass
    async def send(self, *a, **kw): pass


class _CP:
    def __init__(self, start): self.start = start
    async def get(self, chain_id): return (self.start, "0x" + format(self.start, "064x"))
    async def save(self, *a, **kw): pass


@pytest.mark.asyncio
async def test_catchup_issues_window_sized_log_queries():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
        log_query_range_blocks=10,
    )
    adapter = _Adapter(tip=29)
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_evm()

    assert adapter.fetch_logs_calls == [(1, 10), (11, 20), (21, 29)]
    assert adapter.fetch_block_calls == 29


@pytest.mark.asyncio
async def test_catchup_skips_logs_for_native_transfer_only_subs():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000, log_query_range_blocks=10,
    )
    adapter = _Adapter(tip=5)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_evm()

    assert adapter.fetch_logs_calls == []
    assert adapter.fetch_block_calls == 5
