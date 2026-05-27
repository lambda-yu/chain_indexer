"""Verify Solana catchup calls get_blocks per window and only fetch_block for valid slots."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.chains.types import SolanaBlock
from core.config.snapshot import (
    ConfigSnapshot, SnapshotChain, SnapshotChannel, SnapshotSubscription,
)


class _Adapter:
    chain_id = "sol-1"

    def __init__(self, valid_slots: dict[tuple[int, int], list[int]], tip: int):
        self._valid = valid_slots
        self._tip = tip
        self.fetch_block_calls: list[int] = []
        self.get_blocks_calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass
    async def get_latest_slot(self): return self._tip
    async def get_blocks(self, start, end):
        self.get_blocks_calls.append((start, end))
        return list(self._valid.get((start, end), []))
    async def fetch_block(self, slot):
        self.fetch_block_calls.append(slot)
        return SolanaBlock(slot=slot, block_hash="0xaa", parent_slot=slot-1, block_time=0, transactions=[])
    def subscribe_heads(self): ...


class _NullChannel:
    type = "http"
    async def start(self): pass
    async def stop(self): pass
    async def send(self, *a, **kw): pass


class _CP:
    def __init__(self, start): self.start = start
    async def get(self, chain_id): return (self.start, "0x00")
    async def save(self, *a, **kw): pass


@pytest.mark.asyncio
async def test_solana_catchup_skips_empty_slots():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    valid_slots = {(1, 10): [1, 3, 5], (11, 15): [11, 14]}
    adapter = _Adapter(valid_slots=valid_slots, tip=15)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_solana()

    assert adapter.get_blocks_calls == [(1, 10), (11, 15)]
    assert adapter.fetch_block_calls == [1, 3, 5, 11, 14]


@pytest.mark.asyncio
async def test_solana_catchup_size_limit_error_raises():
    class _BadAdapter(_Adapter):
        async def get_blocks(self, start, end):
            raise RuntimeError("query range exceeds maximum allowed")

    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    adapter = _BadAdapter(valid_slots={}, tip=5)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    with pytest.raises(RuntimeError, match="exceeds maximum"):
        await runner._catchup_solana()


@pytest.mark.asyncio
async def test_solana_catchup_transient_error_degrades_to_dense():
    class _FlakyAdapter(_Adapter):
        async def get_blocks(self, start, end):
            self.get_blocks_calls.append((start, end))
            raise RuntimeError("temporary network failure")

    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    adapter = _FlakyAdapter(valid_slots={}, tip=3)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_solana()
    assert adapter.fetch_block_calls == [1, 2, 3]
