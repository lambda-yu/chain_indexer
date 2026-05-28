"""Verify _fetch_logs_with_degrade bisects on 'too large' errors and re-raises at single-block floor."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.matcher.filter_set import EvmLogFilterSet


class _StubAdapter:
    chain_id = "evm-1"

    def __init__(self, fail_until_window: int) -> None:
        self.fail_until_window = fail_until_window
        self.calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass
    async def get_latest_block_number(self): return 100

    async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
        self.calls.append((from_block, to_block))
        if (to_block - from_block + 1) > self.fail_until_window:
            raise RuntimeError("query returned more than 10000 results: result too large")
        return []


class _NullChannel:
    type = "http"
    async def start(self): pass
    async def stop(self): pass
    async def send(self, *a, **kw): pass


class _CheckpointStub:
    async def get(self, _chain_id: str): return None
    async def save(self, *_a, **_kw): pass


@pytest.fixture
def runner():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    r = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: _StubAdapter(fail_until_window=2),
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    return r, snap


@pytest.mark.asyncio
async def test_bisects_to_passing_window_size(runner):
    r, snap = runner
    await r.start(snap)
    f = EvmLogFilterSet(addresses=["0xaaa"], topic0s=None, skip_logs=False)
    result = await r._fetch_logs_with_degrade(1, 4, f)
    assert result == {}
    adapter = r._adapter
    assert (1, 4) in adapter.calls
    assert (1, 2) in adapter.calls
    assert (3, 4) in adapter.calls


@pytest.mark.asyncio
async def test_single_block_floor_propagates(runner):
    r, snap = runner
    await r.start(snap)
    f = EvmLogFilterSet(addresses=["0xaaa"], topic0s=None, skip_logs=False)
    r._adapter.fail_until_window = 0
    with pytest.raises(RuntimeError, match="too large"):
        await r._fetch_logs_with_degrade(7, 7, f)
