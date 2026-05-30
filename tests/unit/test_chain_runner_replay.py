from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.chains.types import Block, BlockHeader


def _build_evm_runner():
    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import SnapshotChain

    chain = SnapshotChain(
        id="eth", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=1, poll_interval_ms=1000, commitment=None,
        trace_internal_calls=False, log_query_range_blocks=100,
        slot_query_range_blocks=1000,
    )
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: MagicMock(),
        channel_factory=lambda c: MagicMock(),
        checkpoint_repo=MagicMock(),
    )
    return runner


def _replay_msg(from_block=10, to_block=11):
    return {
        "request_id": "r1", "chain_id": "eth",
        "subscription": {
            "id": "s1", "name": "t", "chain_id": "eth", "address": None,
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True, "channel_ids": [], "start_block": None,
        },
        "channels": [],
        "from_block": from_block, "to_block": to_block,
    }


@pytest.mark.asyncio
async def test_replay_dispatches_with_replay_flag_and_no_checkpoint(monkeypatch) -> None:
    runner = _build_evm_runner()
    adapter = MagicMock()
    adapter.get_latest_block_number = AsyncMock(return_value=1000)
    hdr = lambda n: BlockHeader(number=n, hash=f"0x{n}", parent_hash="0x0", timestamp=0)
    adapter.fetch_block = AsyncMock(side_effect=lambda n: Block(header=hdr(n), txs=[], logs=[]))
    adapter.fetch_logs = AsyncMock(return_value=[])
    runner._adapter = adapter
    runner._abi_registry = MagicMock()
    runner._cp = MagicMock()
    runner._cp.save = AsyncMock()
    runner._channel_factory = lambda c: MagicMock()
    runner._evm_pipeline = MagicMock()
    runner._evm_pipeline.run = MagicMock(return_value=[])

    import apps.worker.chain_runner as mod
    fake_filter = MagicMock()
    fake_filter.skip_logs = True
    monkeypatch.setattr(mod, "build_evm_log_filter", lambda *a, **k: fake_filter)

    await runner.replay(_replay_msg(10, 11))

    assert adapter.fetch_block.await_count == 2
    runner._cp.save.assert_not_called()


@pytest.mark.asyncio
async def test_replay_clamps_to_safe_tip(monkeypatch) -> None:
    runner = _build_evm_runner()
    adapter = MagicMock()
    adapter.get_latest_block_number = AsyncMock(return_value=15)  # safe_tip = 15 - 1 = 14
    hdr = lambda n: BlockHeader(number=n, hash=f"0x{n}", parent_hash="0x0", timestamp=0)
    adapter.fetch_block = AsyncMock(side_effect=lambda n: Block(header=hdr(n), txs=[], logs=[]))
    adapter.fetch_logs = AsyncMock(return_value=[])
    runner._adapter = adapter
    runner._abi_registry = MagicMock()
    runner._cp = MagicMock(); runner._cp.save = AsyncMock()
    runner._channel_factory = lambda c: MagicMock()
    runner._evm_pipeline = MagicMock(); runner._evm_pipeline.run = MagicMock(return_value=[])

    import apps.worker.chain_runner as mod
    fake_filter = MagicMock(); fake_filter.skip_logs = True
    monkeypatch.setattr(mod, "build_evm_log_filter", lambda *a, **k: fake_filter)

    await runner.replay(_replay_msg(10, 100))
    assert adapter.fetch_block.await_count == 5


def _build_solana_runner():
    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import SnapshotChain

    chain = SnapshotChain(
        id="sol", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        trace_internal_calls=False, log_query_range_blocks=100,
        slot_query_range_blocks=1000,
    )
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: MagicMock(),
        channel_factory=lambda c: MagicMock(),
        checkpoint_repo=MagicMock(),
    )
    return runner


@pytest.mark.asyncio
async def test_replay_solana_skips_none_blocks_and_no_checkpoint() -> None:
    runner = _build_solana_runner()
    adapter = MagicMock()
    adapter.get_latest_slot = AsyncMock(return_value=1000)
    sol_block = MagicMock()
    adapter.fetch_block = AsyncMock(side_effect=lambda s: None if s == 11 else sol_block)
    runner._adapter = adapter
    runner._cp = MagicMock(); runner._cp.save = AsyncMock()
    runner._channel_factory = lambda c: MagicMock()
    runner._solana_pipeline = MagicMock(); runner._solana_pipeline.run = MagicMock(return_value=[])

    msg = {
        "request_id": "r1", "chain_id": "sol",
        "subscription": {
            "id": "s1", "name": "t", "chain_id": "sol", "address": None,
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True, "channel_ids": [], "start_block": None,
        },
        "channels": [],
        "from_block": 10, "to_block": 12,
    }
    await runner.replay(msg)

    assert adapter.fetch_block.await_count == 3
    assert runner._solana_pipeline.run.call_count == 2
    runner._cp.save.assert_not_called()
