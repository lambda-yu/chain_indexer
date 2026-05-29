"""Unit tests for ChainRunner observability instrumentation.

Verifies that tip-gauge and tip-publisher updates happen on the live path
but NOT during catchup (Solana's _process_solana_slot is shared between
live and catchup, so the tip update must live at the live call site).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.metrics import CHAIN_TIP_BLOCK


def _build_runner(chain_kind: str = "evm"):
    """Return a minimal ChainRunner with mocked dependencies."""
    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import SnapshotChain

    chain = SnapshotChain(
        id="test-chain",
        kind=chain_kind,
        rpc_http="http://localhost:8545",
        rpc_ws=None,
        confirmations=12,
        poll_interval_ms=1000,
        commitment="confirmed" if chain_kind == "solana" else None,
        trace_internal_calls=False,
        log_query_range_blocks=100,
        slot_query_range_blocks=1000,
    )
    tip_publisher = AsyncMock()
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: MagicMock(),
        channel_factory=lambda c: MagicMock(),
        checkpoint_repo=MagicMock(),
        tip_publisher=tip_publisher,
    )
    return runner, tip_publisher


@pytest.mark.asyncio
async def test_handle_evm_head_updates_tip_gauge_and_publisher() -> None:
    runner, tip_publisher = _build_runner("evm")
    # Stub the rest of _handle_evm_head's dependencies so we only exercise
    # the tip-update prelude.
    runner._buffer = MagicMock()
    runner._buffer.handle_new_head.return_value = []  # no confirmed blocks
    runner._buffer_tip_hash = None  # no prefetch
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()

    from core.chains.types import BlockHeader
    header = BlockHeader(
        number=12345, hash="0xabc", parent_hash="0xdef", timestamp=0,
    )

    before = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
    await runner._handle_evm_head(header)
    after = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()

    assert after == 12345
    assert after != before or before == 12345  # gauge moved (or was already there)
    tip_publisher.assert_awaited_once_with("test-chain", 12345)


@pytest.mark.asyncio
async def test_process_solana_slot_does_NOT_update_tip_gauge() -> None:
    """The shared per-slot processor must NOT touch the tip; tip is live-only.
    Verified by calling _process_solana_slot directly (no live-loop wrapper)
    and asserting the tip gauge for this chain is untouched."""
    runner, tip_publisher = _build_runner("solana")
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()
    runner._adapter.fetch_block = AsyncMock(return_value=None)  # skip body
    runner._solana_pipeline = MagicMock()

    before = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
    await runner._process_solana_slot(99999)
    after = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()

    assert after == before
    tip_publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_solana_live_loop_updates_tip_gauge_and_publisher(monkeypatch) -> None:
    """The live `async for slot` loop in _run_solana must update the tip BEFORE
    calling _process_solana_slot."""
    runner, tip_publisher = _build_runner("solana")
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()

    # Make catchup a no-op and yield exactly one slot.
    async def _no_catchup() -> None:
        pass
    monkeypatch.setattr(runner, "_catchup_solana", _no_catchup)

    async def _heads():
        yield 555
        runner._stop.set()  # exit loop after one
    runner._adapter.subscribe_heads = _heads

    # Stub _process_solana_slot to capture if tip was set before it ran.
    seen_tip_at_call = []
    async def _stub_process(slot):
        seen_tip_at_call.append(
            CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
        )
    monkeypatch.setattr(runner, "_process_solana_slot", _stub_process)

    await runner._run_solana()

    assert seen_tip_at_call == [555], "tip must be set before _process_solana_slot"
    tip_publisher.assert_awaited_once_with("test-chain", 555)
