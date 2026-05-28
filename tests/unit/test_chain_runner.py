from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest

from apps.worker.chain_runner import ChainRunner
from core.abi.registry import AbiRegistry as _AbiRegistry
from core.chains.types import Block, BlockHeader, Log, Tx
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.config.snapshot import (
    SnapshotAbi as _SnapshotAbi,
)
from core.notifier.channel import Channel
from core.parser.erc20 import ERC20_TRANSFER_TOPIC0


def _chain() -> SnapshotChain:
    return SnapshotChain(
        id="eth-test",
        kind="evm",
        rpc_http="http://x",
        rpc_ws=None,
        confirmations=2,
        poll_interval_ms=10,
    )


def _sub(channel_ids: list[str], **overrides: Any) -> SnapshotSubscription:
    base: dict[str, Any] = dict(
        id="s1",
        name="sub",
        chain_id="eth-test",
        address=None,
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
        channel_ids=channel_ids,
    )
    base.update(overrides)
    return SnapshotSubscription(**base)


def _ch(id_: str = "c1") -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="hook", type="collect-runner", config={})


class _CollectingChannel(Channel):
    type = "collect-runner"
    config_schema: dict = {}

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def send(self, payload: dict[str, Any]) -> None:
        self.calls.append(payload)


class _FakeAdapter:
    """Stand-in for EvmAdapter. Drives BlockHeaders from a test-controlled queue.

    Matches the EvmAdapter lifecycle from Chunk 3: explicit `connect()` /
    `disconnect()` calls. `fetch_block(n)` is the only I/O ChainRunner needs;
    `subscribe_heads()` returns an unbounded async generator that the test
    cancels via `task.cancel()`.
    """

    chain_id = "eth-test"
    confirmations = 2

    def __init__(self, blocks: list[Block]) -> None:
        self._blocks = {b.header.number: b for b in blocks}
        self._head_q: asyncio.Queue[BlockHeader] = asyncio.Queue()
        self.connected = False

    def add_block(self, block: Block) -> None:
        """Add a block to the fetch-by-number index BEFORE pushing its head."""
        self._blocks[block.header.number] = block

    async def connect(self) -> None:
        self.connected = True

    async def push_head(self, header: BlockHeader) -> None:
        await self._head_q.put(header)

    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        async def _gen() -> AsyncIterator[BlockHeader]:
            while True:
                yield await self._head_q.get()

        return _gen()

    async def fetch_block(self, number: int) -> Block:
        return self._blocks[number]

    async def fetch_logs(
        self,
        from_block: int,
        to_block: int,
        addresses: list[str] | None = None,
        topics: list[list[str]] | None = None,
    ) -> list[Any]:
        out: list[Any] = []
        for n in range(from_block, to_block + 1):
            b = self._blocks.get(n)
            if b is not None:
                out.extend(b.logs)
        return out

    async def get_latest_block_number(self) -> int:
        return max(self._blocks) if self._blocks else 0

    async def disconnect(self) -> None:
        self.connected = False


def _hdr(n: int, parent: str = "0xp") -> BlockHeader:
    return BlockHeader(number=n, hash=f"0xh{n}", parent_hash=parent, timestamp=n * 10)


def _block_with_native(n: int, value_wei: int, to: str = "0xdead") -> Block:
    return Block(
        header=_hdr(n, parent=f"0xh{n - 1}" if n > 0 else "0x0"),
        txs=[
            Tx(
                hash=f"0xt{n}",
                index=0,
                from_addr="0xc0ffee",
                to_addr=to,
                value=value_wei,
                input="0x",
                status=1,
            )
        ],
        logs=[],
    )


class _CheckpointStub:
    """In-memory stand-in for the checkpoint repo."""

    def __init__(self, initial: tuple[int, str] | None = None) -> None:
        self.value = initial
        self.saves: list[tuple[int, str]] = []

    async def get(self, _chain_id: str) -> tuple[int, str] | None:
        return self.value

    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None:
        self.value = (last_block, last_block_hash)
        self.saves.append((last_block, last_block_hash))


@pytest.mark.asyncio
async def test_chain_runner_dispatches_native_transfer_through_pipeline() -> None:
    chain = _chain()
    blocks = [_block_with_native(n, value_wei=10**18) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"])],
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        # block 1 + 2 are inside the confirmation window (depth<2) and not emitted yet.
        # block 3 confirms block 1; block 4 confirms block 2.
        # Expect 2 dispatches.
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        assert {c["event"]["block_number"] for c in coll.calls} == {1, 2}
        assert cp.value == (2, "0xh2")  # last checkpoint = last confirmed block
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_chain_runner_apply_snapshot_swaps_subscriptions_live() -> None:
    chain = _chain()
    blocks = [_block_with_native(n, value_wei=10**18) for n in (1, 2, 3)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    initial_snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], enabled=False)],  # disabled at start
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(initial_snap)
    task = asyncio.create_task(runner.run())
    try:
        # Push block 1; should NOT dispatch (subscription disabled).
        await adapter.push_head(blocks[0].header)
        await adapter.push_head(blocks[1].header)
        await adapter.push_head(blocks[2].header)
        await asyncio.sleep(0.1)
        assert len(coll.calls) == 0

        # Hot-reload: enable the subscription.
        new_snap = ConfigSnapshot(
            version=2,
            chains=[chain],
            subscriptions=[_sub(channel_ids=["c1"], enabled=True)],
            channels=[_ch("c1")],
        )
        await runner.apply_snapshot(new_snap)

        # Register block 4 BEFORE pushing its header so fetch_block won't KeyError.
        adapter.add_block(_block_with_native(4, value_wei=10**18))
        await adapter.push_head(_hdr(4, parent="0xh3"))
        # block 2 now sits at depth 2 (tip=4) and will dispatch under the new snapshot.
        # block 1 was already confirmed before the reload and is NOT replayed
        # (apply_snapshot doesn't rewind history — documented contract).
        for _ in range(30):
            if coll.calls:
                break
            await asyncio.sleep(0.02)
        assert any(c["event"]["block_number"] == 2 for c in coll.calls)
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_chain_runner_seeds_buffer_from_checkpoint() -> None:
    chain = _chain()
    adapter = _FakeAdapter(blocks=[])
    cp = _CheckpointStub(initial=(42, "0xh42"))
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=cp,
    )
    snap = ConfigSnapshot(version=1, chains=[chain], subscriptions=[], channels=[])
    await runner.start(snap)
    try:
        assert runner.resume_from == (42, "0xh42")
    finally:
        await runner.stop()


def _block_with_erc20_log(n: int, *, value: int = 1000) -> Block:
    pad = "0" * 24
    _from = "aaaa" + "00" * 18
    _to = "bbbb" + "00" * 18
    return Block(
        header=_hdr(n, parent=f"0xh{n-1}" if n > 0 else "0x0"),
        txs=[
            Tx(hash=f"0xt{n}", index=0, from_addr="0xf0", to_addr="0xtoken",
               value=0, input="0xa9059cbb", status=1),
        ],
        logs=[
            Log(
                tx_hash=f"0xt{n}",
                log_index=0,
                address="0xtoken",
                topics=[
                    ERC20_TRANSFER_TOPIC0,
                    "0x" + pad + _from,
                    "0x" + pad + _to,
                ],
                data="0x" + format(value, "064x"),
                block_number=n,
            ),
        ],
    )


@pytest.mark.asyncio
async def test_chain_runner_dispatches_erc20_token_transfer() -> None:
    chain = _chain()
    blocks = [_block_with_erc20_log(n, value=n * 1000) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], match_kind="token_transfer")],
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        kinds = {c["event"]["kind"] for c in coll.calls}
        assert kinds == {"token_transfer"}
        values = {c["event"]["args"]["value"] for c in coll.calls}
        assert values == {"1000", "2000"}
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


_TRANSFER_ABI = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from",  "type": "address", "indexed": True},
        {"name": "to",    "type": "address", "indexed": True},
        {"name": "value", "type": "uint256", "indexed": False},
    ],
}


def test_chain_runner_pipeline_includes_abi_event_parser_when_registry_given() -> None:
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    chain = _chain()
    runner_with = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
        abi_registry=reg,
    )
    types_with = [type(p).__name__ for p in runner_with._pipeline._parsers]
    assert "AbiEventParser" in types_with

    runner_without = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    types_without = [type(p).__name__ for p in runner_without._pipeline._parsers]
    assert "AbiEventParser" not in types_without
    assert "EvmNativeTransferParser" in types_with and "EvmNativeTransferParser" in types_without
    assert "Erc20TransferParser" in types_with and "Erc20TransferParser" in types_without


@pytest.mark.asyncio
async def test_chain_runner_dispatches_abi_event_match() -> None:
    chain = _chain()
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    blocks = [_block_with_erc20_log(n, value=n * 100) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], match_kind="event", match_name="Transfer")],
        channels=[_ch("c1")],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
        abi_registry=reg,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        names = {c["event"]["name"] for c in coll.calls}
        assert names == {"Transfer"}
        kinds = {c["event"]["kind"] for c in coll.calls}
        assert kinds == {"event"}
        values = {c["event"]["args"]["value"] for c in coll.calls}
        assert values == {"100", "200"}
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def test_chain_runner_pipeline_includes_abi_call_parser_when_registry_given() -> None:
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    chain = _chain()
    runner_with = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
        abi_registry=reg,
    )
    types_with = [type(p).__name__ for p in runner_with._pipeline._parsers]
    assert "AbiEventParser" in types_with
    assert "AbiCallParser" in types_with

    runner_without = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    types_without = [type(p).__name__ for p in runner_without._pipeline._parsers]
    assert "AbiCallParser" not in types_without
    assert "AbiEventParser" not in types_without
    assert "EvmNativeTransferParser" in types_with and "EvmNativeTransferParser" in types_without
    assert "Erc20TransferParser" in types_with and "Erc20TransferParser" in types_without


@pytest.mark.asyncio
async def test_head_following_passes_filter_to_fetch_logs() -> None:
    """ChainRunner with a token_transfer subscription should call fetch_logs
    with the ERC-20 topic0 in `topics`."""
    from core.matcher.filter_set import ERC20_TRANSFER_TOPIC0

    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="eth-test", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[_ch("c1")],
        chains=[_chain()],
        abis=[],
    )

    captured: dict = {}

    class _CapturingAdapter(_FakeAdapter):
        async def fetch_logs(
            self, from_block, to_block,
            addresses=None, topics=None,
        ):
            captured["from"] = from_block
            captured["to"] = to_block
            captured["addresses"] = addresses
            captured["topics"] = topics
            return []

    adapter = _CapturingAdapter(blocks=[_block_with_native(42, value_wei=0)])
    runner = ChainRunner(
        chain=_chain(),
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    await runner.start(snap)
    await runner._process_confirmed_block(
        42, matcher=runner._matcher, notifier=runner._notifier,
    )
    assert captured["from"] == 42
    assert captured["to"] == 42
    assert captured["addresses"] == ["0xaaa"]
    assert captured["topics"] == [[ERC20_TRANSFER_TOPIC0]]


@pytest.mark.asyncio
async def test_head_following_skips_logs_when_no_log_subscription() -> None:
    """If only native_transfer subs exist, fetch_logs should never be called."""
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="eth-test", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[_ch("c1")],
        chains=[_chain()],
        abis=[],
    )

    fetch_logs_called = {"n": 0}

    class _NoLogsAdapter(_FakeAdapter):
        async def fetch_logs(self, *a, **kw):
            fetch_logs_called["n"] += 1
            return []

    adapter = _NoLogsAdapter(blocks=[_block_with_native(7, value_wei=0)])
    runner = ChainRunner(
        chain=_chain(),
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    await runner.start(snap)
    await runner._process_confirmed_block(
        7, matcher=runner._matcher, notifier=runner._notifier,
    )
    assert fetch_logs_called["n"] == 0
