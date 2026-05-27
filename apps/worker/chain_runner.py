from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Protocol

import structlog

from core.abi.registry import AbiRegistry
from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import Block, BlockHeader, Log
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
)
from core.matcher.filter_set import EvmLogFilterSet, build_evm_log_filter
from core.matcher.matcher import Matcher
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.abi_call import AbiCallParser
from core.parser.abi_event import AbiEventParser
from core.parser.anchor_call import AnchorIdlCallParser
from core.parser.anchor_event import AnchorIdlEventParser
from core.parser.erc20 import Erc20TransferParser
from core.parser.internal_call import InternalCallParser
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline, SolanaParserPipeline
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_ops import SplOpsParser
from core.parser.spl_transfer import SplTransferParser

log = structlog.get_logger(__name__)


AdapterFactory = Callable[[SnapshotChain], Any]
ChannelFactory = Callable[[SnapshotChannel], Channel]


class _CheckpointRepo(Protocol):
    async def get(self, chain_id: str) -> tuple[int, str] | None: ...
    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None: ...


class ChainRunner:
    """Owns one chain's pipeline. Supports both EVM and Solana chains.

    EVM path: ConfirmationBuffer → EvmParserPipeline → Matcher → Notifier.
    Solana path: direct slot processing → SolanaParserPipeline → Matcher → Notifier.
    """

    DRAIN_TIMEOUT_S = 30.0
    MAX_CATCHUP_BLOCKS = 10_000

    def __init__(
        self,
        *,
        chain: SnapshotChain,
        adapter_factory: AdapterFactory,
        channel_factory: ChannelFactory,
        checkpoint_repo: _CheckpointRepo,
        notifier_max_concurrency: int = 50,
        abi_registry: AbiRegistry | None = None,
        on_send_failure: Any = None,
        on_send_success: Any = None,
        on_block_processed: Any = None,
    ) -> None:
        self._chain = chain
        self._adapter_factory = adapter_factory
        self._channel_factory = channel_factory
        self._cp = checkpoint_repo
        self._notifier_max_concurrency = notifier_max_concurrency
        self._abi_registry = abi_registry
        self._on_send_failure = on_send_failure
        self._on_send_success = on_send_success
        self._on_block_processed = on_block_processed

        self._adapter: Any = None
        self._buffer: ConfirmationBuffer | None = None
        self._evm_pipeline: EvmParserPipeline | None = None
        self._solana_pipeline: SolanaParserPipeline | None = None
        self._matcher: Matcher | None = None
        self._notifier: Notifier | None = None
        self._evm_filter: EvmLogFilterSet | None = None
        self._current_snap: ConfigSnapshot | None = None
        self._buffer_tip_hash: str | None = None
        self._stop = asyncio.Event()
        self._snap_lock = asyncio.Lock()
        self.resume_from: tuple[int, str] | None = None

        self._build_pipeline()

    def _build_pipeline(self) -> None:
        if self._chain.kind == "solana":
            sol_parsers: list[Any] = [
                SolNativeTransferParser(chain_id=self._chain.id),
                SplTransferParser(chain_id=self._chain.id),
                SplOpsParser(chain_id=self._chain.id),
            ]
            if self._abi_registry is not None:
                sol_parsers.append(AnchorIdlEventParser(chain_id=self._chain.id, registry=self._abi_registry))
                sol_parsers.append(AnchorIdlCallParser(chain_id=self._chain.id, registry=self._abi_registry))
            self._solana_pipeline = SolanaParserPipeline(sol_parsers)
        else:
            evm_parsers: list[Any] = [
                EvmNativeTransferParser(chain_id=self._chain.id),
                Erc20TransferParser(chain_id=self._chain.id),
            ]
            if self._abi_registry is not None:
                evm_parsers.append(AbiEventParser(chain_id=self._chain.id, registry=self._abi_registry))
                evm_parsers.append(AbiCallParser(chain_id=self._chain.id, registry=self._abi_registry))
            self._evm_pipeline = EvmParserPipeline(evm_parsers)

    @property
    def _pipeline(self) -> EvmParserPipeline | SolanaParserPipeline:
        if self._evm_pipeline is not None:
            return self._evm_pipeline
        assert self._solana_pipeline is not None
        return self._solana_pipeline

    async def start(self, snap: ConfigSnapshot) -> None:
        self._adapter = self._adapter_factory(self._chain)
        connect = getattr(self._adapter, "connect", None)
        if callable(connect):
            await connect()
        if self._chain.kind != "solana":
            self._buffer = ConfirmationBuffer(confirmations=self._chain.confirmations)
        self.resume_from = await self._cp.get(self._chain.id)
        if self.resume_from is not None:
            log.info(
                "chain_runner.resuming_from_checkpoint",
                chain_id=self._chain.id,
                last_block=self.resume_from[0],
                last_block_hash=self.resume_from[1],
            )
        self._matcher = Matcher(snap)
        self._notifier = Notifier(
            channel_factory=self._channel_factory,
            max_concurrency=self._notifier_max_concurrency,
            on_failure=self._on_send_failure,
            on_success=self._on_send_success,
        )
        await self._notifier.start(snap.channels)
        self._current_snap = snap
        if self._chain.kind != "solana":
            self._evm_filter = build_evm_log_filter(snap, self._chain.id, self._abi_registry)

    async def apply_snapshot(self, snap: ConfigSnapshot) -> None:
        async with self._snap_lock:
            assert self._notifier is not None
            self._matcher = Matcher(snap)
            await self._notifier.stop()
            self._notifier = Notifier(
                channel_factory=self._channel_factory,
                max_concurrency=self._notifier_max_concurrency,
            )
            await self._notifier.start(snap.channels)
            self._current_snap = snap
            if self._chain.kind != "solana":
                self._evm_filter = build_evm_log_filter(snap, self._chain.id, self._abi_registry)
            log.info(
                "chain_runner.snapshot_applied",
                chain_id=self._chain.id,
                version=snap.version,
            )

    async def run(self) -> None:
        assert self._adapter is not None
        try:
            if self._chain.kind == "solana":
                await self._run_solana()
            else:
                await self._run_evm()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.error("chain_runner.run_failed", chain_id=self._chain.id, error=repr(exc))
            raise

    async def _run_evm(self) -> None:
        assert self._buffer is not None
        # 快速追块：从 checkpoint 追到 tip - confirmations
        await self._catchup_evm()
        async for header in self._adapter.subscribe_heads():
            if self._stop.is_set():
                break
            await self._handle_evm_head(header)

    async def _catchup_evm(self) -> None:
        """Process missed blocks between effective start and chain tip."""
        assert self._adapter is not None and self._matcher is not None and self._notifier is not None
        # Determine effective start: min of checkpoint and all subscription start_blocks
        cp_block = self.resume_from[0] if self.resume_from else None
        sub_starts = [s.start_block for s in (self._current_snap.subscriptions if self._current_snap else [])
                      if s.chain_id == self._chain.id and s.start_block is not None and s.enabled]
        candidates = [b for b in [cp_block, *sub_starts] if b is not None]
        if not candidates:
            return
        last_block = min(candidates)
        try:
            tip = await self._adapter.get_latest_block_number()
        except Exception:  # noqa: BLE001
            log.warning("chain_runner.catchup_tip_failed", chain_id=self._chain.id)
            return
        safe_tip = tip - self._chain.confirmations
        gap = safe_tip - last_block
        if gap <= 0:
            return
        if gap > self.MAX_CATCHUP_BLOCKS:
            log.warning("chain_runner.catchup_gap_too_large", chain_id=self._chain.id, gap=gap, max=self.MAX_CATCHUP_BLOCKS, skipping_to=safe_tip - self.MAX_CATCHUP_BLOCKS)
            last_block = safe_tip - self.MAX_CATCHUP_BLOCKS
            gap = self.MAX_CATCHUP_BLOCKS
        log.info("chain_runner.catchup_starting", chain_id=self._chain.id, from_block=last_block + 1, to_block=safe_tip, gap=gap)
        matcher = self._matcher
        notifier = self._notifier
        processed = 0
        for n in range(last_block + 1, safe_tip + 1):
            if self._stop.is_set():
                break
            try:
                await self._process_confirmed_block(n, matcher=matcher, notifier=notifier)
                processed += 1
                if processed % 100 == 0:
                    log.info("chain_runner.catchup_progress", chain_id=self._chain.id, block=n, remaining=safe_tip - n)
            except Exception:  # noqa: BLE001
                log.error("chain_runner.catchup_block_failed", chain_id=self._chain.id, block=n)
                break
        log.info("chain_runner.catchup_done", chain_id=self._chain.id, processed=processed)

    async def _run_solana(self) -> None:
        # 快速追块：从 checkpoint 追到最新 slot
        await self._catchup_solana()
        async for slot in self._adapter.subscribe_heads():
            if self._stop.is_set():
                break
            await self._process_solana_slot(slot)

    async def _catchup_solana(self) -> None:
        assert self._adapter is not None and self._matcher is not None and self._notifier is not None
        cp_slot = self.resume_from[0] if self.resume_from else None
        sub_starts = [s.start_block for s in (self._current_snap.subscriptions if self._current_snap else [])
                      if s.chain_id == self._chain.id and s.start_block is not None and s.enabled]
        candidates = [b for b in [cp_slot, *sub_starts] if b is not None]
        if not candidates:
            return
        last_slot = min(candidates)
        try:
            tip = await self._adapter.get_latest_slot()
        except Exception:  # noqa: BLE001
            log.warning("chain_runner.catchup_tip_failed", chain_id=self._chain.id)
            return
        gap = tip - last_slot
        if gap <= 0:
            return
        if gap > self.MAX_CATCHUP_BLOCKS:
            log.warning("chain_runner.catchup_gap_too_large", chain_id=self._chain.id, gap=gap, max=self.MAX_CATCHUP_BLOCKS)
            last_slot = tip - self.MAX_CATCHUP_BLOCKS
            gap = self.MAX_CATCHUP_BLOCKS
        log.info("chain_runner.catchup_starting", chain_id=self._chain.id, from_slot=last_slot + 1, to_slot=tip, gap=gap)
        matcher = self._matcher
        notifier = self._notifier
        processed = 0
        for s in range(last_slot + 1, tip + 1):
            if self._stop.is_set():
                break
            try:
                await self._process_solana_slot(s)
                processed += 1
                if processed % 100 == 0:
                    log.info("chain_runner.catchup_progress", chain_id=self._chain.id, slot=s, remaining=tip - s)
            except Exception:  # noqa: BLE001
                log.error("chain_runner.catchup_slot_failed", chain_id=self._chain.id, slot=s)
                continue
        log.info("chain_runner.catchup_done", chain_id=self._chain.id, processed=processed)

    async def _handle_evm_head(self, header: BlockHeader) -> None:
        assert self._buffer is not None and self._adapter is not None
        assert self._matcher is not None and self._notifier is not None
        matcher = self._matcher
        notifier = self._notifier

        cache: dict[str, BlockHeader] = {}
        if self._buffer_tip_hash is not None and self._buffer_tip_hash != header.parent_hash:
            cache = await self._prefetch_ancestors_for(header)

        def resolve_parent(n: int, h: str) -> BlockHeader:
            try:
                return cache[h]
            except KeyError as e:
                raise KeyError(f"ancestor {h} at height {n} not in prefetch cache") from e

        result = self._buffer.handle_new_head(header, resolve_parent=resolve_parent)
        self._buffer_tip_hash = header.hash

        confirmed: list[BlockHeader]
        if isinstance(result, ReorgEvent):
            if result.deep:
                log.error(
                    "chain_runner.deep_reorg",
                    chain_id=self._chain.id,
                    divergent_oldest=result.divergent_oldest,
                    new_head=result.new_head.number if result.new_head else None,
                )
            confirmed = result.confirmed
        else:
            confirmed = result

        for h in confirmed:
            await self._process_confirmed_block(h.number, matcher=matcher, notifier=notifier)

    async def _prefetch_ancestors_for(self, header: BlockHeader) -> dict[str, BlockHeader]:
        assert self._adapter is not None
        depth = max(1, self._chain.confirmations + 1)
        out: dict[str, BlockHeader] = {}
        for i in range(1, depth + 1):
            n = header.number - i
            if n < 0:
                break
            try:
                blk = await self._adapter.fetch_block(n)
            except Exception:  # noqa: BLE001
                log.warning("chain_runner.prefetch_failed", chain_id=self._chain.id, height=n)
                break
            out[blk.header.hash] = blk.header
        return out

    async def _process_confirmed_block(
        self,
        number: int,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        assert self._adapter is not None and self._evm_pipeline is not None
        assert self._evm_filter is not None
        log_filter = self._evm_filter

        block_coro = self._adapter.fetch_block(number)
        if log_filter.skip_logs:
            block = await block_coro
            logs: list[Log] = []
        else:
            logs_coro = self._adapter.fetch_logs(
                number, number,
                addresses=log_filter.addresses,
                topics=log_filter.topics_param,
            )
            block, logs = await asyncio.gather(block_coro, logs_coro)

        await self._process_block_with_prefetched_logs(
            number, block, logs, matcher=matcher, notifier=notifier,
        )

    async def _process_block_with_prefetched_logs(
        self,
        number: int,
        block: Block,
        prefetched_logs: list[Log],
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        assert self._adapter is not None and self._evm_pipeline is not None
        from dataclasses import replace
        block = replace(block, logs=prefetched_logs)

        events = list(self._evm_pipeline.run(block))

        # 优化2: 所有 event dispatch 并发而非串行
        dispatch_tasks: list[asyncio.Task[None]] = []
        matched_sub_ids: set[str] = set()
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if hits:
                dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))
                for sub, _ in hits:
                    matched_sub_ids.add(sub.id)

        if self._chain.trace_internal_calls and self._abi_registry is not None:
            trace_fn = getattr(self._adapter, "trace_block", None)
            if callable(trace_fn):
                traces = await trace_fn(number)
                if traces:
                    internal_parser = InternalCallParser(chain_id=self._chain.id, registry=self._abi_registry)
                    for event in internal_parser.parse(traces, block):
                        hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
                        if hits:
                            dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))

        if dispatch_tasks:
            await asyncio.gather(*dispatch_tasks, return_exceptions=True)

        await self._cp.save(self._chain.id, block.header.number, block.header.hash)
        if self._on_block_processed and matched_sub_ids:
            try:
                await self._on_block_processed(matched_sub_ids, block.header.number)
            except Exception:  # noqa: BLE001
                pass

    async def _process_solana_slot(self, slot: int) -> None:
        assert self._adapter is not None and self._solana_pipeline is not None
        assert self._matcher is not None and self._notifier is not None
        matcher = self._matcher
        notifier = self._notifier

        block = await self._adapter.fetch_block(slot)
        if block is None:
            return
        events = list(self._solana_pipeline.run(block))
        # 优化: 并发 dispatch
        dispatch_tasks: list[asyncio.Task[None]] = []
        matched_sub_ids: set[str] = set()
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if hits:
                dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))
                for sub, _ in hits:
                    matched_sub_ids.add(sub.id)
        if dispatch_tasks:
            await asyncio.gather(*dispatch_tasks, return_exceptions=True)
        await self._cp.save(self._chain.id, block.slot, block.block_hash)
        if self._on_block_processed and matched_sub_ids:
            try:
                await self._on_block_processed(matched_sub_ids, block.slot)
            except Exception:  # noqa: BLE001
                pass

    async def stop(self) -> None:
        self._stop.set()
        if self._notifier is not None:
            try:
                await asyncio.wait_for(self._notifier.stop(), timeout=self.DRAIN_TIMEOUT_S)
            except TimeoutError:
                log.warning("chain_runner.notifier_drain_timeout", chain_id=self._chain.id)
        if self._adapter is not None:
            try:
                await self._adapter.disconnect()
            except Exception:  # noqa: BLE001
                log.exception("chain_runner.adapter_disconnect_failed", chain_id=self._chain.id)
