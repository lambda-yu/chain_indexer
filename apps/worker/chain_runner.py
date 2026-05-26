from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Protocol

import structlog

from core.abi.registry import AbiRegistry
from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import BlockHeader
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
)
from core.matcher.matcher import Matcher
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.abi_call import AbiCallParser
from core.parser.abi_event import AbiEventParser
from core.parser.erc20 import Erc20TransferParser
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline, SolanaParserPipeline
from core.parser.sol_native import SolNativeTransferParser

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

    def __init__(
        self,
        *,
        chain: SnapshotChain,
        adapter_factory: AdapterFactory,
        channel_factory: ChannelFactory,
        checkpoint_repo: _CheckpointRepo,
        notifier_max_concurrency: int = 50,
        abi_registry: AbiRegistry | None = None,
    ) -> None:
        self._chain = chain
        self._adapter_factory = adapter_factory
        self._channel_factory = channel_factory
        self._cp = checkpoint_repo
        self._notifier_max_concurrency = notifier_max_concurrency
        self._abi_registry = abi_registry

        self._adapter: Any = None
        self._buffer: ConfirmationBuffer | None = None
        self._evm_pipeline: EvmParserPipeline | None = None
        self._solana_pipeline: SolanaParserPipeline | None = None
        self._matcher: Matcher | None = None
        self._notifier: Notifier | None = None
        self._current_snap: ConfigSnapshot | None = None
        self._buffer_tip_hash: str | None = None
        self._stop = asyncio.Event()
        self._snap_lock = asyncio.Lock()
        self.resume_from: tuple[int, str] | None = None

        self._build_pipeline()

    def _build_pipeline(self) -> None:
        if self._chain.kind == "solana":
            self._solana_pipeline = SolanaParserPipeline([
                SolNativeTransferParser(chain_id=self._chain.id),
            ])
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
        )
        await self._notifier.start(snap.channels)
        self._current_snap = snap

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
        except Exception:  # noqa: BLE001
            log.exception("chain_runner.run_failed", chain_id=self._chain.id)
            raise

    async def _run_evm(self) -> None:
        assert self._buffer is not None
        async for header in self._adapter.subscribe_heads():
            if self._stop.is_set():
                break
            await self._handle_evm_head(header)

    async def _run_solana(self) -> None:
        async for slot in self._adapter.subscribe_heads():
            if self._stop.is_set():
                break
            await self._process_solana_slot(slot)

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
        block = await self._adapter.fetch_block(number)
        events = list(self._evm_pipeline.run(block))
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if not hits:
                continue
            await notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.header.number, block.header.hash)

    async def _process_solana_slot(self, slot: int) -> None:
        assert self._adapter is not None and self._solana_pipeline is not None
        assert self._matcher is not None and self._notifier is not None
        matcher = self._matcher
        notifier = self._notifier

        block = await self._adapter.fetch_block(slot)
        if block is None:
            return
        events = list(self._solana_pipeline.run(block))
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if not hits:
                continue
            await notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.slot, block.block_hash)

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
