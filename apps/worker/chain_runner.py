from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Protocol

import structlog

from core.chains.adapter import ChainAdapter
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
from core.parser.native import NativeTransferParser
from core.parser.pipeline import ParserPipeline

log = structlog.get_logger(__name__)


AdapterFactory = Callable[[SnapshotChain], ChainAdapter]
ChannelFactory = Callable[[SnapshotChannel], Channel]


class _CheckpointRepo(Protocol):
    async def get(self, chain_id: str) -> tuple[int, str] | None: ...
    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None: ...


class ChainRunner:
    """Owns one chain's pipeline.

    Lifecycle:
      1. `start(snap)` — construct adapter (and `await adapter.connect()`),
         confirmation buffer, parser, matcher, notifier; seed `resume_from`
         from the persisted checkpoint.
      2. `run()` — drive `subscribe_heads()` through the buffer, parse + match
         + dispatch each confirmed block, save checkpoint per block.
      3. `apply_snapshot(snap)` — rebuild matcher index + notifier channel set
         in place (no listener restart).
      4. `stop()` — cancel listener, drain in-flight notifications (<=30s),
         disconnect adapter.

    ConfirmationBuffer note (Chunk 4): `handle_new_head` is SYNC and takes a
    SYNC `resolve_parent(n, h) -> BlockHeader`. ChainRunner pre-fetches
    ancestors via async I/O *before* calling the buffer, then passes a cache
    lookup as the sync resolver. Pre-fetch only runs when the new head's
    `parent_hash` doesn't match our mirrored buffer tip.
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
    ) -> None:
        self._chain = chain
        self._adapter_factory = adapter_factory
        self._channel_factory = channel_factory
        self._cp = checkpoint_repo
        self._notifier_max_concurrency = notifier_max_concurrency

        self._adapter: ChainAdapter | None = None
        self._buffer: ConfirmationBuffer | None = None
        self._pipeline = ParserPipeline([NativeTransferParser(chain_id=self._chain.id)])
        self._matcher: Matcher | None = None
        self._notifier: Notifier | None = None
        self._current_snap: ConfigSnapshot | None = None
        self._buffer_tip_hash: str | None = None  # mirrors buffer's rightmost header
        self._stop = asyncio.Event()
        self._snap_lock = asyncio.Lock()
        self.resume_from: tuple[int, str] | None = None

    async def start(self, snap: ConfigSnapshot) -> None:
        self._adapter = self._adapter_factory(self._chain)
        # EvmAdapter (Chunk 3) requires an explicit connect() before any RPC call.
        connect = getattr(self._adapter, "connect", None)
        if callable(connect):
            await connect()
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
            # For M1 the cheap path is stop-then-start; HttpChannel instances
            # are cheap. M4 (MQ) will need a diff to avoid bouncing live AMQP
            # connections.
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
        assert self._adapter is not None and self._buffer is not None
        try:
            async for header in self._adapter.subscribe_heads():
                if self._stop.is_set():
                    break
                await self._handle_head(header)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("chain_runner.run_failed", chain_id=self._chain.id)
            raise

    async def _handle_head(self, header: BlockHeader) -> None:
        assert self._buffer is not None and self._adapter is not None
        # Capture matcher/notifier refs once per head so a concurrent
        # apply_snapshot() swap doesn't drop events on a half-rebuilt notifier.
        # Matches the documented contract: snapshot swaps don't replay history,
        # and any block already in flight finishes under the snapshot it started.
        assert self._matcher is not None and self._notifier is not None
        matcher = self._matcher
        notifier = self._notifier

        # Pre-fetch ancestors only when the head doesn't link cleanly. The
        # buffer's sync resolver then becomes a pure dict lookup.
        cache: dict[str, BlockHeader] = {}
        if self._buffer_tip_hash is not None and self._buffer_tip_hash != header.parent_hash:
            cache = await self._prefetch_ancestors_for(header)

        def resolve_parent(n: int, h: str) -> BlockHeader:
            try:
                return cache[h]
            except KeyError as e:
                # Buffer treats a missing ancestor as a deep reorg (exhausts walk).
                # We translate to KeyError for clarity in logs.
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
            confirmed = result  # list[BlockHeader]

        for h in confirmed:
            await self._process_confirmed_block(h.number, matcher=matcher, notifier=notifier)

    async def _prefetch_ancestors_for(self, header: BlockHeader) -> dict[str, BlockHeader]:
        """Fetch up to `confirmations + 1` blocks at the heights below `header`
        and index them by hash. The RPC node has typically already reorged, so
        `fetch_block(n)` returns the new fork's header at height `n`.
        """
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
        assert self._adapter is not None
        block = await self._adapter.fetch_block(number)
        events = list(self._pipeline.run(block))
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if not hits:
                continue
            await notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.header.number, block.header.hash)

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
