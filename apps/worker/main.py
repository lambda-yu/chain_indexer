from __future__ import annotations

import asyncio
import contextlib
import signal

import structlog

from apps.worker.chain_runner import ChainRunner
from apps.worker.config_watcher import ConfigWatcher
from core.bus.redis_bus import RedisBus
from core.chains.evm import EvmAdapter
from core.config.db import Database
from core.config.repositories import CheckpointRepo
from core.config.snapshot import ConfigSnapshot, SnapshotChain, SnapshotChannel, load_snapshot
from core.logging import configure_logging
from core.notifier.channel import CHANNEL_REGISTRY, Channel
from core.notifier.http import HttpChannel  # noqa: F401 — side-effect: register http
from core.settings import Settings, load_settings

log = structlog.get_logger(__name__)


def _default_adapter_factory(cfg: SnapshotChain) -> EvmAdapter:
    if cfg.kind != "evm":
        # Chunk 10 (Solana) extends this branch. M1 hard-fails so misconfigs are loud.
        raise NotImplementedError(f"chain kind {cfg.kind!r} not supported yet")
    return EvmAdapter(
        chain_id=cfg.id,
        rpc_http=cfg.rpc_http,
        rpc_ws=cfg.rpc_ws,
        confirmations=cfg.confirmations,
        poll_interval_ms=cfg.poll_interval_ms,
    )


def _default_channel_factory(cfg: SnapshotChannel) -> Channel:
    cls = CHANNEL_REGISTRY[cfg.type]
    return cls(config=cfg.config)  # type: ignore[call-arg]


class _CheckpointAdapter:
    """Bridges the ChainRunner's `(get/save) -> tuple[int,str]` contract to
    Chunk 2's `CheckpointRepo` (returns ORM row, uses `upsert`, no commit).
    Opens its own session per call so multiple runners don't share one.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    async def get(self, chain_id: str) -> tuple[int, str] | None:
        async with self._db.session() as s:
            row = await CheckpointRepo(s).get(chain_id)
            if row is None:
                return None
            return (row.last_block, row.last_block_hash)

    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None:
        async with self._db.session() as s:
            await CheckpointRepo(s).upsert(
                chain_id,
                last_block=last_block,
                last_block_hash=last_block_hash,
            )
            await s.commit()


class _Worker:
    """Holds the shared DB / bus / watcher and a map of chain_id → (runner, task)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._db = Database(settings.database.url, echo=settings.database.echo)
        self._bus = RedisBus(url=settings.redis.url)
        self._checkpoint_adapter = _CheckpointAdapter(self._db)
        self._snap_queue: asyncio.Queue[ConfigSnapshot] = asyncio.Queue(maxsize=8)
        self._watcher: ConfigWatcher | None = None
        self._runners: dict[str, tuple[ChainRunner, asyncio.Task[None]]] = {}
        self._stop = asyncio.Event()

    async def start(self) -> None:
        await self._db.connect()
        await self._bus.connect()
        self._watcher = ConfigWatcher(
            bus=self._bus,
            session_factory=self._db.session,
            load_snapshot=load_snapshot,
            out_queue=self._snap_queue,
            poll_interval_s=5.0,
        )
        await self._watcher.start()

    async def run(self) -> None:
        """Main loop: dequeue snapshots, reconcile runners, exit on _stop."""
        while not self._stop.is_set():
            snap = await self._dequeue_snapshot_or_stop()
            if snap is None:
                return
            await self._reconcile(snap)

    async def _dequeue_snapshot_or_stop(self) -> ConfigSnapshot | None:
        get_task = asyncio.create_task(self._snap_queue.get())
        stop_task = asyncio.create_task(self._stop.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done:
                get_task.cancel()
                return None
            stop_task.cancel()
            return get_task.result()
        finally:
            for t in (get_task, stop_task):
                if not t.done():
                    t.cancel()

    async def _reconcile(self, snap: ConfigSnapshot) -> None:
        enabled = {c.id: c for c in snap.chains}
        for chain_id in list(self._runners):
            if chain_id not in enabled:
                await self._stop_runner(chain_id)
        for chain_id, cfg in enabled.items():
            if chain_id in self._runners:
                runner, _ = self._runners[chain_id]
                await runner.apply_snapshot(snap)
            else:
                runner = ChainRunner(
                    chain=cfg,
                    adapter_factory=_default_adapter_factory,
                    channel_factory=_default_channel_factory,
                    checkpoint_repo=self._checkpoint_adapter,
                )
                await runner.start(snap)
                task = asyncio.create_task(runner.run(), name=f"chain-runner:{chain_id}")
                self._runners[chain_id] = (runner, task)
                log.info("worker.chain_runner_started", chain_id=chain_id)

    async def _stop_runner(self, chain_id: str) -> None:
        runner, task = self._runners.pop(chain_id)
        # stop() sets the runner's _stop event so its `async for header` exits
        # on the next head; cancel() is the safety net for runners blocked in
        # subscribe_heads() with no traffic.
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        log.info("worker.chain_runner_stopped", chain_id=chain_id)

    async def shutdown(self) -> None:
        """Trigger graceful drain per spec §9.1. Idempotent and safe to call
        after a partially-failed `start()` — `RedisBus.disconnect()` and
        `Database.disconnect()` both guard on connection state.

        Runner stops are issued in parallel (each runner has its own ~30s drain
        timeout; sequential shutdown would compound to N×30s). Bus/DB
        disconnect must happen strictly AFTER all runners stop so a runner
        cannot publish during teardown.
        """
        if self._stop.is_set():
            return
        self._stop.set()
        log.info("worker.shutdown_starting")
        if self._watcher is not None:
            await self._watcher.stop()
        if self._runners:
            await asyncio.gather(
                *(self._stop_runner(cid) for cid in list(self._runners)),
                return_exceptions=False,
            )
        await self._bus.disconnect()
        await self._db.disconnect()
        log.info("worker.shutdown_complete")


async def run_worker(settings: Settings, stop_event: asyncio.Event) -> None:
    """Public coroutine that boots a `_Worker` and runs until `stop_event` is set.

    Does NOT install signal handlers — the caller is responsible for triggering
    `stop_event` (E2E tests do this directly; the CLI entry point does it from
    SIGTERM/SIGINT handlers via `_amain` below).

    If `worker.start()` fails partway through, `shutdown()` is called to
    release any resources that were already acquired (DB pool, Redis pool).
    """
    worker = _Worker(settings)
    try:
        await worker.start()
    except BaseException:
        await worker.shutdown()
        raise
    run_task = asyncio.create_task(worker.run(), name="worker-main-loop")
    stop_task = asyncio.create_task(stop_event.wait(), name="worker-stop-wait")
    try:
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_task.cancel()
        await worker.shutdown()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task


async def _amain() -> None:
    settings = load_settings()
    configure_logging(level=settings.logging.level, format=settings.logging.format)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.signal_received", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    await run_worker(settings, stop_event)


def main() -> None:
    """Console-script entry point (referenced from pyproject.toml `[project.scripts]`)."""
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
