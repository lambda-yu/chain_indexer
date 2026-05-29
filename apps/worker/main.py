from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Callable

import structlog

import uuid as _uuid_mod

from apps.worker.chain_runner import ChainRunner
from apps.worker.config_watcher import ConfigWatcher
from core.abi.registry import AbiRegistry
from core.bus.redis_bus import RedisBus
from core.chains.evm import EvmAdapter
from core.chains.solana import SolanaAdapter
from core.config.db import Database
from core.config.repositories import CheckpointRepo
from core.config.snapshot import ConfigSnapshot, SnapshotChain, SnapshotChannel, load_snapshot
from core.logging import configure_logging
from core.notifier.channel import CHANNEL_REGISTRY, Channel
from core.notifier.http import HttpChannel  # noqa: F401 — side-effect: register http
from core.notifier.kafka import KafkaChannel  # noqa: F401 — side-effect: register kafka
from core.notifier.rabbitmq import RabbitMQChannel  # noqa: F401 — side-effect: register rabbitmq
from core.notifier.redis_streams import RedisStreamsChannel  # noqa: F401 — side-effect: register mq
from core.notifier.websocket import WebSocketChannel  # noqa: F401 — side-effect: register ws
from core.settings import Settings, load_settings

log = structlog.get_logger(__name__)


def _default_adapter_factory(cfg: SnapshotChain) -> EvmAdapter | SolanaAdapter:
    if cfg.kind == "evm":
        return EvmAdapter(
            chain_id=cfg.id,
            rpc_http=cfg.rpc_http,
            rpc_ws=cfg.rpc_ws,
            confirmations=cfg.confirmations,
            poll_interval_ms=cfg.poll_interval_ms,
        )
    if cfg.kind == "solana":
        assert cfg.commitment is not None, "Solana chain must have commitment set"
        return SolanaAdapter(
            chain_id=cfg.id,
            rpc_http=cfg.rpc_http,
            commitment=cfg.commitment,
            poll_interval_ms=cfg.poll_interval_ms,
            rpc_ws=cfg.rpc_ws,
        )
    raise NotImplementedError(f"chain kind {cfg.kind!r} not supported")


def _make_channel_factory(bus: RedisBus) -> Callable[[SnapshotChannel], Channel]:
    def factory(cfg: SnapshotChannel) -> Channel:
        cls = CHANNEL_REGISTRY[cfg.type]
        return cls(config=cfg.config, bus=bus)  # type: ignore[call-arg]
    return factory


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

    def __init__(
        self,
        settings: Settings,
        *,
        ready_event: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._db = Database(settings.database.url, echo=settings.database.echo)
        self._bus = RedisBus(url=settings.redis.url)
        self._checkpoint_adapter = _CheckpointAdapter(self._db)
        self._registry = AbiRegistry()
        self._snap_queue: asyncio.Queue[ConfigSnapshot] = asyncio.Queue(maxsize=8)
        self._watcher: ConfigWatcher | None = None
        self._runners: dict[str, tuple[ChainRunner, asyncio.Task[None]]] = {}
        self._locks: dict[str, Any] = {}
        self._worker_id = str(_uuid_mod.uuid4())[:8]
        self._stop = asyncio.Event()
        self._ready = ready_event
        self._cleanup_task: asyncio.Task[None] | None = None

    async def _on_delivery_failure(
        self, subscription_id: str, channel_id: str, chain_id: str,
        payload: dict, error: str, attempts: int,
    ) -> None:
        from core.config.repositories import DeliveryRecordRepo
        try:
            async with self._db.session() as s:
                await DeliveryRecordRepo(s).create(
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    chain_id=chain_id,
                    event_payload=payload,
                    error=error,
                    attempts=attempts,
                    status="failed",
                )
                await s.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("worker.failed_delivery_save_error", error=repr(exc))

    async def _on_delivery_success(
        self, subscription_id: str, channel_id: str, chain_id: str,
        payload: dict, _error: None, _attempts: int,
    ) -> None:
        from core.config.repositories import DeliveryRecordRepo
        try:
            async with self._db.session() as s:
                await DeliveryRecordRepo(s).create(
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    chain_id=chain_id,
                    event_payload=payload,
                    status="success",
                )
                await s.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("worker.delivery_success_save_error", error=repr(exc))

    async def _on_block_processed(self, sub_ids: set[str], block_number: int) -> None:
        from core.config.repositories import SubscriptionRepo
        try:
            async with self._db.session() as s:
                repo = SubscriptionRepo(s)
                for sid in sub_ids:
                    await repo.update(sid, last_processed_block=block_number)
                await s.commit()
        except Exception:  # noqa: BLE001
            pass

    async def _publish_tip(self, chain_id: str, block_number: int) -> None:
        """Write chain:{chain_id}:tip to Redis (TTL 60s) for the web's /lag endpoint.

        Errors are logged and swallowed: a Redis hiccup must not kill the
        chain runner. The Dashboard chip just goes ⚪ "unknown" until the
        next live head re-publishes the key.
        """
        try:
            await self._bus.client.set(
                f"chain:{chain_id}:tip", block_number, ex=60,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "worker.publish_tip_failed", chain_id=chain_id, error=repr(exc),
            )

    async def _run_cleanup_loop(self) -> None:
        """Periodically delete oldest status='success' delivery_records rows
        so the table stays under settings.delivery_records.max_success_rows.

        Failed/retrying/resolved rows are never touched.
        """
        from core.config.repositories import DeliveryRecordRepo
        cfg = self._settings.delivery_records
        while not self._stop.is_set():
            try:
                async with self._db.session() as s:
                    deleted = await DeliveryRecordRepo(s).cleanup_success(
                        keep=cfg.max_success_rows,
                        batch=cfg.cleanup_batch_size,
                    )
                    await s.commit()
                if deleted > 0:
                    log.info(
                        "delivery_records.cleanup_done",
                        deleted=deleted,
                        keep=cfg.max_success_rows,
                    )
            except Exception:  # noqa: BLE001
                log.exception("delivery_records.cleanup_error")
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=cfg.cleanup_interval_seconds,
                )
            except TimeoutError:
                pass  # interval elapsed; loop again

    async def start(self) -> None:
        log.info("worker.starting", worker_id=self._worker_id)
        await self._db.connect()
        await self._bus.connect()
        from core.logging import set_log_redis_client
        set_log_redis_client(self._bus.client)
        self._watcher = ConfigWatcher(
            bus=self._bus,
            session_factory=self._db.session,
            load_snapshot=load_snapshot,
            out_queue=self._snap_queue,
            poll_interval_s=5.0,
        )
        await self._watcher.start()
        # Prometheus metrics server (daemon thread; survives until process exit)
        if self._settings.metrics.enabled:
            from importlib.metadata import PackageNotFoundError
            from importlib.metadata import version as pkg_version

            from prometheus_client import start_http_server

            from core.metrics import WORKER_INFO, WORKER_UP

            start_http_server(self._settings.metrics.port)
            WORKER_UP.set(1)
            try:
                v = pkg_version("chain-indexer")
            except PackageNotFoundError:
                v = "dev"
            WORKER_INFO.labels(worker_id=self._worker_id, version=v).set(1)
            log.info(
                "worker.metrics_server_started",
                port=self._settings.metrics.port,
            )
        self._cleanup_task = asyncio.create_task(
            self._run_cleanup_loop(), name="delivery_records_cleanup",
        )

    async def run(self) -> None:
        """Main loop: dequeue snapshots, reconcile runners, exit on _stop.
        Sets `_ready` after the first reconcile completes (i.e. all enabled
        chains have started)."""
        first_reconcile_done = False
        while not self._stop.is_set():
            snap = await self._dequeue_snapshot_or_stop()
            if snap is None:
                return
            await self._reconcile(snap)
            if not first_reconcile_done:
                first_reconcile_done = True
                if self._ready is not None:
                    self._ready.set()

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
        from core.worker.chain_lock import ChainLock
        self._registry.refresh(snap)
        enabled = {c.id: c for c in snap.chains}
        # Stop runners for disabled/removed chains + release locks
        for chain_id in list(self._runners):
            if chain_id not in enabled:
                await self._stop_runner(chain_id)
                if chain_id in self._locks:
                    await self._locks.pop(chain_id).release()
        for chain_id, cfg in enabled.items():
            if chain_id in self._runners:
                runner, _ = self._runners[chain_id]
                await runner.apply_snapshot(snap)
            else:
                # Try to acquire distributed lock
                lock = ChainLock(self._bus.client, chain_id, self._worker_id)
                if not await lock.acquire():
                    continue
                self._locks[chain_id] = lock
                runner = ChainRunner(
                    chain=cfg,
                    adapter_factory=_default_adapter_factory,
                    channel_factory=_make_channel_factory(self._bus),
                    checkpoint_repo=self._checkpoint_adapter,
                    abi_registry=self._registry,
                    on_send_failure=self._on_delivery_failure,
                    on_send_success=self._on_delivery_success,
                    on_block_processed=self._on_block_processed,
                )
                try:
                    await runner.start(snap)
                except Exception as exc:  # noqa: BLE001
                    log.error("worker.chain_runner_start_failed", chain_id=chain_id, error=repr(exc))
                    await lock.release()
                    self._locks.pop(chain_id, None)
                    continue
                task = asyncio.create_task(runner.run(), name=f"chain-runner:{chain_id}")
                self._runners[chain_id] = (runner, task)
                log.info("worker.chain_runner_started", chain_id=chain_id, worker_id=self._worker_id)

    async def _stop_runner(self, chain_id: str) -> None:
        runner, task = self._runners.pop(chain_id)
        # stop() sets the runner's _stop event so its `async for header` exits
        # on the next head; cancel() is the safety net for runners blocked in
        # subscribe_heads() with no traffic.
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
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
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
        if self._watcher is not None:
            await self._watcher.stop()
        if self._runners:
            await asyncio.gather(
                *(self._stop_runner(cid) for cid in list(self._runners)),
                return_exceptions=False,
            )
        # Release all distributed locks
        for chain_id, lock in list(self._locks.items()):
            try:
                await lock.release()
            except Exception:  # noqa: BLE001
                pass
        self._locks.clear()
        await self._bus.disconnect()
        await self._db.disconnect()
        log.info("worker.shutdown_complete")


async def run_worker(
    settings: Settings,
    stop_event: asyncio.Event,
    *,
    ready_event: asyncio.Event | None = None,
) -> None:
    """Public coroutine that boots a `_Worker` and runs until `stop_event` is set.

    `ready_event` (optional) is set after `_Worker.start()` succeeds AND the
    first reconcile completes (i.e. all enabled chains have started). Tests
    use this to avoid timing-based sleeps; production callers (`_amain`)
    ignore it.

    Does NOT install signal handlers — the caller is responsible for triggering
    `stop_event` (E2E tests do this directly; the CLI entry point does it from
    SIGTERM/SIGINT handlers via `_amain` below).
    """
    worker = _Worker(settings, ready_event=ready_event)
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
    """POSIX-targeted CLI entry. Installs SIGTERM/SIGINT handlers and runs
    until either signal sets `stop_event`.

    Windows note: `loop.add_signal_handler` raises `NotImplementedError` on
    the Proactor event loop. Windows users should embed `run_worker(settings,
    stop_event)` directly into their own process supervisor and drive
    `stop_event` from whatever signal mechanism their OS provides (e.g. a
    SetConsoleCtrlHandler shim). Embedding callers don't need this CLI.
    """
    settings = load_settings()
    configure_logging(level=settings.logging.level, format=settings.logging.format)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.signal_received", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig)
        except NotImplementedError:
            # Windows Proactor loop: signal handlers unsupported. Surface the
            # advice and bail; embedding callers should use run_worker() directly.
            log.error(
                "worker.signal_handler_unsupported",
                signal=sig.name,
                hint="run_worker(settings, stop_event) directly on Windows",
            )
            raise

    await run_worker(settings, stop_event)


def main() -> None:
    """Console-script entry point (referenced from pyproject.toml `[project.scripts]`)."""
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
