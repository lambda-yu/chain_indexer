from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol

import structlog

from core.config.snapshot import ConfigSnapshot

log = structlog.get_logger(__name__)

_DEFAULT_CHANNEL = "config_changed"


class _Bus(Protocol):
    """Subset of `core.bus.redis_bus.RedisBus` that ConfigWatcher uses."""

    def subscribe(self, channel: str, *, ready: asyncio.Event | None = ...):  # type: ignore[no-untyped-def]
        ...


SessionCM = AbstractAsyncContextManager[object]
SessionFactory = Callable[[], SessionCM]
LoadSnapshotFn = Callable[[object], Awaitable[ConfigSnapshot]]


class ConfigWatcher:
    """Refreshes a `ConfigSnapshot` on Redis `config_changed` events or a periodic
    `config_version.version` poll (whichever fires first).

    Emits the new snapshot onto `out_queue` ONLY when its `version` differs from the
    previously emitted version. This means downstream consumers (the worker main
    loop) see at most one emission per actual configuration change, regardless of
    how many triggers fire.

    Spec §5.5: the 5-s poll is the authoritative fallback if Redis is unreachable.
    """

    def __init__(
        self,
        *,
        bus: _Bus,
        session_factory: SessionFactory,
        load_snapshot: LoadSnapshotFn,
        out_queue: asyncio.Queue[ConfigSnapshot],
        channel: str = _DEFAULT_CHANNEL,
        poll_interval_s: float = 5.0,
    ) -> None:
        self._bus = bus
        self._session_factory = session_factory
        self._load_snapshot = load_snapshot
        self._out = out_queue
        self._channel = channel
        self._poll_interval_s = poll_interval_s
        self._last_version: int | None = None
        self._sub_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._trigger = asyncio.Event()

    async def start(self) -> None:
        # Initial load — must complete before the worker's main loop blocks on `out`.
        await self._reload_and_maybe_emit(reason="initial")
        self._sub_task = asyncio.create_task(self._run_subscriber(), name="config-watcher-sub")
        self._poll_task = asyncio.create_task(self._run_poller(), name="config-watcher-poll")

    async def stop(self) -> None:
        self._stop.set()
        self._trigger.set()  # unblock any pending wait
        for t in (self._sub_task, self._poll_task):
            if t is None:
                continue
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):  # noqa: BLE001
                await t
        self._sub_task = self._poll_task = None

    async def _run_subscriber(self) -> None:
        # NOTE: the `ready` event exists for callers (e.g. tests) to know when
        # the subscription is live. The watcher itself just iterates `gen` —
        # awaiting `ready` here would deadlock since the generator body (which
        # sets `ready`) only runs once iteration starts.
        gen = self._bus.subscribe(self._channel, ready=asyncio.Event())
        try:
            async for _msg in gen:
                if self._stop.is_set():
                    break
                await self._reload_and_maybe_emit(reason="redis")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("config_watcher.subscriber_failed")
        finally:
            aclose = getattr(gen, "aclose", None)
            if aclose is not None:
                with contextlib.suppress(Exception):  # noqa: BLE001
                    await aclose()

    async def _run_poller(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval_s)
                    return  # stop fired
                except TimeoutError:
                    pass
                await self._reload_and_maybe_emit(reason="poll")
        except asyncio.CancelledError:
            raise

    async def _reload_and_maybe_emit(self, *, reason: str) -> None:
        try:
            async with self._session_factory() as session:
                snap = await self._load_snapshot(session)
        except Exception:  # noqa: BLE001
            log.exception("config_watcher.reload_failed", reason=reason)
            return
        if self._last_version is not None and snap.version == self._last_version:
            return
        self._last_version = snap.version
        await self._out.put(snap)
        log.info("config_watcher.snapshot_emitted", version=snap.version, reason=reason)
