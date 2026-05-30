from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

log = structlog.get_logger(__name__)

_DEFAULT_CHANNEL = "replay_request"


class _Bus(Protocol):
    def subscribe(self, channel: str, *, ready: asyncio.Event | None = ...):  # type: ignore[no-untyped-def]
        ...


class ReplayWatcher:
    """Subscribes to the Redis `replay_request` channel and hands each message
    to `on_replay`. A raising callback is logged and does not stop the loop."""

    def __init__(
        self,
        *,
        bus: _Bus,
        on_replay: Callable[[dict[str, Any]], Awaitable[None]],
        channel: str = _DEFAULT_CHANNEL,
    ) -> None:
        self._bus = bus
        self._on_replay = on_replay
        self._channel = channel
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="replay_watcher")

    async def _run(self) -> None:
        async for msg in self._bus.subscribe(self._channel, ready=self._ready):
            try:
                await self._on_replay(msg)
            except Exception:  # noqa: BLE001
                log.exception("replay_watcher.dispatch_failed")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
