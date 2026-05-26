from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Any

import structlog

from core.config.snapshot import SnapshotChannel, SnapshotSubscription
from core.notifier.channel import CHANNEL_REGISTRY, Channel
from core.notifier.payload import build_payload
from core.parser.event import Event

log = structlog.get_logger(__name__)


def _default_factory(cfg: SnapshotChannel) -> Channel:
    cls = CHANNEL_REGISTRY[cfg.type]
    return cls(config=cfg.config)  # type: ignore[call-arg]


class Notifier:
    """Owns instantiated channels and dispatches events to them concurrently.

    A bounded `asyncio.Semaphore` (default 50) caps total in-flight sends across
    all channels held by this `Notifier` instance. Spec §6.1 specifies a *per-chain*
    semaphore — the worker (Chunk 7) instantiates one `Notifier` per chain, so the
    per-instance limit here IS the per-chain limit. Do not share a single `Notifier`
    across chains; that would conflate the two budgets.

    The semaphore is built lazily on the first `_send_one` call so it binds to
    the running loop. Constructing a `Notifier` outside a running loop (e.g.
    inside a sync fixture body) used to crash at first `send` with
    `RuntimeError: ... bound to a different event loop`.

    Failures in one channel do not block sibling channels — each `send` is wrapped
    to log-and-continue, and `asyncio.gather(..., return_exceptions=True)` is used
    defensively so a bug *outside* the `try` in `_send_one` cannot cancel siblings.
    """

    def __init__(
        self,
        *,
        channel_factory: Callable[[SnapshotChannel], Channel] = _default_factory,
        max_concurrency: int = 50,
    ) -> None:
        self._factory = channel_factory
        self._max_concurrency = max_concurrency
        self._sem: asyncio.Semaphore | None = None
        self._channels: dict[str, Channel] = {}

    def _get_sem(self) -> asyncio.Semaphore:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._max_concurrency)
        return self._sem

    async def start(self, channels: Sequence[SnapshotChannel]) -> None:
        for cfg in channels:
            inst = self._factory(cfg)
            await inst.start()
            self._channels[cfg.id] = inst

    async def stop(self) -> None:
        for ch in self._channels.values():
            try:
                await ch.stop()
            except Exception:  # noqa: BLE001
                log.exception("notifier.channel_stop_failed", type=ch.type)
        self._channels.clear()

    async def dispatch(
        self,
        event: Event,
        hits: Sequence[tuple[SnapshotSubscription, Sequence[SnapshotChannel]]],
    ) -> None:
        """Build one payload per (sub, channel) pair and send concurrently."""
        tasks: list[asyncio.Task[None]] = []
        for sub, chans in hits:
            payload = build_payload(event=event, subscription=sub)
            for ch_cfg in chans:
                ch = self._channels.get(ch_cfg.id)
                if ch is None:
                    log.warning(
                        "notifier.channel_not_started",
                        channel_id=ch_cfg.id,
                        subscription_id=sub.id,
                    )
                    continue
                tasks.append(asyncio.create_task(self._send_one(ch, payload, sub.id, ch_cfg.id)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_one(
        self, ch: Channel, payload: dict[str, Any], subscription_id: str, channel_id: str
    ) -> None:
        async with self._get_sem():
            try:
                await ch.send(payload)
            except Exception:  # noqa: BLE001 — log-only per spec §9
                log.exception(
                    "notifier.send_failed",
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    delivery_id=payload.get("delivery_id"),
                )
