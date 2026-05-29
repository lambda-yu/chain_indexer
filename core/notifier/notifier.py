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


FailureCallback = Callable[[str, str, str, dict[str, Any], str, int], Any] | None
SuccessCallback = Callable[[str, str, str, dict[str, Any], None, int], Any] | None


class Notifier:
    """Owns instantiated channels and dispatches events to them concurrently."""

    def __init__(
        self,
        *,
        channel_factory: Callable[[SnapshotChannel], Channel] = _default_factory,
        max_concurrency: int = 50,
        on_failure: FailureCallback = None,
        on_success: SuccessCallback = None,
    ) -> None:
        self._factory = channel_factory
        self._max_concurrency = max_concurrency
        self._sem: asyncio.Semaphore | None = None
        self._channels: dict[str, Channel] = {}
        self._on_failure = on_failure
        self._on_success = on_success

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
        import time

        from core.metrics import CHANNEL_SEND_SECONDS, CHANNEL_SENDS_TOTAL

        t0 = time.perf_counter()
        send_status = "failed"  # pessimistic default
        async with self._get_sem():
            try:
                await ch.send(payload)
                send_status = "success"
                if self._on_success:
                    try:
                        await self._on_success(
                            subscription_id, channel_id,
                            payload.get("chain_id", ""),
                            payload, None, 1,
                        )
                    except Exception:  # noqa: BLE001
                        log.error("notifier.on_success_callback_error")
            except Exception as exc:  # noqa: BLE001
                attempts = getattr(exc, "attempts", 1)
                log.error(
                    "notifier.send_failed",
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    delivery_id=payload.get("delivery_id"),
                    attempts=attempts,
                    error=repr(exc),
                )
                if self._on_failure:
                    try:
                        await self._on_failure(
                            subscription_id, channel_id,
                            payload.get("chain_id", ""),
                            payload, repr(exc), attempts,
                        )
                    except Exception:  # noqa: BLE001
                        log.error("notifier.on_failure_callback_error")
            finally:
                CHANNEL_SEND_SECONDS.labels(ch.type).observe(time.perf_counter() - t0)
                CHANNEL_SENDS_TOTAL.labels(ch.type, send_status).inc()
