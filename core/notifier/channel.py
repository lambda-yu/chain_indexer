from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar


class Channel(ABC):
    """Abstract base for a notification channel driver.

    Lifecycle: `start()` → many `send()` → `stop()`. Implementations should be
    safe to construct from a `SnapshotChannel.config` dict; the worker calls
    `start()` once on first use per chain pipeline.
    """

    type: ClassVar[str]

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, payload: dict[str, Any]) -> None: ...


CHANNEL_REGISTRY: dict[str, type[Channel]] = {}


def register_channel(cls: type[Channel]) -> type[Channel]:
    """Register a Channel subclass under its `type` attribute. Idempotent only
    if the same class object is re-registered; different classes for the same
    type raise.
    """
    t = cls.type
    if t in CHANNEL_REGISTRY and CHANNEL_REGISTRY[t] is not cls:
        raise ValueError(f"channel type {t!r} already registered to {CHANNEL_REGISTRY[t]!r}")
    CHANNEL_REGISTRY[t] = cls
    return cls
