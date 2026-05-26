from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar


class Channel(ABC):
    """Abstract base for a notification channel driver.

    Lifecycle: `start()` → many `send()` → `stop()`. Implementations should be
    safe to construct from a `SnapshotChannel.config` dict; the worker calls
    `start()` once on first use per chain pipeline.

    Subclasses must declare a non-empty `type: ClassVar[str]` and are
    auto-registered in `CHANNEL_REGISTRY` at class-definition time.
    """

    type: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        t = getattr(cls, "type", None)
        if not isinstance(t, str) or not t:
            raise TypeError(
                f"{cls.__name__} must declare a `type` class attribute "
                f"(non-empty str). Got {t!r}."
            )
        register_channel(cls)

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
