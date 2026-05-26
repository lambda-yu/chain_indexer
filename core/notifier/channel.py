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
    config_schema: ClassVar[dict[str, Any]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        t = getattr(cls, "type", None)
        if not isinstance(t, str) or not t:
            raise TypeError(
                f"{cls.__name__} must declare a `type` class attribute "
                f"(non-empty str). Got {t!r}."
            )
        if not hasattr(cls, "config_schema") or not isinstance(cls.config_schema, dict):
            raise TypeError(
                f"{cls.__name__} must declare a `config_schema` class attribute (dict)."
            )
        register_channel(cls)

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, payload: dict[str, Any]) -> None: ...

    @property
    def _retry_config(self) -> tuple[int, float, float]:
        cfg = getattr(self, "_config", None) or {}
        retry = cfg.get("retry", {}) if isinstance(cfg, dict) else {}
        return (
            retry.get("max_attempts", 3),
            retry.get("base_delay", 1.0),
            retry.get("backoff_factor", 4.0),
        )


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
