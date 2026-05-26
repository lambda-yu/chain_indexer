from __future__ import annotations

import json
from functools import partial
from typing import Any, ClassVar

from core.bus.redis_bus import RedisBus
from core.notifier.channel import Channel
from core.notifier.retry import retry_with_backoff


class RedisStreamsChannel(Channel):
    """XADD-based Redis Streams notification driver.

    Reuses the worker's shared `RedisBus` connection — does not own a client.
    """

    type = "mq"
    config_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "required": ["stream"],
        "properties": {
            "stream": {"type": "string"},
            "maxlen": {"type": "integer"},
        },
    }

    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: RedisBus,
        base_delay: float = 1.0,
    ) -> None:
        self._stream: str = config["stream"]
        self._maxlen: int | None = config.get("maxlen")
        self._bus = bus
        self._base_delay = base_delay

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"))
        await retry_with_backoff(
            partial(self._xadd_once, body=body),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _xadd_once(self, *, body: str) -> None:
        client = self._bus.client
        kwargs: dict[str, Any] = {}
        if self._maxlen is not None:
            kwargs["maxlen"] = self._maxlen
            kwargs["approximate"] = True
        await client.xadd(self._stream, {"data": body}, **kwargs)
