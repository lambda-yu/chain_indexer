from __future__ import annotations

import json
from functools import partial
from typing import Any

from core.bus.redis_bus import RedisBus
from core.notifier.channel import Channel
from core.notifier.retry import retry_with_backoff


class WebSocketChannel(Channel):
    """Redis Pub/Sub-backed notification driver for WebSocket fan-out."""

    type = "ws"

    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: RedisBus,
        base_delay: float = 1.0,
    ) -> None:
        self._fanout_channel: str = config["ws_fanout_channel"]
        self._bus = bus
        self._base_delay = base_delay

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"))
        await retry_with_backoff(
            partial(self._publish_once, body=body),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _publish_once(self, *, body: str) -> None:
        client = self._bus.client
        await client.publish(self._fanout_channel, body)
