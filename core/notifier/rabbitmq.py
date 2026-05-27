from __future__ import annotations

import json
from typing import Any, ClassVar

import structlog

from core.notifier.channel import Channel
from core.notifier.retry import retry_with_backoff

log = structlog.get_logger(__name__)


class RabbitMQChannel(Channel):
    """RabbitMQ notification driver via aio-pika."""

    type = "rabbitmq"
    config_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "required": ["url"],
        "properties": {
            "url": {"type": "string"},
            "exchange": {"type": "string"},
            "routing_key": {"type": "string"},
            "queue": {"type": "string"},
        },
    }

    def __init__(self, *, config: dict[str, Any], bus: object = None, base_delay: float = 1.0) -> None:
        del bus
        self._config = config
        self._url: str = config["url"]
        self._exchange_name: str = config.get("exchange", "")
        self._routing_key: str = config.get("routing_key", "")
        self._queue_name: str | None = config.get("queue")
        self._connection: Any = None
        self._channel: Any = None

    async def start(self) -> None:
        import aio_pika  # type: ignore[import-untyped]
        self._connection = await aio_pika.connect_robust(self._url)
        self._channel = await self._connection.channel()

    async def stop(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            self._channel = None

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._channel is not None
        import aio_pika  # type: ignore[import-untyped]
        body = json.dumps(payload, separators=(",", ":")).encode()
        max_attempts, base_delay, factor = self._retry_config

        async def _publish() -> None:
            exchange = await self._channel.get_exchange(self._exchange_name) if self._exchange_name else self._channel.default_exchange
            await exchange.publish(
                aio_pika.Message(body=body, content_type="application/json"),
                routing_key=self._routing_key,
            )

        await retry_with_backoff(_publish, max_attempts=max_attempts, base_delay=base_delay, factor=factor)
