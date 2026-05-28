from __future__ import annotations

import json
from typing import Any, ClassVar

import structlog

from core.notifier.channel import Channel
from core.notifier.retry import retry_with_backoff

log = structlog.get_logger(__name__)


class KafkaChannel(Channel):
    """Kafka notification driver via confluent-kafka.

    Producer-side batching is enabled by default (`linger.ms=10`,
    `batch.size=16384`). At sub-10ms cost this lets librdkafka coalesce
    bursts of events into single network roundtrips, which is the main win
    for indexers that fan out one event to one Kafka topic.
    """

    type = "kafka"
    config_schema: ClassVar[dict[str, Any]] = {
        "type": "object",
        "required": ["bootstrap_servers", "topic"],
        "properties": {
            "bootstrap_servers": {"type": "string"},
            "topic": {"type": "string"},
            "key": {"type": "string"},
            "compression_type": {"type": "string"},
            "linger_ms": {"type": "integer", "minimum": 0},
            "batch_size": {"type": "integer", "minimum": 1},
            "acks": {"type": "string"},
        },
    }

    def __init__(self, *, config: dict[str, Any], bus: object = None, base_delay: float = 1.0) -> None:
        del bus
        self._config = config
        self._bootstrap_servers: str = config["bootstrap_servers"]
        self._topic: str = config["topic"]
        self._key: str | None = config.get("key")
        self._compression: str = config.get("compression_type", "none")
        self._linger_ms: int = config.get("linger_ms", 10)
        self._batch_size: int = config.get("batch_size", 16384)
        self._acks: str = config.get("acks", "1")
        self._producer: Any = None

    async def start(self) -> None:
        from confluent_kafka import Producer  # type: ignore[import-untyped]
        self._producer = Producer({
            "bootstrap.servers": self._bootstrap_servers,
            "compression.type": self._compression,
            "linger.ms": self._linger_ms,
            "batch.size": self._batch_size,
            "acks": self._acks,
        })

    async def stop(self) -> None:
        if self._producer is not None:
            self._producer.flush(timeout=5.0)
            self._producer = None

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._producer is not None
        body = json.dumps(payload, separators=(",", ":")).encode()
        max_attempts, base_delay, factor = self._retry_config

        async def _produce() -> None:
            self._producer.produce(
                self._topic,
                value=body,
                key=self._key.encode() if self._key else None,
            )
            self._producer.poll(0)

        await retry_with_backoff(_produce, max_attempts=max_attempts, base_delay=base_delay, factor=factor)
