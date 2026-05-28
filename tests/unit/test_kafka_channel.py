from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import MagicMock

import pytest


def _install_fake_confluent_kafka(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Install a fake confluent_kafka module so KafkaChannel can be imported and
    exercised without the real C extension. Returns the Producer mock class."""
    producer_cls = MagicMock(name="Producer")
    fake_module = types.ModuleType("confluent_kafka")
    fake_module.Producer = producer_cls  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_module)
    return producer_cls


@pytest.mark.asyncio
async def test_default_producer_config_enables_batching(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default linger.ms and batch.size must be non-zero so librdkafka batches."""
    producer_cls = _install_fake_confluent_kafka(monkeypatch)
    from core.notifier.kafka import KafkaChannel

    ch = KafkaChannel(config={
        "bootstrap_servers": "localhost:9092",
        "topic": "events",
    })
    await ch.start()
    try:
        cfg = producer_cls.call_args[0][0]
        assert cfg["bootstrap.servers"] == "localhost:9092"
        assert cfg["linger.ms"] == 10
        assert cfg["batch.size"] == 16384
        assert cfg["acks"] == "1"
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_producer_config_overrides_from_channel_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """User-supplied linger_ms / batch_size / acks must reach librdkafka."""
    producer_cls = _install_fake_confluent_kafka(monkeypatch)
    from core.notifier.kafka import KafkaChannel

    ch = KafkaChannel(config={
        "bootstrap_servers": "kafka:9092",
        "topic": "events",
        "linger_ms": 50,
        "batch_size": 65536,
        "acks": "all",
        "compression_type": "gzip",
    })
    await ch.start()
    try:
        cfg = producer_cls.call_args[0][0]
        assert cfg["linger.ms"] == 50
        assert cfg["batch.size"] == 65536
        assert cfg["acks"] == "all"
        assert cfg["compression.type"] == "gzip"
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_send_produces_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """send() forwards JSON-encoded payload to producer.produce()."""
    producer_cls = _install_fake_confluent_kafka(monkeypatch)
    producer_inst = MagicMock()
    producer_cls.return_value = producer_inst

    from core.notifier.kafka import KafkaChannel

    ch = KafkaChannel(config={
        "bootstrap_servers": "localhost:9092",
        "topic": "events",
    })
    await ch.start()
    try:
        payload: dict[str, Any] = {"k": 1, "subscription_id": "s1"}
        await ch.send(payload)
    finally:
        await ch.stop()

    producer_inst.produce.assert_called_once()
    _, kwargs = producer_inst.produce.call_args
    assert kwargs["value"] == json.dumps(payload, separators=(",", ":")).encode()


def test_type_attribute_matches_db_enum_kafka_slot(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_confluent_kafka(monkeypatch)
    from core.notifier.kafka import KafkaChannel
    assert KafkaChannel.type == "kafka"
