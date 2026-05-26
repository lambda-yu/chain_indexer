from __future__ import annotations

from typing import Any

import pytest

from core.notifier.channel import CHANNEL_REGISTRY, Channel, register_channel


class _FakeChannel(Channel):
    type = "fake"
    config_schema: dict = {}

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_type_auto_registers_at_class_definition() -> None:
    assert CHANNEL_REGISTRY["fake"] is _FakeChannel


def test_subclass_without_type_attr_raises_type_error() -> None:
    with pytest.raises(TypeError, match="must declare a `type` class attribute"):
        class _Missing(Channel):
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_empty_type_raises_type_error() -> None:
    with pytest.raises(TypeError, match="must declare a `type` class attribute"):
        class _Empty(Channel):
            type = ""
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_duplicate_type_raises_at_class_definition() -> None:
    with pytest.raises(ValueError, match="already registered"):
        class _Dup(Channel):
            type = "fake"
            config_schema: dict = {}
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_explicit_register_channel_remains_idempotent_for_same_class() -> None:
    register_channel(_FakeChannel)
    register_channel(_FakeChannel)
    assert CHANNEL_REGISTRY["fake"] is _FakeChannel


def test_http_channel_remains_registered_without_explicit_call() -> None:
    from core.notifier.http import HttpChannel
    assert CHANNEL_REGISTRY["http"] is HttpChannel
