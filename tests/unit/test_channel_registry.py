from __future__ import annotations

from typing import Any

import pytest

from core.notifier.channel import CHANNEL_REGISTRY, Channel, register_channel


class _FakeChannel(Channel):
    type = "fake"

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict[str, Any]) -> None: ...


def test_register_and_lookup() -> None:
    register_channel(_FakeChannel)
    try:
        assert CHANNEL_REGISTRY["fake"] is _FakeChannel
    finally:
        del CHANNEL_REGISTRY["fake"]


def test_register_duplicate_raises() -> None:
    register_channel(_FakeChannel)
    try:

        class _FakeChannelDup(Channel):
            type = "fake"

            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...

        with pytest.raises(ValueError, match="already registered"):
            register_channel(_FakeChannelDup)
    finally:
        del CHANNEL_REGISTRY["fake"]


def test_register_same_class_is_idempotent() -> None:
    register_channel(_FakeChannel)
    try:
        register_channel(_FakeChannel)
        assert CHANNEL_REGISTRY["fake"] is _FakeChannel
    finally:
        del CHANNEL_REGISTRY["fake"]
