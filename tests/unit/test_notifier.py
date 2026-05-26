from __future__ import annotations

import asyncio
from typing import Any

import pytest

from core.config.snapshot import SnapshotChannel, SnapshotSubscription
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.event import Event


class _CollectingChannel(Channel):
    type = "collect-notifier"
    config_schema: dict = {}

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send(self, payload: dict[str, Any]) -> None:
        self.calls.append(payload)


def _sub(channel_ids: list[str]) -> SnapshotSubscription:
    return SnapshotSubscription(
        id="s1",
        name="sub",
        chain_id="eth",
        address=None,
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
        channel_ids=channel_ids,
    )


def _ch(id_: str = "c1") -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="x", type="collect", config={})


def _event() -> Event:
    return Event(
        chain_id="eth",
        block_number=1,
        block_hash="0xh",
        block_timestamp=0,
        tx_hash="0xt",
        tx_index=0,
        log_index=None,
        kind="native_transfer",
        contract=None,
        name=None,
        args={"from": "0xa", "to": "0xb", "value": "1"},
        raw={},
    )


@pytest.mark.asyncio
async def test_dispatch_sends_payload_to_each_channel() -> None:
    created: list[_CollectingChannel] = []

    def factory(cfg: SnapshotChannel) -> Channel:
        inst = _CollectingChannel()
        created.append(inst)
        return inst

    notifier = Notifier(
        channel_factory=factory,
        max_concurrency=10,
    )
    await notifier.start([_ch("c1"), _ch("c2")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c1", "c2"]), [_ch("c1"), _ch("c2")])])
        # Both channels should have received exactly one payload.
        assert all(len(c.calls) == 1 for c in created)
        for c in created:
            assert c.calls[0]["chain_id"] == "eth"
    finally:
        await notifier.stop()


@pytest.mark.asyncio
async def test_one_channel_failure_does_not_block_others() -> None:
    class _Bad(Channel):
        type = "bad"
        config_schema: dict = {}

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None:
            raise RuntimeError("boom")

    good = _CollectingChannel()
    bad = _Bad()
    mapping: dict[str, Channel] = {"c-good": good, "c-bad": bad}

    notifier = Notifier(
        channel_factory=lambda cfg: mapping[cfg.id],
        max_concurrency=10,
    )
    await notifier.start([_ch("c-good"), _ch("c-bad")])
    try:
        await notifier.dispatch(
            _event(),
            [(_sub(["c-good", "c-bad"]), [_ch("c-good"), _ch("c-bad")])],
        )
        assert len(good.calls) == 1  # good received despite bad failing
    finally:
        await notifier.stop()


@pytest.mark.asyncio
async def test_semaphore_caps_concurrent_sends() -> None:
    """With max_concurrency=2 and 5 channels using a slow send, never >2 inflight."""
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    class _Slow(Channel):
        type = "slow"
        config_schema: dict = {}

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None:
            nonlocal in_flight, peak
            async with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1

    chans = [_ch(f"c{i}") for i in range(5)]
    mapping: dict[str, Channel] = {c.id: _Slow() for c in chans}
    notifier = Notifier(channel_factory=lambda cfg: mapping[cfg.id], max_concurrency=2)
    await notifier.start(chans)
    try:
        await notifier.dispatch(_event(), [(_sub([c.id for c in chans]), chans)])
        assert peak <= 2
    finally:
        await notifier.stop()
