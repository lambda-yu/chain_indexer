from __future__ import annotations

from typing import Any

from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.event import Event


class _StubChannel(Channel):
    type = "stub-sem"
    config_schema: dict = {}

    def __init__(self, config: dict[str, Any]) -> None:
        self.sent: list[dict[str, Any]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict[str, Any]) -> None:
        self.sent.append(payload)


def test_constructing_notifier_outside_a_running_loop_does_not_bind() -> None:
    """Constructing Notifier with no running loop must not raise nor pre-bind
    a semaphore to a now-defunct loop."""
    n = Notifier(max_concurrency=5)
    # _sem is None until the first send call inside a running loop.
    assert n._sem is None


async def test_first_send_binds_sem_to_running_loop(tmp_path: Any) -> None:
    """Build Notifier in one place, call dispatch in the test's loop. Must work."""
    from core.config.snapshot import SnapshotChannel, SnapshotSubscription

    def _factory(cfg: SnapshotChannel) -> Channel:
        return _StubChannel(cfg.config)

    n = Notifier(channel_factory=_factory, max_concurrency=3)
    await n.start([SnapshotChannel(id="c1", name="c1", type="stub-sem", config={})])

    ev = Event(
        chain_id="x", block_number=1, block_hash="0xb", block_timestamp=0,
        tx_hash="0xt", tx_index=0, log_index=None, kind="native_transfer",
        contract=None, name=None, args={}, raw={},
    )
    sub = SnapshotSubscription(
        id="s1", name="s", chain_id="x", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None,
        arg_filters={}, enabled=True, channel_ids=["c1"],
    )
    chan = SnapshotChannel(id="c1", name="c1", type="stub-sem", config={})

    await n.dispatch(ev, [(sub, [chan])])

    # After first dispatch the semaphore exists and is bound to *this* loop.
    sem = n._sem
    assert sem is not None
    assert sem._value == 3  # default value, fully released

    await n.stop()
