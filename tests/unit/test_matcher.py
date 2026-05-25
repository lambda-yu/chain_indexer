from __future__ import annotations

from typing import Any

from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription
from core.matcher.matcher import Matcher
from core.parser.event import Event


def _event(
    *,
    chain_id: str = "eth",
    to: str = "0xb",
    value: str = "100",
    kind: str = "native_transfer",
) -> Event:
    return Event(
        chain_id=chain_id,
        block_number=1,
        block_hash="0xh",
        block_timestamp=0,
        tx_hash="0xt",
        tx_index=0,
        log_index=None,
        kind=kind,  # type: ignore[arg-type]
        contract=None,
        name=None,
        args={"from": "0xa", "to": to, "value": value},
        raw={},
    )


def _snap(subs: list[SnapshotSubscription], chans: list[SnapshotChannel]) -> ConfigSnapshot:
    return ConfigSnapshot(version=1, subscriptions=subs, channels=chans)


def _sub(**kw: Any) -> SnapshotSubscription:
    defaults: dict[str, Any] = dict(
        id="s1",
        name="x",
        chain_id="eth",
        address=None,
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
        channel_ids=["c1"],
    )
    defaults.update(kw)
    return SnapshotSubscription(**defaults)


def _ch(**kw: Any) -> SnapshotChannel:
    defaults: dict[str, Any] = dict(id="c1", name="hook", type="http", config={"url": "http://x"})
    defaults.update(kw)
    return SnapshotChannel(**defaults)


def test_matches_by_chain_and_kind() -> None:
    m = Matcher(_snap([_sub()], [_ch()]))
    hits = list(m.match(_event()))
    assert len(hits) == 1
    sub, channels = hits[0]
    assert sub.id == "s1"
    assert [ch.id for ch in channels] == ["c1"]


def test_disabled_subscription_does_not_match() -> None:
    m = Matcher(_snap([_sub(enabled=False)], [_ch()]))
    assert list(m.match(_event())) == []


def test_multiple_subscriptions_all_hit() -> None:
    subs = [
        _sub(id="s1"),
        _sub(id="s2", arg_filters={"to": "0xb"}),
    ]
    m = Matcher(_snap(subs, [_ch()]))
    hits = list(m.match(_event(to="0xb")))
    assert {h[0].id for h in hits} == {"s1", "s2"}


def test_address_match_case_insensitive() -> None:
    sub = _sub(
        match_kind="token_transfer",
        address="0xAAA",
        channel_ids=["c1"],
    )
    m = Matcher(_snap([sub], [_ch()]))
    ev = Event(
        chain_id="eth",
        block_number=1,
        block_hash="0xh",
        block_timestamp=0,
        tx_hash="0xt",
        tx_index=0,
        log_index=0,
        kind="token_transfer",
        contract="0xaaa",
        name="Transfer",
        args={"to": "0xb", "value": "1"},
        raw={},
    )
    assert len(list(m.match(ev))) == 1


def test_chain_id_mismatch_does_not_match() -> None:
    m = Matcher(_snap([_sub(chain_id="bsc")], [_ch()]))
    assert list(m.match(_event(chain_id="eth"))) == []


def test_arg_filter_range_applied() -> None:
    m = Matcher(_snap([_sub(arg_filters={"value_gte": "1000"})], [_ch()]))
    assert list(m.match(_event(value="999"))) == []
    assert len(list(m.match(_event(value="1000")))) == 1


def test_unknown_channel_id_is_skipped_silently() -> None:
    """A subscription bound to a non-existent channel id matches but yields empty channels."""
    m = Matcher(_snap([_sub(channel_ids=["c-missing"])], [_ch(id="c1")]))
    hits = list(m.match(_event()))
    assert len(hits) == 1
    assert hits[0][1] == []
