from typing import Any

from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription


def _sub(**overrides: Any) -> SnapshotSubscription:
    base: dict[str, Any] = dict(
        id="s1",
        name="wallet1",
        chain_id="eth-mainnet",
        address="0xabc",
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
        channel_ids=["c1"],
    )
    base.update(overrides)
    return SnapshotSubscription(**base)


def _ch(**overrides: Any) -> SnapshotChannel:
    base: dict[str, Any] = dict(id="c1", name="hook", type="http", config={"url": "http://x"})
    base.update(overrides)
    return SnapshotChannel(**base)


def test_subscriptions_for_chain_returns_only_matching_chain() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[
            _sub(id="s1", chain_id="eth-mainnet"),
            _sub(id="s2", chain_id="bsc-mainnet"),
        ],
        channels=[_ch()],
    )
    res = s.subscriptions_for_chain("eth-mainnet")
    assert [r.id for r in res] == ["s1"]


def test_disabled_subscriptions_are_skipped() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[
            _sub(id="s1", enabled=False),
            _sub(id="s2", enabled=True),
        ],
        channels=[_ch()],
    )
    res = s.subscriptions_for_chain("eth-mainnet")
    assert [r.id for r in res] == ["s2"]


def test_channels_for_subscription_resolves_ids() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[_sub(channel_ids=["c1", "c2"])],
        channels=[_ch(id="c1"), _ch(id="c2", type="ws")],
    )
    sub = s.subscriptions_for_chain("eth-mainnet")[0]
    chans = s.channels_for_subscription(sub)
    assert {c.id for c in chans} == {"c1", "c2"}


def test_missing_channel_id_is_ignored() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[_sub(channel_ids=["c1", "c-missing"])],
        channels=[_ch(id="c1")],
    )
    sub = s.subscriptions_for_chain("eth-mainnet")[0]
    chans = s.channels_for_subscription(sub)
    assert [c.id for c in chans] == ["c1"]


def test_subscription_snapshot_business_name_defaults_to_none() -> None:
    s = _sub()
    assert s.business_name is None


def test_subscription_snapshot_business_name_carried_through() -> None:
    s = _sub(business_name="trading-team")
    assert s.business_name == "trading-team"
