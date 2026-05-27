from __future__ import annotations

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotAbi,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.matcher.filter_set import (
    ERC20_TRANSFER_TOPIC0,
    EvmLogFilterSet,
    build_evm_log_filter,
)


def _snap(subs: list[SnapshotSubscription], abis: list[SnapshotAbi] | None = None) -> ConfigSnapshot:
    return ConfigSnapshot(
        version=1,
        subscriptions=subs,
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[],
        abis=abis or [],
    )


def _sub(**kw):
    return SnapshotSubscription(
        id=kw.get("id", "s1"),
        name=kw.get("name", "s1"),
        chain_id=kw["chain_id"],
        address=kw.get("address"),
        abi_id=kw.get("abi_id"),
        match_kind=kw["match_kind"],
        match_name=kw.get("match_name"),
        arg_filters=kw.get("arg_filters", {}),
        enabled=kw.get("enabled", True),
        channel_ids=["c1"],
    )


def test_skip_logs_when_no_event_or_token_subscriptions():
    snap = _snap([_sub(chain_id="evm-1", match_kind="native_transfer")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.skip_logs is True
    assert f.addresses is None
    assert f.topic0s is None


def test_addresses_concrete_when_all_relevant_subs_have_address():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xABCDEF"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0x123456", id="s2"),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.skip_logs is False
    assert f.addresses == ["0x123456", "0xabcdef"]
    assert f.topic0s == [ERC20_TRANSFER_TOPIC0]


def test_addresses_none_when_any_relevant_sub_has_no_address():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xABCDEF"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address=None, id="s2"),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.addresses is None
    assert f.topic0s == [ERC20_TRANSFER_TOPIC0]


def test_topic0s_includes_computed_event_signatures():
    abi_body = [{
        "type": "event",
        "name": "Foo",
        "inputs": [{"name": "x", "type": "uint256", "indexed": False}],
    }]
    expected_t0 = event_topic0(abi_body[0]).lower()
    abi = SnapshotAbi(id="abi-1", name="abi-1", kind="evm_abi", body=abi_body)
    registry = AbiRegistry()
    registry.refresh(ConfigSnapshot(version=1, subscriptions=[], channels=[], chains=[], abis=[abi]))
    snap = _snap(
        [_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name="Foo", address="0xaaa")],
        abis=[abi],
    )
    f = build_evm_log_filter(snap, "evm-1", registry)
    assert f.topic0s == [expected_t0]


def test_topic0s_none_when_event_sub_missing_abi_id():
    snap = _snap([_sub(chain_id="evm-1", match_kind="event", abi_id=None, match_name="Foo", address="0xaaa")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.topic0s is None
    assert f.addresses == ["0xaaa"]


def test_topic0s_none_when_event_sub_missing_match_name():
    snap = _snap([_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name=None, address="0xaaa")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.topic0s is None


def test_topic0s_none_when_event_signature_not_found_in_registry():
    abi_body = [{"type": "event", "name": "Bar", "inputs": []}]
    abi = SnapshotAbi(id="abi-1", name="abi-1", kind="evm_abi", body=abi_body)
    registry = AbiRegistry()
    registry.refresh(ConfigSnapshot(version=1, subscriptions=[], channels=[], chains=[], abis=[abi]))
    snap = _snap(
        [_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name="Missing", address="0xaaa")],
        abis=[abi],
    )
    f = build_evm_log_filter(snap, "evm-1", registry)
    assert f.topic0s is None


def test_topics_param_shape():
    f = EvmLogFilterSet(addresses=None, topic0s=["0xaa", "0xbb"], skip_logs=False)
    assert f.topics_param == [["0xaa", "0xbb"]]
    g = EvmLogFilterSet(addresses=None, topic0s=None, skip_logs=False)
    assert g.topics_param is None


def test_only_enabled_subscriptions_considered():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xaaa"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address=None, id="s2", enabled=False),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.addresses == ["0xaaa"]
