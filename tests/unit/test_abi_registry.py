from __future__ import annotations

import pytest

from core.abi.decoder import event_topic0, function_selector
from core.abi.errors import AbiNotFound
from core.abi.registry import AbiRegistry
from core.config.snapshot import ConfigSnapshot, SnapshotAbi

_ERC20_TRANSFER = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}


def _snap_with(*abis: SnapshotAbi) -> ConfigSnapshot:
    return ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[], abis=list(abis),
    )


def test_registry_returns_body_by_abi_id() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    body = r.get_body("a1")
    assert body == [_ERC20_TRANSFER]


def test_registry_raises_for_unknown_abi() -> None:
    r = AbiRegistry()
    r.refresh(_snap_with())
    with pytest.raises(AbiNotFound):
        r.get_body("does-not-exist")


def test_refresh_evicts_deleted_abis() -> None:
    snap1 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    snap2 = _snap_with()  # a1 deleted
    r = AbiRegistry()
    r.refresh(snap1)
    assert r.get_body("a1") == [_ERC20_TRANSFER]
    r.refresh(snap2)
    with pytest.raises(AbiNotFound):
        r.get_body("a1")


def test_refresh_replaces_body_when_hash_changes() -> None:
    body_v1 = [_ERC20_TRANSFER]
    body_v2 = [{**_ERC20_TRANSFER, "name": "TransferV2"}]
    snap_v1 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body_v1))
    snap_v2 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body_v2))
    r = AbiRegistry()
    r.refresh(snap_v1)
    assert r.get_body("a1") == body_v1
    r.refresh(snap_v2)
    assert r.get_body("a1") == body_v2


def test_registry_decoder_cache_persists_across_refresh_if_body_unchanged() -> None:
    """When an ABI's body hash is unchanged across two refreshes, _evict
    is not called for that abi_id, so any prior decoder entries in
    `_decoders[(abi_id, *)]` should survive. We prove this by poking a
    sentinel into `_decoders` and confirming refresh preserves it; the
    real-decoder identity check lives in Task 2.7
    (test_decoder_cache_is_reused_for_same_abi_id_and_key)."""
    body = [_ERC20_TRANSFER]
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body))
    r = AbiRegistry()
    r.refresh(snap)
    sentinel = object()
    # Prime the internal decoder dict with a sentinel under a fake key.
    r._decoders[("a1", "0xdead")] = sentinel  # type: ignore[index]
    r.refresh(snap)  # body hash unchanged → preserve cache
    assert r._decoders.get(("a1", "0xdead")) is sentinel


_FN_TRANSFER = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to", "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}


def test_get_event_decoder_returns_decoder_for_known_topic0() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    decoder = r.get_event_decoder("a1", t0)
    args = decoder(
        topics=[
            t0,
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ],
        data="0x000000000000000000000000000000000000000000000000000000000000007b",
    )
    assert args["from"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert args["value"] == "123"


def test_get_event_decoder_raises_for_unknown_topic0() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    with pytest.raises(KeyError):
        r.get_event_decoder("a1", "0xdeadbeef" + "00" * 28)


def test_get_call_decoder_returns_decoder_for_known_selector() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_FN_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    decoder = r.get_call_decoder("a1", sel)
    args = decoder(
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    assert args["value"] == "999"


def test_decoder_cache_is_reused_for_same_abi_id_and_key() -> None:
    """Calling get_event_decoder twice for the same (abi_id, topic0) should
    return the same callable instance (cached)."""
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    d1 = r.get_event_decoder("a1", t0)
    d2 = r.get_event_decoder("a1", t0)
    assert d1 is d2


_SECOND_EVENT = {
    "type": "event", "name": "Approval",
    "inputs": [
        {"name": "owner",   "type": "address", "indexed": True},
        {"name": "spender", "type": "address", "indexed": True},
        {"name": "value",   "type": "uint256", "indexed": False},
    ],
}


def test_lookup_event_by_topic0_returns_decoder_for_known_topic() -> None:
    snap = _snap_with(SnapshotAbi(
        id="a1", name="erc20", kind="evm_abi",
        body=[_ERC20_TRANSFER, _SECOND_EVENT],
    ))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    result = r.lookup_event_by_topic0(t0)
    assert result is not None
    name, decoder = result
    assert name == "Transfer"
    args = decoder(
        topics=[
            t0,
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ],
        data="0x000000000000000000000000000000000000000000000000000000000000007b",
    )
    assert args["value"] == "123"


def test_lookup_event_by_topic0_returns_none_for_unknown() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.lookup_event_by_topic0("0xdead" + "00" * 30) is None


def test_lookup_event_picks_first_abi_on_topic0_collision() -> None:
    snap = _snap_with(
        SnapshotAbi(id="a1", name="erc20a", kind="evm_abi", body=[_ERC20_TRANSFER]),
        SnapshotAbi(id="a2", name="erc20b", kind="evm_abi", body=[_ERC20_TRANSFER]),
    )
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    result = r.lookup_event_by_topic0(t0)
    assert result is not None
    assert r.lookup_event_by_topic0(t0) is result


def test_topic0_index_rebuilt_on_abi_removal() -> None:
    snap_with = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    snap_without = _snap_with()
    r = AbiRegistry()
    r.refresh(snap_with)
    t0 = event_topic0(_ERC20_TRANSFER)
    assert r.lookup_event_by_topic0(t0) is not None
    r.refresh(snap_without)
    assert r.lookup_event_by_topic0(t0) is None


_FN_APPROVE = {
    "type": "function", "name": "approve",
    "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "value",   "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}


def test_lookup_function_by_selector_returns_decoder_for_known_selector() -> None:
    snap = _snap_with(SnapshotAbi(
        id="a1", name="erc20", kind="evm_abi",
        body=[_FN_TRANSFER, _FN_APPROVE],
    ))
    r = AbiRegistry()
    r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    result = r.lookup_function_by_selector(sel)
    assert result is not None
    name, decoder = result
    assert name == "transfer"
    args = decoder(
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    assert args["to"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert args["value"] == "999"


def test_lookup_function_by_selector_returns_none_for_unknown() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_FN_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.lookup_function_by_selector("0xdeadbeef") is None


def test_lookup_function_picks_first_abi_on_selector_collision() -> None:
    snap = _snap_with(
        SnapshotAbi(id="a1", name="erc20a", kind="evm_abi", body=[_FN_TRANSFER]),
        SnapshotAbi(id="a2", name="erc20b", kind="evm_abi", body=[_FN_TRANSFER]),
    )
    r = AbiRegistry()
    r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    first = r.lookup_function_by_selector(sel)
    assert first is not None
    assert r.lookup_function_by_selector(sel) is first


def test_event_topic0_for_returns_lowercase_topic0_for_known_event() -> None:
    snap = _snap_with(SnapshotAbi(
        id="a1", name="erc20", kind="evm_abi",
        body=[_ERC20_TRANSFER, _SECOND_EVENT],
    ))
    r = AbiRegistry()
    r.refresh(snap)
    expected = event_topic0(_ERC20_TRANSFER).lower()
    assert r.event_topic0_for("a1", "Transfer") == expected
    # And for the second event in the same abi.
    expected_2 = event_topic0(_SECOND_EVENT).lower()
    assert r.event_topic0_for("a1", "Approval") == expected_2


def test_event_topic0_for_returns_none_for_unknown_event_name() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.event_topic0_for("a1", "DoesNotExist") is None


def test_event_topic0_for_returns_none_for_unknown_abi_id() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.event_topic0_for("missing-abi", "Transfer") is None


def test_selector_index_rebuilt_on_abi_removal() -> None:
    snap_with = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_FN_TRANSFER]))
    snap_without = _snap_with()
    r = AbiRegistry()
    r.refresh(snap_with)
    sel = function_selector(_FN_TRANSFER)
    assert r.lookup_function_by_selector(sel) is not None
    r.refresh(snap_without)
    assert r.lookup_function_by_selector(sel) is None
