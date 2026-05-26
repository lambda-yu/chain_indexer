from __future__ import annotations

import pytest

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
