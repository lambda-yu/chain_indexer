from __future__ import annotations

import pytest

from core.config.snapshot import SnapshotSubscription
from core.notifier.payload import build_payload
from core.parser.event import Event


def _sub() -> SnapshotSubscription:
    return SnapshotSubscription(
        id="11111111-1111-1111-1111-111111111111",
        name="USDC big transfers",
        chain_id="eth-mainnet",
        address="0xA0b8...",
        abi_id=None,
        match_kind="token_transfer",
        match_name="Transfer",
        arg_filters={},
        enabled=True,
        channel_ids=["c1"],
    )


def _event() -> Event:
    return Event(
        chain_id="eth-mainnet",
        block_number=19000000,
        block_hash="0xbh",
        block_timestamp=1735689600,
        tx_hash="0xtx",
        tx_index=42,
        log_index=7,
        kind="token_transfer",
        contract="0xA0b8...",
        name="Transfer",
        args={"from": "0xfa", "to": "0xfb", "value": "1000000000"},
        raw={},
    )


def test_payload_shape_matches_spec_section_8(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pin clock + delivery_id to assert exact values.
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "delivery-uuid")

    p = build_payload(event=_event(), subscription=_sub())
    assert p == {
        "subscription_id": "11111111-1111-1111-1111-111111111111",
        "subscription_name": "USDC big transfers",
        "chain_id": "eth-mainnet",
        "event": {
            "kind": "token_transfer",
            "name": "Transfer",
            "contract": "0xA0b8...",
            "block_number": 19000000,
            "block_hash": "0xbh",
            "block_timestamp": 1735689600,
            "tx_hash": "0xtx",
            "tx_index": 42,
            "log_index": 7,
            "args": {"from": "0xfa", "to": "0xfb", "value": "1000000000"},
        },
        "delivered_at": 1735689601,
        "delivery_id": "delivery-uuid",
    }
    import json

    json.dumps(p)


def test_delivery_id_is_unique_per_call() -> None:
    a = build_payload(event=_event(), subscription=_sub())
    b = build_payload(event=_event(), subscription=_sub())
    assert a["delivery_id"] != b["delivery_id"]
