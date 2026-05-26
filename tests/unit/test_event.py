from core.parser.event import Event


def test_event_round_trip_dict() -> None:
    e = Event(
        chain_id="eth-mainnet",
        block_number=100,
        block_hash="0xbb",
        block_timestamp=1700000000,
        tx_hash="0xtx",
        tx_index=3,
        log_index=None,
        kind="native_transfer",
        contract=None,
        name=None,
        args={"from": "0xa", "to": "0xb", "value": "1000"},
        raw={},
    )
    assert e.kind == "native_transfer"
    assert e.args["value"] == "1000"


def test_event_kind_literal_contains_event_not_log() -> None:
    from typing import get_args

    from core.parser.event import EventKind

    kinds = set(get_args(EventKind))
    assert "event" in kinds
    assert "log" not in kinds
    assert {"native_transfer", "token_transfer", "call"}.issubset(kinds)
