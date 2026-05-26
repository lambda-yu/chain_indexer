from __future__ import annotations

from core.abi.decoder import function_selector
from core.abi.registry import AbiRegistry
from core.chains.types import Block, BlockHeader, Tx
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.abi_call import AbiCallParser

_FN_TRANSFER = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to",    "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}
_FN_APPROVE = {
    "type": "function", "name": "approve",
    "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "value",   "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}
_TRANSFER_SEL = function_selector(_FN_TRANSFER)   # "0xa9059cbb"
_APPROVE_SEL = function_selector(_FN_APPROVE)     # "0x095ea7b3"
_TO = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _registry_with(*entries: dict) -> AbiRegistry:  # type: ignore[type-arg]
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=list(entries))],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _block(txs: list[Tx]) -> Block:
    return Block(
        header=BlockHeader(number=42, hash="0xh42", parent_hash="0xh41", timestamp=1700000000),
        txs=txs,
        logs=[],
    )


def _transfer_calldata(value: int = 999) -> str:
    return (
        _TRANSFER_SEL
        + "0" * 24 + _TO
        + format(value, "064x")
    )


def test_emits_call_kind_for_known_selector_with_decoded_args() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    tx = Tx(
        hash="0xt1", index=0, from_addr="0xf00", to_addr="0xCAFE",
        value=0, input=_transfer_calldata(value=999), status=1,
    )
    events = list(p.parse(_block([tx])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "call"
    assert e.name == "transfer"
    assert e.contract == "0xcafe"
    assert e.args == {
        "to":    "0x" + _TO,
        "value": "999",
    }
    assert e.chain_id == "eth-mainnet"
    assert e.block_number == 42
    assert e.tx_hash == "0xt1"
    assert e.tx_index == 0
    assert e.log_index is None


def test_skips_tx_with_unknown_selector_no_downgrade() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    unknown = "0xdeadbeef" + "00" * 32
    tx = Tx(hash="0xt2", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=unknown, status=1)
    assert list(p.parse(_block([tx]))) == []


def test_skips_tx_with_empty_or_short_input() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    txs = [
        Tx(hash="0xt3", index=0, from_addr="0xf", to_addr="0xc", value=1, input="0x", status=1),
        Tx(hash="0xt4", index=1, from_addr="0xf", to_addr="0xc", value=0, input="0xa905", status=1),
        Tx(hash="0xt5", index=2, from_addr="0xf", to_addr="0xc", value=0, input="", status=1),
    ]
    assert list(p.parse(_block(txs))) == []


def test_skips_failed_tx() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    tx = Tx(hash="0xt6", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=_transfer_calldata(), status=0)
    assert list(p.parse(_block([tx]))) == []


def test_skips_contract_creation_tx() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    initcode = _transfer_calldata(value=999)
    tx = Tx(hash="0xt8", index=0, from_addr="0xf", to_addr=None,
            value=0, input=initcode, status=1)
    assert list(p.parse(_block([tx]))) == []


def test_skips_known_selector_on_decode_failure() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    bad = _TRANSFER_SEL + "00" * 16
    tx = Tx(hash="0xt7", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=bad, status=1)
    events = list(p.parse(_block([tx])))
    assert events == []


def test_emits_one_event_per_matching_tx() -> None:
    reg = _registry_with(_FN_TRANSFER, _FN_APPROVE)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    txs = [
        Tx(hash="0xtA", index=0, from_addr="0xf", to_addr="0xC1",
           value=0, input=_transfer_calldata(value=100), status=1),
        Tx(hash="0xtB", index=1, from_addr="0xf", to_addr="0xC2",
           value=0,
           input=_APPROVE_SEL + "0" * 24 + _TO + format(7, "064x"),
           status=1),
        Tx(hash="0xtC", index=2, from_addr="0xf", to_addr="0xC3",
           value=0, input="0xdead" + "beef" * 16, status=1),
    ]
    events = list(p.parse(_block(txs)))
    assert [e.name for e in events] == ["transfer", "approve"]
    assert [e.kind for e in events] == ["call", "call"]
    assert [e.tx_index for e in events] == [0, 1]


def test_preserves_raw_input_for_debugging() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    calldata = _transfer_calldata(value=42)
    tx = Tx(hash="0xt9", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=calldata, status=1)
    events = list(p.parse(_block([tx])))
    assert len(events) == 1
    assert events[0].raw["input"] == calldata
    assert events[0].raw["tx_hash"] == "0xt9"
