from __future__ import annotations

from core.abi.registry import AbiRegistry
from core.chains.types import Block, BlockHeader, InternalCall
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.internal_call import InternalCallParser

_FN_TRANSFER = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to", "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}

_TRANSFER_SELECTOR = "0xa9059cbb"


def _registry() -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_FN_TRANSFER])],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _block() -> Block:
    return Block(header=BlockHeader(number=42, hash="0xh42", parent_hash="0xh41", timestamp=1700000000))


def _call(
    *,
    type: str = "CALL",
    to: str | None = "0xtoken",
    input: str = "0x",
    value: int = 0,
    error: str | None = None,
    calls: list[InternalCall] | None = None,
    created_address: str | None = None,
) -> InternalCall:
    return InternalCall(
        type=type, from_addr="0xsender", to_addr=to,
        value=value, gas=21000, input=input, output="0x",
        error=error, calls=calls or [], created_address=created_address,
    )


def test_decodes_known_selector() -> None:
    reg = _registry()
    p = InternalCallParser(chain_id="eth", registry=reg)
    calldata = (
        _TRANSFER_SELECTOR
        + "0" * 24 + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        + "0" * 62 + "01"
    )
    traces = [_call(input=calldata)]
    events = list(p.parse(traces, _block()))
    assert len(events) == 1
    assert events[0].kind == "call"
    assert events[0].name == "transfer"


def test_skips_unknown_selector() -> None:
    reg = _registry()
    p = InternalCallParser(chain_id="eth", registry=reg)
    traces = [_call(input="0xdeadbeef" + "00" * 32)]
    assert list(p.parse(traces, _block())) == []


def test_skips_failed_calls() -> None:
    reg = _registry()
    p = InternalCallParser(chain_id="eth", registry=reg)
    calldata = _TRANSFER_SELECTOR + "0" * 24 + "bb" * 20 + "0" * 62 + "01"
    traces = [_call(input=calldata, error="revert")]
    assert list(p.parse(traces, _block())) == []


def test_emits_create_event() -> None:
    reg = _registry()
    p = InternalCallParser(chain_id="eth", registry=reg)
    traces = [_call(type="CREATE", to=None, created_address="0xnew", value=100)]
    events = list(p.parse(traces, _block()))
    assert len(events) == 1
    assert events[0].name == "<create>"
    assert events[0].contract == "0xnew"
    assert events[0].args["value"] == "100"


def test_walks_nested_calls() -> None:
    reg = _registry()
    p = InternalCallParser(chain_id="eth", registry=reg)
    calldata = (
        _TRANSFER_SELECTOR
        + "0" * 24 + "bb" * 20
        + "0" * 62 + "01"
    )
    inner = _call(input=calldata)
    outer = _call(input="0xdeadbeef" + "00" * 32, calls=[inner])
    events = list(p.parse([outer], _block()))
    assert len(events) == 1
    assert events[0].name == "transfer"
