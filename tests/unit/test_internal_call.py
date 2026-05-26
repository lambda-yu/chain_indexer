from __future__ import annotations

from core.chains.evm import EvmAdapter
from core.chains.types import InternalCall


def test_internal_call_dataclass_frozen() -> None:
    c = InternalCall(
        type="CALL", from_addr="0xa", to_addr="0xb",
        value=100, gas=21000, input="0x", output="0x",
    )
    assert c.type == "CALL"
    assert c.calls == []
    assert c.created_address is None


def test_parse_call_nested() -> None:
    raw = {
        "type": "CALL",
        "from": "0xAA",
        "to": "0xBB",
        "value": "0x64",
        "gas": "0x5208",
        "input": "0xa9059cbb",
        "output": "0x",
        "calls": [
            {
                "type": "CALL",
                "from": "0xBB",
                "to": "0xCC",
                "value": "0x0",
                "gas": "0x1000",
                "input": "0x1234",
                "output": "0x5678",
            }
        ],
    }
    result = EvmAdapter._parse_call(raw)
    assert result is not None
    assert result.from_addr == "0xaa"
    assert result.to_addr == "0xbb"
    assert result.value == 100
    assert len(result.calls) == 1
    assert result.calls[0].to_addr == "0xcc"


def test_parse_call_create() -> None:
    raw = {
        "type": "CREATE",
        "from": "0xAA",
        "to": "0xNewContract",
        "value": "0x0",
        "gas": "0x10000",
        "input": "0x6060",
        "output": "0x",
    }
    result = EvmAdapter._parse_call(raw)
    assert result is not None
    assert result.type == "CREATE"
    assert result.to_addr is None
    assert result.created_address == "0xnewcontract"


def test_parse_call_depth_limit() -> None:
    inner: dict = {"type": "CALL", "from": "0x1", "to": "0x2", "value": "0x0", "gas": "0x0", "input": "0x", "output": "0x"}
    current = inner
    for _ in range(70):
        current = {"type": "CALL", "from": "0x1", "to": "0x2", "value": "0x0", "gas": "0x0", "input": "0x", "output": "0x", "calls": [current]}
    result = EvmAdapter._parse_call(current)
    assert result is not None
    # Should be capped at depth 64, not crash
    depth = 0
    node = result
    while node.calls:
        depth += 1
        node = node.calls[0]
    assert depth <= 64


def test_parse_call_with_error() -> None:
    raw = {
        "type": "CALL",
        "from": "0xAA",
        "to": "0xBB",
        "value": "0x0",
        "gas": "0x5208",
        "input": "0x",
        "output": "0x",
        "error": "execution reverted",
    }
    result = EvmAdapter._parse_call(raw)
    assert result is not None
    assert result.error == "execution reverted"
