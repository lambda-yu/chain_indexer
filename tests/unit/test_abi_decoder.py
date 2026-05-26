from __future__ import annotations

import pytest

from core.abi.decoder import (
    canonical_event_signature,
    canonical_function_signature,
    decode_event,
    decode_function_call,
    event_topic0,
    function_selector,
)
from core.abi.errors import DecodeFailed

_ERC20_TRANSFER_EVENT = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}

_TRANSFER_FN = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to",    "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
    "stateMutability": "nonpayable",
}


def test_canonical_signatures() -> None:
    assert canonical_event_signature(_ERC20_TRANSFER_EVENT) == "Transfer(address,address,uint256)"
    assert canonical_function_signature(_TRANSFER_FN) == "transfer(address,uint256)"


def test_event_topic0_is_keccak() -> None:
    t = event_topic0(_ERC20_TRANSFER_EVENT)
    # ERC-20 Transfer canonical topic0:
    assert t == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def test_function_selector_is_first_4_bytes_of_keccak() -> None:
    s = function_selector(_TRANSFER_FN)
    # `transfer(address,uint256)` 4-byte selector:
    assert s == "0xa9059cbb"


def test_decode_event_extracts_indexed_and_data() -> None:
    """Decoded args dict contains all fields with addresses lowercased and
    big ints as decimal strings (matches Event.args convention)."""
    topics = [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # topic0
        "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",  # from
        "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",  # to
    ]
    # uint256(123) encoded:
    data = "0x000000000000000000000000000000000000000000000000000000000000007b"
    args = decode_event(_ERC20_TRANSFER_EVENT, topics, data)
    assert args == {
        "from":  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "to":    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "value": "123",
    }


def test_decode_event_fails_on_topic_count_mismatch() -> None:
    with pytest.raises(DecodeFailed, match="topic count"):
        decode_event(_ERC20_TRANSFER_EVENT, ["0xddf2..."], "0x")


def test_decode_function_call_extracts_args() -> None:
    # transfer(0xbbbb...bbb, 999)
    calldata = (
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    args = decode_function_call(_TRANSFER_FN, calldata)
    assert args == {
        "to":    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "value": "999",
    }


def test_decode_function_call_fails_on_wrong_selector() -> None:
    bad = "0xdeadbeef" + ("00" * 32)
    with pytest.raises(DecodeFailed, match="selector"):
        decode_function_call(_TRANSFER_FN, bad)
