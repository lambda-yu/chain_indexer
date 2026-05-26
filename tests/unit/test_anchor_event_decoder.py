from __future__ import annotations

import struct

import construct

from core.abi.decoder import (
    build_anchor_event_struct,
    decode_anchor_borsh,
)


def _build(fields: list[dict], types: list[dict] | None = None) -> construct.Construct | None:
    event = {"name": "TestEvent", "fields": fields}
    return build_anchor_event_struct(event, types_section=types or [])


def test_scalar_fields_still_work() -> None:
    s = _build([{"name": "price", "type": "u64"}, {"name": "slot", "type": "u64"}])
    assert s is not None
    data = struct.pack("<QQ", 42000, 200)
    parsed = s.parse(data)
    assert parsed["price"] == 42000
    assert parsed["slot"] == 200


def test_vec_of_u64() -> None:
    s = _build([{"name": "values", "type": {"vec": "u64"}}])
    assert s is not None
    # borsh Vec: 4-byte LE length prefix + items
    data = struct.pack("<I", 3) + struct.pack("<QQQ", 10, 20, 30)
    parsed = s.parse(data)
    assert list(parsed["values"]) == [10, 20, 30]


def test_option_u64_present() -> None:
    s = _build([{"name": "maybe", "type": {"option": "u64"}}])
    assert s is not None
    data = b"\x01" + struct.pack("<Q", 999)
    parsed = s.parse(data)
    assert parsed["maybe"] == 999


def test_option_u64_absent() -> None:
    s = _build([{"name": "maybe", "type": {"option": "u64"}}])
    assert s is not None
    data = b"\x00"
    parsed = s.parse(data)
    assert parsed["maybe"] is None


def test_array_of_u8() -> None:
    s = _build([{"name": "hash", "type": {"array": ["u8", 4]}}])
    assert s is not None
    data = bytes([0xDE, 0xAD, 0xBE, 0xEF])
    parsed = s.parse(data)
    assert list(parsed["hash"]) == [0xDE, 0xAD, 0xBE, 0xEF]


def test_defined_nested_struct() -> None:
    types_section = [
        {
            "name": "Price",
            "type": {
                "kind": "struct",
                "fields": [
                    {"name": "value", "type": "u64"},
                    {"name": "decimals", "type": "u8"},
                ],
            },
        }
    ]
    s = _build([{"name": "price", "type": {"defined": "Price"}}], types=types_section)
    assert s is not None
    data = struct.pack("<Q", 42000) + bytes([6])
    result = decode_anchor_borsh(s, data)
    assert result["price"]["value"] == 42000
    assert result["price"]["decimals"] == 6


def test_vec_of_pubkey_decodes_to_base58_list() -> None:
    s = _build([{"name": "signers", "type": {"vec": "pubkey"}}])
    assert s is not None
    key1 = b"\x01" * 32
    key2 = b"\x02" * 32
    data = struct.pack("<I", 2) + key1 + key2
    result = decode_anchor_borsh(s, data)
    assert len(result["signers"]) == 2
    assert isinstance(result["signers"][0], str)


def test_circular_defined_returns_none() -> None:
    types_section = [
        {
            "name": "Node",
            "type": {
                "kind": "struct",
                "fields": [{"name": "child", "type": {"defined": "Node"}}],
            },
        }
    ]
    s = _build([{"name": "root", "type": {"defined": "Node"}}], types=types_section)
    assert s is None


def test_depth_exceeds_limit_returns_none() -> None:
    types_section = []
    for i in range(10):
        next_name = f"Level{i + 1}" if i < 9 else "u64"
        next_type: dict | str = {"defined": next_name} if i < 9 else next_name
        types_section.append({
            "name": f"Level{i}",
            "type": {"kind": "struct", "fields": [{"name": "inner", "type": next_type}]},
        })
    s = _build([{"name": "deep", "type": {"defined": "Level0"}}], types=types_section)
    assert s is None


def test_unknown_compound_type_returns_none() -> None:
    s = _build([{"name": "x", "type": {"hashMap": ["string", "u64"]}}])
    assert s is None
