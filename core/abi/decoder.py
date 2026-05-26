"""EVM ABI decoders backed by eth-abi + eth-utils. Solana Anchor IDL decoders
land in chunk 13 via `borsh-construct` and live in this same module."""
from __future__ import annotations

from typing import Any

from eth_abi.abi import decode as eth_abi_decode
from eth_utils.abi import (
    event_signature_to_log_topic,
    function_signature_to_4byte_selector,
)

from core.abi.errors import DecodeFailed


def canonical_event_signature(event_abi: dict[str, Any]) -> str:
    """Build `Name(type,type,...)` from a JSON ABI event entry."""
    inputs = event_abi.get("inputs", [])
    types = ",".join(_canonical_type(i) for i in inputs)
    return f"{event_abi['name']}({types})"


def canonical_function_signature(fn_abi: dict[str, Any]) -> str:
    """Build `name(type,type,...)` from a JSON ABI function entry."""
    inputs = fn_abi.get("inputs", [])
    types = ",".join(_canonical_type(i) for i in inputs)
    return f"{fn_abi['name']}({types})"


def _canonical_type(input_entry: dict[str, Any]) -> str:
    """Render a single ABI input's canonical type, including tuples.

    Recursive on `components` for tuple types: a `tuple` with components
    `[uint256, address]` renders as `(uint256,address)`. Array suffixes
    (`[]`, `[3]`) are preserved.
    """
    t: str = input_entry["type"]
    if t.startswith("tuple"):
        comps = input_entry.get("components", [])
        inner = ",".join(_canonical_type(c) for c in comps)
        suffix = t[len("tuple"):]
        return f"({inner}){suffix}"
    return t


def event_topic0(event_abi: dict[str, Any]) -> str:
    sig = canonical_event_signature(event_abi)
    return "0x" + event_signature_to_log_topic(sig).hex()


def function_selector(fn_abi: dict[str, Any]) -> str:
    sig = canonical_function_signature(fn_abi)
    return "0x" + function_signature_to_4byte_selector(sig).hex()


def _split_indexed(event_abi: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    indexed: list[dict[str, Any]] = []
    not_indexed: list[dict[str, Any]] = []
    for inp in event_abi.get("inputs", []):
        if inp.get("indexed"):
            indexed.append(inp)
        else:
            not_indexed.append(inp)
    return indexed, not_indexed


def _normalize_value(t: str, v: Any) -> Any:
    """Match the `Event.args` convention: addresses are 0x-lowercase strings;
    big ints (uint*/int*) are decimal strings; bytes are 0x-hex strings.

    Array / tuple element-wise normalisation is deferred: ABI decode for
    `uint256[]` returns a tuple of Python ints. JSON serialisation in the
    delivery payload (Task 9.x) will widen the contract: parser-side fields
    must already be `Event.args`-compatible (scalar address/int/bytes). Array
    element coercion for ABI-event parsers lands with the AbiEventParser in
    chunk 4 where it actually matters.
    """
    if t == "address":
        assert isinstance(v, str), f"unexpected address type: {type(v).__name__}"
        return v.lower()
    if t.startswith(("uint", "int")) and not t.endswith("]"):
        return str(int(v))
    if t.startswith("bytes") and not t.endswith("]"):
        assert isinstance(v, (bytes, bytearray)), f"unexpected bytes type: {type(v).__name__}"
        return "0x" + v.hex()
    return v


def decode_event(
    event_abi: dict[str, Any], topics: list[str], data: str
) -> dict[str, Any]:
    """Decode an event log per spec §5.2. Returns a `{name: value}` dict
    aligned with `Event.args` conventions.

    Indexed reference-type args (``string``, ``bytes``, arrays, tuples) carry
    the 32-byte topic hash (0x-hex), not the original value — Solidity hashes
    them into the topic and the plaintext is unrecoverable."""
    indexed, not_indexed = _split_indexed(event_abi)
    expected_topic_count = 1 + len(indexed)  # topic0 + indexed inputs
    if len(topics) != expected_topic_count:
        raise DecodeFailed(
            f"topic count {len(topics)} != expected {expected_topic_count} "
            f"for {event_abi.get('name')}"
        )

    args: dict[str, Any] = {}

    # Indexed topics: each topic is the 32-byte abi-encoded value (or hash for
    # dynamic types per Solidity ABI rules). For value types we re-decode.
    for inp, topic_hex in zip(indexed, topics[1:], strict=True):
        t = _canonical_type(inp)
        if t in ("string", "bytes") or t.endswith("]") or t.startswith("("):
            # Reference types: Solidity hashes the value into the topic, so we
            # can't recover the plaintext. Surface as the raw hash hex.
            args[inp["name"]] = topic_hex
            continue
        raw = bytes.fromhex(topic_hex.removeprefix("0x"))
        decoded = eth_abi_decode([t], raw)[0]
        args[inp["name"]] = _normalize_value(t, decoded)

    # Non-indexed: concatenated abi-encoded in `data`.
    if not_indexed:
        types = [_canonical_type(i) for i in not_indexed]
        raw = bytes.fromhex(data.removeprefix("0x"))
        try:
            decoded_tuple = eth_abi_decode(types, raw)
        except (ValueError, OverflowError) as exc:
            raise DecodeFailed(f"data decode failed: {exc}") from exc
        for inp, val in zip(not_indexed, decoded_tuple, strict=True):
            args[inp["name"]] = _normalize_value(_canonical_type(inp), val)

    return args


def decode_function_call(fn_abi: dict[str, Any], calldata: str) -> dict[str, Any]:
    """Decode a function-call `input` per spec §5.2 (kind=call)."""
    expected = function_selector(fn_abi)
    raw = bytes.fromhex(calldata.removeprefix("0x"))
    if len(raw) < 4:
        raise DecodeFailed("calldata shorter than selector")
    sel = "0x" + raw[:4].hex()
    if sel != expected:
        raise DecodeFailed(
            f"selector {sel} != expected {expected} for {fn_abi.get('name')}"
        )

    types = [_canonical_type(i) for i in fn_abi.get("inputs", [])]
    try:
        decoded_tuple = eth_abi_decode(types, raw[4:])
    except (ValueError, OverflowError) as exc:
        raise DecodeFailed(f"calldata decode failed: {exc}") from exc

    out: dict[str, Any] = {}
    for inp, val in zip(fn_abi.get("inputs", []), decoded_tuple, strict=True):
        out[inp["name"]] = _normalize_value(_canonical_type(inp), val)
    return out
