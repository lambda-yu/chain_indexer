"""EVM ABI decoders backed by eth-abi + eth-utils. Solana Anchor IDL decoders
land in chunk 13 via `borsh-construct` and live in this same module."""
from __future__ import annotations

import hashlib
from typing import Any

import base58 as _base58
import borsh_construct
import construct
from eth_abi.abi import decode as eth_abi_decode
from eth_abi.exceptions import DecodingError as EthAbiDecodingError
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
        except (ValueError, OverflowError, EthAbiDecodingError) as exc:
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
    except (ValueError, OverflowError, EthAbiDecodingError) as exc:
        raise DecodeFailed(f"calldata decode failed: {exc}") from exc

    out: dict[str, Any] = {}
    for inp, val in zip(fn_abi.get("inputs", []), decoded_tuple, strict=True):
        out[inp["name"]] = _normalize_value(_canonical_type(inp), val)
    return out


# ---------------------------------------------------------------------------
# Anchor IDL helpers (Solana)
# ---------------------------------------------------------------------------

_ANCHOR_SCALAR_MAP: dict[str, construct.Construct[Any, Any]] = {
    "bool": borsh_construct.Bool,
    "u8": borsh_construct.U8,
    "u16": borsh_construct.U16,
    "u32": borsh_construct.U32,
    "u64": borsh_construct.U64,
    "u128": borsh_construct.U128,
    "i8": borsh_construct.I8,
    "i16": borsh_construct.I16,
    "i32": borsh_construct.I32,
    "i64": borsh_construct.I64,
    "i128": borsh_construct.I128,
    "bytes": borsh_construct.Bytes,
    "string": borsh_construct.String,
    "pubkey": construct.Bytes(32),
    "publicKey": construct.Bytes(32),
}

_MAX_TYPE_DEPTH = 8


def _resolve_type(
    type_spec: Any,
    types_section: list[dict[str, Any]],
    seen: frozenset[str] = frozenset(),
    depth: int = 0,
) -> construct.Construct[Any, Any] | None:
    if depth >= _MAX_TYPE_DEPTH:
        return None
    if isinstance(type_spec, str):
        return _ANCHOR_SCALAR_MAP.get(type_spec)
    if not isinstance(type_spec, dict):
        return None

    if "defined" in type_spec:
        name = type_spec["defined"]
        if name in seen:
            return None
        for t in types_section:
            if t.get("name") == name:
                kind_obj = t.get("type", {})
                if kind_obj.get("kind") != "struct":
                    return None
                fields: list[Any] = []
                for f in kind_obj.get("fields", []):
                    resolved = _resolve_type(
                        f.get("type"), types_section, seen | {name}, depth + 1,
                    )
                    if resolved is None:
                        return None
                    fields.append(f["name"] / resolved)
                return borsh_construct.CStruct(*fields)
        return None

    if "vec" in type_spec:
        inner = _resolve_type(type_spec["vec"], types_section, seen, depth + 1)
        return borsh_construct.Vec(inner) if inner is not None else None

    if "option" in type_spec:
        inner = _resolve_type(type_spec["option"], types_section, seen, depth + 1)
        return borsh_construct.Option(inner) if inner is not None else None

    if "array" in type_spec:
        arr = type_spec["array"]
        if isinstance(arr, list) and len(arr) == 2:
            inner = _resolve_type(arr[0], types_section, seen, depth + 1)
            if inner is not None:
                return construct.Array(arr[1], inner)
        return None

    return None


def anchor_event_discriminator(event_name: str) -> bytes:
    return hashlib.sha256(f"event:{event_name}".encode()).digest()[:8]


def anchor_call_discriminator(fn_name: str) -> bytes:
    return hashlib.sha256(f"global:{fn_name}".encode()).digest()[:8]


def build_anchor_event_struct(
    idl_event: dict[str, Any],
    types_section: list[dict[str, Any]] | None = None,
) -> construct.Construct[Any, Any] | None:
    ts = types_section or []
    fields_spec: list[Any] = []
    for field in idl_event.get("fields", []):
        resolved = _resolve_type(field.get("type"), ts)
        if resolved is None:
            return None
        fields_spec.append(field["name"] / resolved)
    return borsh_construct.CStruct(*fields_spec)


def _normalize_borsh_value(v: Any) -> Any:
    if isinstance(v, bytes) and len(v) == 32:
        return _base58.b58encode(v).decode()
    if isinstance(v, int):
        return str(v) if abs(v) > 2**53 else v
    if isinstance(v, list):
        return [_normalize_borsh_value(item) for item in v]
    if hasattr(v, "items"):
        return {k: _normalize_borsh_value(val) for k, val in v.items() if k != "_io"}
    return v


def decode_anchor_borsh(
    struct: construct.Construct[Any, Any],
    body_bytes: bytes,
) -> dict[str, Any]:
    try:
        parsed = struct.parse(body_bytes)
    except Exception as exc:
        raise DecodeFailed(f"borsh decode failed: {exc}") from exc
    out: dict[str, Any] = {}
    for k, v in parsed.items():
        if k == "_io":
            continue
        out[k] = _normalize_borsh_value(v)
    return out


# Backward compat alias
decode_anchor_event = decode_anchor_borsh
