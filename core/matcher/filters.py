from __future__ import annotations

from typing import Any


class FilterError(ValueError):
    """Raised when an arg_filters map contains an unknown operator suffix."""


_OPERATOR_SUFFIXES = ("_in", "_gte", "_lte")
_FORBIDDEN_TYPO_SUFFIXES = ("_eq", "_ne", "_gt", "_lt", "_ge", "_le", "_neq", "_like")


def _split(key: str) -> tuple[str, str]:
    """Return (field, op) where op is one of "eq", "in", "gte", "lte"."""
    for s in _OPERATOR_SUFFIXES:
        if key.endswith(s):
            return key[: -len(s)], s[1:]
    return key, "eq"


def _norm(v: Any) -> Any:
    """Lowercase only EVM hex strings (start with '0x'). Solana base58 is case-sensitive."""
    if isinstance(v, str) and v.startswith("0x"):
        return v.lower()
    return v


def validate(filters: dict[str, Any]) -> None:
    """Reject keys that look like a mistyped operator (e.g. `value_eq`, `to_gt`).

    Plain field names are allowed regardless of length or underscores. Only the
    `_OPERATOR_SUFFIXES` (eq/in/gte/lte) and a small forbidden-typo list are
    enforced; everything else passes through as equality.
    """
    for k in filters:
        if any(k.endswith(s) for s in _OPERATOR_SUFFIXES):
            continue
        if any(k.endswith(s) for s in _FORBIDDEN_TYPO_SUFFIXES):
            raise FilterError(
                f"unknown operator in filter key: {k!r}; allowed: <field>, "
                f"<field>_in, <field>_gte, <field>_lte"
            )


def evaluate(filters: dict[str, Any], args: dict[str, Any]) -> bool:
    """AND-combine all filter entries; missing fields fail the match."""
    for key, expected in filters.items():
        field, op = _split(key)
        if field not in args:
            return False
        actual = args[field]
        if op == "eq":
            if _norm(actual) != _norm(expected):
                return False
        elif op == "in":
            allowed = {_norm(v) for v in expected}
            if _norm(actual) not in allowed:
                return False
        elif (op == "gte" and int(actual) < int(expected)) or (
            op == "lte" and int(actual) > int(expected)
        ):
            return False
    return True
