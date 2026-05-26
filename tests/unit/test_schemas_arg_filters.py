from __future__ import annotations

import pytest
from pydantic import ValidationError

from apps.web.schemas import SubscriptionCreate


def _base(**overrides):  # type: ignore[no-untyped-def]
    payload = dict(
        name="s1",
        chain_id="eth-mainnet",
        address=None,
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
    )
    payload.update(overrides)
    return payload


def test_arg_filters_accepts_string_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"from": "0xabc"}))
    assert s.arg_filters == {"from": "0xabc"}


def test_arg_filters_accepts_int_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"value_gte": 1000}))
    assert s.arg_filters == {"value_gte": 1000}


def test_arg_filters_accepts_bool_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"is_live": True}))
    assert s.arg_filters is not None
    assert s.arg_filters["is_live"] is True


def test_arg_filters_accepts_list_of_primitives() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"to_in": ["0x1", "0x2", "0x3"]}))
    assert s.arg_filters == {"to_in": ["0x1", "0x2", "0x3"]}


def test_arg_filters_empty_dict_is_accepted() -> None:
    s = SubscriptionCreate(**_base(arg_filters={}))
    assert s.arg_filters == {}


def test_arg_filters_rejects_nested_dict() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SubscriptionCreate(**_base(arg_filters={"from": {"nested": "dict"}}))
    msg = str(exc_info.value)
    assert "arg_filters" in msg


def test_arg_filters_rejects_float_value() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"value_gte": 1.5}))


def test_arg_filters_rejects_none_value() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"x": None}))


def test_arg_filters_rejects_list_with_dict_element() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(
            **_base(arg_filters={"to_in": ["0x1", {"nested": "x"}]})
        )


def test_arg_filters_rejects_list_with_float_element() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"vals_in": [1, 2.5]}))


def test_arg_filters_rejects_typo_eq_suffix() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SubscriptionCreate(**_base(arg_filters={"value_eq": 100}))
    assert "value_eq" in str(exc_info.value) or "unknown operator" in str(exc_info.value)


def test_arg_filters_rejects_typo_ne_suffix() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"to_ne": "0x1"}))


def test_arg_filters_accepts_valid_operator_suffixes() -> None:
    s = SubscriptionCreate(**_base(arg_filters={
        "to": "0xabc",
        "to_in": ["0x1", "0x2"],
        "value_gte": 100,
        "value_lte": 200,
    }))
    assert set(s.arg_filters.keys()) == {"to", "to_in", "value_gte", "value_lte"}
