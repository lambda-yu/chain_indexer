import pytest

from core.matcher.filters import FilterError, evaluate, validate


def test_validate_rejects_unknown_operator() -> None:
    with pytest.raises(FilterError, match="unknown operator"):
        validate({"value_eq": 1})


def test_validate_accepts_plain_field_names_with_underscores() -> None:
    """Plain field names like `first_name`, `user_id` must NOT be rejected."""
    validate({"first_name": "alice", "user_id": "u1", "my_long_field_name": 0})


def test_validate_accepts_known_operators() -> None:
    validate({"to": "0xA", "to_in": ["0xa", "0xb"], "value_gte": "1", "value_lte": "10"})


def test_evaluate_equality_case_insensitive_for_hex_addresses() -> None:
    assert evaluate({"to": "0xAbC"}, {"to": "0xabc", "from": "0x1"}) is True
    assert evaluate({"to": "0xZZZ"}, {"to": "0xabc"}) is False


def test_evaluate_in_membership_case_insensitive() -> None:
    assert evaluate({"to_in": ["0xA", "0xB"]}, {"to": "0xb"}) is True
    assert evaluate({"to_in": ["0xA"]}, {"to": "0xc"}) is False


def test_evaluate_gte_lte_for_decimal_strings() -> None:
    f = {"value_gte": "1000000000000000000", "value_lte": "5000000000000000000"}
    assert evaluate(f, {"value": "1000000000000000000"}) is True
    assert evaluate(f, {"value": "5000000000000000001"}) is False
    assert evaluate(f, {"value": "999999999999999999"}) is False


def test_evaluate_missing_field_fails_match() -> None:
    assert evaluate({"to": "0xa"}, {}) is False


def test_evaluate_empty_filter_matches_anything() -> None:
    assert evaluate({}, {"anything": "goes"}) is True


def test_evaluate_combines_all_keys_with_and() -> None:
    f = {"to": "0xa", "value_gte": "10"}
    assert evaluate(f, {"to": "0xa", "value": "11"}) is True
    assert evaluate(f, {"to": "0xa", "value": "9"}) is False
    assert evaluate(f, {"to": "0xb", "value": "11"}) is False
