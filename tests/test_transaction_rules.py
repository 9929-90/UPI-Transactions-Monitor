
import pytest

from src.validation.transaction_rules import (
    classify_risk,
    validate_transaction,
)


@pytest.fixture
def valid_transaction():
    return {
        "transaction_id": "UPI-001",
        "amount": 250.0,
        "currency": "INR",
        "status": "SUCCESS",
        "event_time": "2026-10-27T12:00:00+05:30",
    }


def test_valid_transaction_has_no_errors(valid_transaction):
    assert validate_transaction(valid_transaction) == []


@pytest.mark.parametrize(
    ("amount", "expected_error"),
    [
        (0, "amount_must_be_positive"),
        (-10, "amount_must_be_positive"),
        ("invalid", "invalid_amount"),
        (None, "invalid_amount"),
    ],
)
def test_invalid_amounts(valid_transaction, amount, expected_error):
    valid_transaction["amount"] = amount
    assert expected_error in validate_transaction(valid_transaction)


def test_missing_transaction_id_is_rejected(valid_transaction):
    valid_transaction["transaction_id"] = ""
    assert "missing_transaction_id" in validate_transaction(valid_transaction)


def test_unsupported_currency_is_rejected(valid_transaction):
    valid_transaction["currency"] = "USD"
    assert "unsupported_currency" in validate_transaction(valid_transaction)


def test_invalid_timestamp_is_rejected(valid_transaction):
    valid_transaction["event_time"] = "not-a-timestamp"
    assert "invalid_event_timestamp" in validate_transaction(valid_transaction)


def test_high_value_transaction_is_flagged():
    result = classify_risk(75_000)
    assert result["risk_reason"] == "high_value_transaction"
    assert result["risk_level"] == "HIGH"
    assert result["risk_threshold"] == 50_000


def test_amount_equal_to_threshold_is_flagged():
    assert classify_risk(50_000)["risk_level"] == "HIGH"


def test_normal_transaction_is_not_flagged():
    result = classify_risk(499)
    assert result["risk_reason"] is None
    assert result["risk_level"] == "NORMAL"


def test_non_positive_risk_threshold_is_rejected():
    with pytest.raises(ValueError):
        classify_risk(100, threshold=0)

@pytest.mark.parametrize("amount", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_amount_is_rejected(valid_transaction, amount):
    valid_transaction["amount"] = amount
    assert "invalid_amount" in validate_transaction(valid_transaction)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_risk_threshold_is_rejected(value):
    with pytest.raises(ValueError):
        classify_risk(100, threshold=value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_risk_amount_is_rejected(value):
    with pytest.raises(ValueError):
        classify_risk(value)
