
from datetime import datetime
import math


SUPPORTED_CURRENCIES = {"INR"}
VALID_STATUSES = {"SUCCESS", "FAILED", "PENDING"}
DEFAULT_HIGH_VALUE_THRESHOLD = 50_000.0


def validate_transaction(transaction: dict) -> list[str]:
    """Return validation errors for a transaction; an empty list means valid."""
    errors = []

    if not transaction.get("transaction_id"):
        errors.append("missing_transaction_id")

    amount = transaction.get("amount")
    try:
        numeric_amount = float(amount)
        if not math.isfinite(numeric_amount):
            errors.append("invalid_amount")
        elif numeric_amount <= 0:
            errors.append("amount_must_be_positive")
    except (TypeError, ValueError):
        errors.append("invalid_amount")

    currency = transaction.get("currency")
    if currency not in SUPPORTED_CURRENCIES:
        errors.append("unsupported_currency")

    status = transaction.get("status")
    if status not in VALID_STATUSES:
        errors.append("invalid_status")

    event_time = transaction.get("event_time")
    try:
        if not event_time:
            raise ValueError("Missing timestamp")
        datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError):
        errors.append("invalid_event_timestamp")

    return errors


def classify_risk(
    amount: float,
    threshold: float = DEFAULT_HIGH_VALUE_THRESHOLD,
) -> dict:
    """Classify a transaction by amount using the configured threshold."""
    numeric_amount = float(amount)
    numeric_threshold = float(threshold)

    if not math.isfinite(numeric_threshold) or numeric_threshold <= 0:
        raise ValueError("threshold must be positive")

    if not math.isfinite(numeric_amount) or numeric_amount <= 0:
        raise ValueError("amount must be positive")

    if numeric_amount >= numeric_threshold:
        return {
            "risk_reason": "high_value_transaction",
            "risk_level": "HIGH",
            "risk_threshold": numeric_threshold,
        }

    return {
        "risk_reason": None,
        "risk_level": "NORMAL",
        "risk_threshold": numeric_threshold,
    }
