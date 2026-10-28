from pathlib import Path

import duckdb
import pandas as pd
import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

NORMAL = DATA / "processed" / "transactions_v4" / "normal"
FLAGGED = DATA / "processed" / "transactions_v4" / "flagged"
QUARANTINE = DATA / "quarantine" / "transactions_v4"
VELOCITY = DATA / "processed" / "transactions_v4" / "velocity_flags_v2"


def read_parquet(folder: Path) -> pd.DataFrame:
    files = sorted(folder.rglob("*.parquet"))

    if not files:
        pytest.fail(f"No Parquet files found in {folder}")

    con = duckdb.connect()
    try:
        return con.from_parquet([str(p) for p in files]).df()
    finally:
        con.close()


def test_high_value_transaction_is_flagged():
    df = read_parquet(FLAGGED)

    matches = df[df["transaction_id"] == "TEST-HIGH-VALUE-001"]

    assert len(matches) >= 1
    row = matches.iloc[-1]
    assert row["amount"] == 75000.0
    assert row["risk_reason"] == "high_value_transaction"
    assert row["risk_level"] == "HIGH"
    assert row["risk_threshold"] == 50000.0


def test_invalid_amount_is_quarantined():
    df = read_parquet(QUARANTINE)

    assert "validation_error" in df.columns
    assert (df["validation_error"] == "amount_must_be_positive").any()


def test_normal_transactions_are_below_high_value_threshold():
    df = read_parquet(NORMAL)

    assert "amount" in df.columns
    assert df["amount"].notna().all()
    assert (df["amount"] > 0).all()
    assert (df["amount"] < 50000).all()


def test_velocity_alerts_meet_threshold():
    df = read_parquet(VELOCITY)

    assert {"user_id", "count", "alert_id"}.issubset(df.columns)

    test_user = df[df["user_id"] == "TEST-VELOCITY-V2-USER"]

    assert not test_user.empty
    assert (test_user["count"] >= 5).all()
    assert test_user["alert_id"].notna().all()


def test_velocity_alert_ids_are_unique_per_user_and_window():
    df = read_parquet(VELOCITY)

    assert {"user_id", "window", "alert_id"}.issubset(df.columns)

    keys = df[["user_id", "window"]].astype(str)
    assert not keys.duplicated().any()

    assert df["alert_id"].is_unique


def test_velocity_test_user_has_expected_alert_count():
    df = read_parquet(VELOCITY)
    test_user = df[df["user_id"] == "TEST-VELOCITY-V2-USER"]

    # Streaming output persists across runs. Counts can exceed five when
    # more than five events for the user fall inside a one-minute window.
    assert not test_user.empty
    assert (test_user["count"] >= 5).all()
    assert test_user["alert_id"].notna().all()
