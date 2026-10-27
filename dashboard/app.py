
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

NORMAL_DIR = DATA / "processed" / "transactions_v4" / "normal"
FLAGGED_DIR = DATA / "processed" / "transactions_v4" / "flagged"
VELOCITY_DIR = (
    DATA / "processed" / "transactions_v4" / "velocity_flags"
)
QUARANTINE_DIR = DATA / "quarantine" / "transactions_v4"

st.set_page_config(
    page_title="UPI Transaction Monitor",
    page_icon="📊",
    layout="wide",
)

st.title("UPI Transaction Monitoring")
st.caption(
    "Local analytics dashboard | PySpark Structured Streaming, "
    "Parquet and DuckDB"
)


def parquet_pattern(folder):
    files = list(folder.glob("**/*.parquet"))
    if not files:
        return None
    return (folder / "**" / "*.parquet").as_posix().replace("'", "''")


def read_dataset(folder):
    pattern = parquet_pattern(folder)
    if not pattern:
        return pd.DataFrame()

    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT *
            FROM read_parquet(
                '{pattern}',
                union_by_name = true
            )
            """
        ).fetchdf()
    finally:
        con.close()


def read_velocity_summary():
    pattern = parquet_pattern(VELOCITY_DIR)
    if not pattern:
        return pd.DataFrame()

    con = duckdb.connect()
    try:
        return con.execute(
            f"""
            SELECT
                user_id,
                "window".start AS window_start,
                "window"."end" AS window_end,
                "count" AS transaction_count,
                batch_id
            FROM read_parquet(
                '{pattern}',
                union_by_name = true
            )
            ORDER BY window_start DESC
            """
        ).fetchdf()
    finally:
        con.close()


if st.button("Refresh dashboard", type="primary"):
    st.rerun()

normal = read_dataset(NORMAL_DIR)
flagged = read_dataset(FLAGGED_DIR)
quarantine = read_dataset(QUARANTINE_DIR)
velocity = read_velocity_summary()

normal_count = len(normal)
flagged_count = len(flagged)
quarantine_count = len(quarantine)
velocity_windows = len(velocity)

st.subheader("Operational overview")

c1, c2, c3, c4 = st.columns(4)

c1.metric("Normal records", f"{normal_count:,}")
c2.metric("High-value flags", f"{flagged_count:,}")
c3.metric("Quarantined records", f"{quarantine_count:,}")
c4.metric("Velocity alert windows", f"{velocity_windows:,}")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("High-value transactions")

    if flagged.empty:
        st.info("No high-value transactions recorded.")
    else:
        display_cols = [
            c for c in [
                "transaction_id",
                "user_id",
                "amount",
                "risk_reason",
                "risk_level",
                "risk_threshold",
                "event_time",
            ]
            if c in flagged.columns
        ]

        st.dataframe(
            flagged[display_cols],
            use_container_width=True,
            hide_index=True,
        )

        if "amount" in flagged.columns:
            st.metric(
                "Total flagged amount",
                f"₹{flagged['amount'].sum():,.2f}",
            )

with right:
    st.subheader("Quarantine breakdown")

    if quarantine.empty:
        st.success("No quarantined records.")
    elif "validation_error" in quarantine.columns:
        breakdown = (
            quarantine["validation_error"]
            .fillna("unknown")
            .value_counts()
            .rename_axis("Validation error")
            .reset_index(name="Records")
        )
        st.dataframe(
            breakdown,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.dataframe(
            quarantine,
            use_container_width=True,
            hide_index=True,
        )

st.divider()

st.subheader("Velocity detection")

if velocity.empty:
    st.info("No velocity alert windows available yet.")
else:
    st.caption(
        "Each row is a qualifying user-window. Overlapping windows "
        "can represent the same burst, not separate incidents."
    )

    st.dataframe(
        velocity,
        use_container_width=True,
        hide_index=True,
    )

    if "transaction_count" in velocity.columns:
        st.metric(
            "Highest transactions in a qualifying window",
            int(velocity["transaction_count"].max()),
        )

st.divider()

st.subheader("Normal transaction sample")

if normal.empty:
    st.info("No normal transaction records available.")
else:
    display_cols = [
        c for c in [
            "transaction_id",
            "user_id",
            "merchant_id",
            "amount",
            "currency",
            "status",
            "event_time",
        ]
        if c in normal.columns
    ]

    st.dataframe(
        normal[display_cols].tail(20),
        use_container_width=True,
        hide_index=True,
    )

st.caption(
    "Data is read from local Parquet files. Refresh to reload stored "
    "results. This dashboard is not a live fraud decisioning system."
)
