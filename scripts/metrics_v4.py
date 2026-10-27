
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "data"

DATASETS = {
    "normal": BASE / "processed" / "transactions_v4" / "normal",
    "high_value": BASE / "processed" / "transactions_v4" / "flagged",
    "velocity": BASE / "processed" / "transactions_v4" / "velocity_flags",
    "quarantine": BASE / "quarantine" / "transactions_v4",
}

con = duckdb.connect()


def parquet_glob(folder):
    if not folder.exists():
        return None

    files = list(folder.glob("**/*.parquet"))
    if not files:
        return None

    return (folder / "**" / "*.parquet").as_posix().replace("'", "''")


def count_rows(folder):
    pattern = parquet_glob(folder)
    if not pattern:
        return 0

    return con.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{pattern}', union_by_name=true)
        """
    ).fetchone()[0]


print("=" * 60)
print("UPI TRANSACTION MONITORING — V4 OPERATIONAL METRICS")
print("=" * 60)

normal_count = count_rows(DATASETS["normal"])
high_value_count = count_rows(DATASETS["high_value"])
quarantine_count = count_rows(DATASETS["quarantine"])
velocity_windows = count_rows(DATASETS["velocity"])

print(f"Normal transaction records : {normal_count}")
print(f"High-value flagged records : {high_value_count}")
print(f"Quarantined records        : {quarantine_count}")
print(f"Velocity alert windows     : {velocity_windows}")

pattern = parquet_glob(DATASETS["high_value"])

if pattern:
    print("\nHIGH-VALUE ALERT SUMMARY")
    print("-" * 60)

    summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS flagged_records,
            ROUND(SUM(amount), 2) AS flagged_amount_total,
            ROUND(AVG(amount), 2) AS average_flagged_amount,
            ROUND(MAX(amount), 2) AS largest_flagged_amount
        FROM read_parquet('{pattern}', union_by_name=true)
        """
    ).fetchdf()

    print(summary.to_string(index=False))

pattern = parquet_glob(DATASETS["quarantine"])

if pattern:
    print("\nQUARANTINE BREAKDOWN")
    print("-" * 60)

    quarantine_summary = con.execute(
        f"""
        SELECT validation_error, COUNT(*) AS records
        FROM read_parquet('{pattern}', union_by_name=true)
        GROUP BY validation_error
        ORDER BY records DESC
        """
    ).fetchdf()

    print(quarantine_summary.to_string(index=False))

pattern = parquet_glob(DATASETS["velocity"])

if pattern:
    print("\nVELOCITY ALERT WINDOWS")
    print("-" * 60)

    velocity_summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS qualifying_window_records,
            COUNT(DISTINCT user_id) AS distinct_users,
            MAX("count") AS highest_window_transaction_count
        FROM read_parquet('{pattern}', union_by_name=true)
        """
    ).fetchdf()

    print(velocity_summary.to_string(index=False))

con.close()
