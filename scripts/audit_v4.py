
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
FLAGGED = (
    ROOT / "data" / "processed" / "transactions_v4" / "flagged"
)
NORMAL = (
    ROOT / "data" / "processed" / "transactions_v4" / "normal"
)
QUARANTINE = (
    ROOT / "data" / "quarantine" / "transactions_v4"
)

con = duckdb.connect()


def inspect(label, folder):
    files = list(folder.glob("**/*.parquet"))

    print(f"\n{label}")
    print("-" * 55)

    if not files:
        print("No Parquet files found.")
        return

    pattern = (folder / "**" / "*.parquet").as_posix()
    safe_pattern = pattern.replace("'", "''")

    print(
        con.execute(
            f"""
            SELECT COUNT(*) AS total_rows
            FROM read_parquet('{safe_pattern}', union_by_name=true)
            """
        ).fetchdf().to_string(index=False)
    )

    if folder == FLAGGED:
        print("\nFlagged transactions:")
        print(
            con.execute(
                f"""
                SELECT transaction_id, user_id, amount,
                       risk_reason, risk_level, risk_threshold
                FROM read_parquet(
                    '{safe_pattern}', union_by_name=true
                )
                ORDER BY amount DESC
                """
            ).fetchdf().to_string(index=False)
        )

    elif folder == QUARANTINE:
        print("\nQuarantine reasons:")
        print(
            con.execute(
                f"""
                SELECT validation_error, COUNT(*) AS records
                FROM read_parquet(
                    '{safe_pattern}', union_by_name=true
                )
                GROUP BY validation_error
                """
            ).fetchdf().to_string(index=False)
        )


inspect("V4 NORMAL TRANSACTIONS", NORMAL)
inspect("V4 FLAGGED TRANSACTIONS", FLAGGED)
inspect("V4 QUARANTINE", QUARANTINE)

con.close()
