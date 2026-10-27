
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]

DATASETS = {
    "V3 PROCESSED TRANSACTIONS": (
        ROOT / "data" / "processed" / "transactions_v3"
    ),
    "V3 QUARANTINED TRANSACTIONS": (
        ROOT / "data" / "quarantine" / "transactions_v3"
    ),
}

con = duckdb.connect()


def audit(label, folder):
    files = list(folder.glob("**/*.parquet"))

    print(f"\n{label}")
    print("-" * 55)

    if not files:
        print("No Parquet files found.")
        return

    pattern = (folder / "**" / "*.parquet").as_posix()
    safe_pattern = pattern.replace("'", "''")

    # Discover the real schema instead of assuming all columns exist.
    schema = con.execute(
        f"""
        DESCRIBE
        SELECT * FROM read_parquet(
            '{safe_pattern}', union_by_name = true
        )
        """
    ).fetchall()

    columns = {row[0] for row in schema}

    print("Columns:", ", ".join(sorted(columns)))

    expressions = ["COUNT(*) AS total_rows"]

    if "transaction_id" in columns:
        expressions.extend([
            "COUNT(transaction_id) AS non_null_ids",
            "COUNT(DISTINCT transaction_id) AS distinct_ids",
        ])

    if "amount" in columns:
        expressions.append(
            """
            SUM(
                CASE WHEN amount IS NULL OR amount <= 0
                     THEN 1 ELSE 0 END
            ) AS invalid_amounts
            """
        )

    query = f"""
        SELECT {", ".join(expressions)}
        FROM read_parquet('{safe_pattern}', union_by_name = true)
    """

    print(con.execute(query).fetchdf().to_string(index=False))
    print(f"Parquet files: {len(files)}")

    if "validation_error" in columns:
        print("\nValidation errors:")
        errors = con.execute(
            f"""
            SELECT validation_error, COUNT(*) AS records
            FROM read_parquet(
                '{safe_pattern}', union_by_name = true
            )
            GROUP BY validation_error
            ORDER BY records DESC
            """
        )
        print(errors.fetchdf().to_string(index=False))

    if "transaction_id" not in columns:
        print(
            "WARNING: transaction_id is missing from this dataset. "
            "We should inspect the streaming code to preserve the "
            "original transaction fields in quarantine."
        )


for label, folder in DATASETS.items():
    audit(label, folder)

con.close()
