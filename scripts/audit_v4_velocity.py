
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
FOLDER = (
    ROOT / "data" / "processed" / "transactions_v4" / "velocity_flags"
)

files = list(FOLDER.glob("**/*.parquet"))

if not files:
    print("No velocity flags found yet.")
    print("Check that V4 is running and the event-time watermark advanced.")
    raise SystemExit(0)

pattern = (FOLDER / "**" / "*.parquet").as_posix().replace("'", "''")

con = duckdb.connect()

print("V4 VELOCITY DETECTION RESULTS")
print("-" * 55)

print(
    con.execute(
        f"""
        SELECT
            user_id,
            "window".start AS window_start,
            "window"."end" AS window_end,
            "count" AS transaction_count,
            batch_id
        FROM read_parquet('{pattern}', union_by_name=true)
        WHERE user_id = 'TEST-VELOCITY-USER-001'
        ORDER BY window_start, batch_id
        """
    ).fetchdf().to_string(index=False)
)

con.close()
