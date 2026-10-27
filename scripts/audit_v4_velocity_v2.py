from pathlib import Path
import duckdb

root = Path("data/processed/transactions_v4/velocity_flags_v2")
files = sorted(root.rglob("*.parquet"))

if not files:
    print("NO PARQUET OUTPUT FOUND")
    raise SystemExit(1)

con = duckdb.connect()
df = con.from_parquet([str(p) for p in files]).df()

print(f"Parquet files: {len(files)}")
print(f"Total alert rows: {len(df)}")
print("Columns:", ", ".join(df.columns))

if "user_id" in df.columns:
    test_user = "TEST-VELOCITY-V2-USER"
    test = df[df["user_id"] == test_user]

    print(f"\nTest-user alert rows: {len(test)}")
    if not test.empty:
        for col in ["alert_id", "user_id", "window", "count", "batch_id"]:
            if col in test.columns:
                print(f"\n{col}:")
                print(test[col].to_string(index=False))

        if "alert_id" in test.columns:
            print("\nDistinct alert IDs:", test["alert_id"].nunique())

        if "count" in test.columns:
            print("Observed counts:", sorted(test["count"].dropna().unique().tolist()))
    else:
        print("No finalized alert rows for the test user yet.")

print("\nAll alert rows:")
print(df.to_string(index=False, max_rows=30))
