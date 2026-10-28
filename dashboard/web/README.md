UPI Sentinel — Live HTML Dashboard

Run from the repository root with the project virtual environment active:

    python dashboard/api.py

Open http://127.0.0.1:8765. The dashboard uses the Python standard library HTTP server and DuckDB to read local Parquet output directories:

- data/processed/transactions_v4/normal
- data/processed/transactions_v4/flagged
- data/processed/transactions_v4/velocity_flags_v2
- data/quarantine/transactions_v4

API endpoints: GET /api/health, /api/summary, /api/transactions, /api/alerts, /api/velocity, /api/quarantine, /api/settings; POST /api/settings updates the runtime display threshold.

Semantics and safety:
- This reads stored Parquet and does not consume Kafka directly. Refresh re-reads the files written by Spark.
- The display threshold is not the same as the Spark processing threshold. Set UPI_HIGH_VALUE_THRESHOLD before starting Spark to configure processing. Historical files are not rewritten automatically.
- Velocity alert windows overlap and do not necessarily equal distinct incidents.
- Synthetic educational data only; not connected to a bank, NPCI, or production fraud detection system.
- The server binds to 127.0.0.1 by default and has no authentication. Do not expose it publicly as-is.
- Chart.js is loaded from a CDN and requires internet access.
