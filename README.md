# UPI Transaction Monitoring & Analytics Pipeline

A local, real-time data engineering project that simulates Indian UPI transactions, streams events through Apache Kafka, processes them with PySpark Structured Streaming, detects suspicious patterns, and publishes analytics-ready outputs.

## Architecture

Python Transaction Generator → Apache Kafka → PySpark Structured Streaming → Parquet Data Lake → DuckDB SQL Analytics → Streamlit Dashboard

## Key Features

- **Event ingestion:** Synthetic UPI transaction events published to Kafka.
- **Stream processing:** PySpark Structured Streaming reads and processes transaction events.
- **Data quality:** Validates transaction fields and routes invalid records to quarantine.
- **Deduplication:** Uses transaction IDs and event-time watermarks to suppress duplicate events within the configured watermark.
- **High-value detection:** Flags transactions that meet the configurable high-value threshold.
- **Velocity monitoring:** Identifies users with repeated transactions within sliding event-time windows.
- **Analytics:** Stores results as Parquet and queries them using DuckDB.
- **Automated tests:** Pytest checks transaction classification, quarantine behavior, and velocity-alert outputs.

## Technology Stack

Python, Apache Kafka, PySpark Structured Streaming, Parquet, DuckDB, Streamlit, SQL, pytest.

## Project Status

The current local pipeline includes normal transaction outputs, high-value alerts, invalid-record quarantine, and velocity monitoring. Six regression tests currently pass against the generated outputs.

The pipeline is under active development. Recovery behavior, end-to-end streaming validation, and automated continuous integration remain areas for further verification.

## Running Tests

Activate the project virtual environment, then run:

```powershell
python -m pytest tests/test_pipeline_outputs.py -v
```

## Data and Configuration

Generated Parquet outputs, checkpoints, raw local data, virtual environments, and temporary files are excluded from version control. The source code and test suite are intended to be committed.

## Disclaimer

This project uses synthetic transaction data for educational and demonstration purposes. It is not connected to a bank, UPI payment provider, or production fraud-detection system.

## Live HTML dashboard

The repository now includes a standalone HTML/CSS/JavaScript dashboard backed by a local Python + DuckDB API. It reads the V4 Parquet outputs rather than generating fake transactions.

### Start it

Activate the existing project virtual environment from the repository root, then run:

    python dashboard/api.py

Open http://127.0.0.1:8765.

The UI includes transaction KPIs, status/value charts, search and filters, CSV export, a risk view, velocity alerts, quarantine inspection, theme switching, and configurable display threshold/refresh interval.

API endpoints: /api/health, /api/summary, /api/transactions, /api/alerts, /api/velocity, /api/quarantine, /api/settings.

**Threshold distinction:** changing the dashboard display threshold does not change Spark's processing configuration or rewrite existing Parquet. To change processing, set UPI_HIGH_VALUE_THRESHOLD before starting Spark. Velocity windows overlap and should not be interpreted as unique fraud incidents without further correlation.

The API binds to localhost by default and has no authentication; do not expose it publicly as-is. Chart.js loads from a CDN and requires internet access. This remains an educational synthetic-data project, not a bank-connected or production fraud decision system.

### Dashboard API tests

    python -m pip install duckdb pytest
    python -m pytest tests/test_dashboard_api.py -v
