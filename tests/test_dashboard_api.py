from pathlib import Path

import dashboard.api as api


def test_number_handles_invalid_values():
    assert api.number("12.5") == 12.5
    assert api.number(None) == 0.0
    assert api.number("bad", default=-1) == -1


def test_safe_converts_nested_datetimes_and_nan():
    from datetime import datetime
    import math

    value = api.safe({"created": datetime(2026, 1, 1), "nested": [1, float("nan")]})
    assert value["created"] == "2026-01-01T00:00:00"
    assert value["nested"][0] == 1
    assert value["nested"][1] is None


def test_missing_dataset_returns_empty(monkeypatch, tmp_path):
    monkeypatch.setitem(api.DATASETS, "normal", tmp_path / "does-not-exist")
    assert api.read_dataset("normal") == []


def test_display_threshold_reclassifies_existing_flagged_records(monkeypatch):
    monkeypatch.setattr(api, "threshold", 100000.0)
    def fake_read_dataset(name):
        if name == "normal":
            return [{"transaction_id": "A", "amount": 75000, "status": "SUCCESS", "event_time": "2026-10-27T10:00:00"}]
        if name == "flagged":
            return [{"transaction_id": "B", "amount": 125000, "status": "SUCCESS", "risk_level": "HIGH", "event_time": "2026-10-27T10:01:00"}]
        return []
    monkeypatch.setattr(api, "read_dataset", fake_read_dataset)
    rows = api.transactions()
    by_id = {row["transaction_id"]: row for row in rows}
    assert by_id["A"]["risk_level"] == "NORMAL"
    assert by_id["B"]["risk_level"] == "HIGH"


def test_summary_is_zero_safe_when_datasets_empty(monkeypatch):
    monkeypatch.setattr(api, "read_dataset", lambda name: [])
    monkeypatch.setattr(api, "threshold", 50000.0)
    result = api.summary()
    assert result["total_transactions"] == 0
    assert result["success_rate"] == 0
    assert result["quarantine_count"] == 0
