import sqlite3
from pathlib import Path
import pytest

DB = Path("data/processed/clickstream.db")

@pytest.mark.skipif(not DB.exists(), reason="DB not found; run make ingest/validate first")
def test_tables_exist():
    with sqlite3.connect(DB) as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = {r[0] for r in cur.fetchall()}
    assert "clickstream_raw" in names
    assert "dq_monthly" in names
    assert "ingest_audit" in names

@pytest.mark.skipif(not DB.exists(), reason="DB not found; run make ingest/validate first")
def test_month_metrics_present():
    with sqlite3.connect(DB) as conn:
        row = conn.execute("SELECT COUNT(*) FROM dq_monthly").fetchone()
    assert row[0] >= 1
