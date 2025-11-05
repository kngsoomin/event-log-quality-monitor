import sqlite3
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from app.api import app, DB_PATH

client = TestClient(app)

def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"

@pytest.mark.skipif(not DB_PATH.exists(), reason="DB not found; run make ingest/validate first")
def test_metrics_one_month():
    # pick any month present in dq_monthly
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT load_month FROM dq_monthly ORDER BY load_month DESC LIMIT 1").fetchone()
    assert row, "dq_monthly empty"
    month = row[0]

    r = client.get("/metrics", params={"month": month})
    assert r.status_code == 200
    body = r.json()
    assert body["load_month"] == month
    for key in ["row_count", "null_rate", "duplicate_rate", "range_error_rate", "schema_valid"]:
        assert key in body
