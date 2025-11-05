import sqlite3
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_PATH = Path("data/processed/clickstream.db")

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="Clickstream DQ & SLA API", version="0.1.0")


def row_to_dict(cur: sqlite3.Cursor, row: sqlite3.Row) -> Dict[str, Any]:
    # As SQLite returns the query result as a tuple
    return {col[0]: row[idx] for idx, col in enumerate(cur.description)}


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise HTTPException(status_code=500, detail="DB not found. Run ingestion first.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # enabling metadata (mapping btw colname to val)
    return conn


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def get_metrics(month: str = Query(..., pattern=r"^\d{4}-\d{2}$")) -> Dict[str, Any]:
    """Return data-quality metrics for a specific month from dq_monthly."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT load_month, row_count, null_rate, duplicate_rate, range_error_rate, schema_valid
            FROM dq_monthly
            WHERE load_month = ?
            """,
            (month,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"No metrics for month={month}")
        return row_to_dict(cur, row)


@app.get("/trend")
def get_trend(limit: int = Query(6, ge=1, le=60)) -> List[Dict[str, Any]]:
    """Return recent N months of dq_monthly, newest first."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT load_month, row_count, null_rate, duplicate_rate, range_error_rate, schema_valid
            FROM dq_monthly
            ORDER BY load_month DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]


@app.get("/audit")
def get_audit(limit: int = Query(20, ge=1, le=200)) -> List[Dict[str, Any]]:
    """Return latest ingestion audit entries, newest first."""
    with get_conn() as conn:
        cur = conn.execute(
            """
            SELECT id, load_month, source_file, inserted_rows, skipped_lines,
                   started_at, ended_at, status
            FROM ingest_audit
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [row_to_dict(cur, r) for r in rows]
