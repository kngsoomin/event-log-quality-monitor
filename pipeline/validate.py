import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_PATH = Path("data/processed/clickstream.db")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("validate")


# -----------------------------------------------------------------------------
# Demo
# -----------------------------------------------------------------------------
def inject_null_for_demo(df: pd.DataFrame):
    for col in ("prev", "curr", "type"):
        if col in df.columns:
            df[col] = df[col].astype("string")
            df[col] = df[col].str.strip()
            df[col] = df[col].replace("", pd.NA)
    
    return df


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
def read_month(conn: sqlite3.Connection, month: str) -> pd.DataFrame:
    """Read one month's rows from clickstream_raw."""
    q = """
        SELECT prev, curr, type, n
        FROM clickstream_raw
        WHERE load_month = ?
    """
    return pd.read_sql_query(q, conn, params=(month,))


def compute_quality(df: pd.DataFrame) -> dict[str, float | int]:
    """Compute basic data-quality metrics for a given DataFrame."""
    core_cols = ["prev", "curr", "type", "n"]
    cols_ok = all(c in df.columns for c in core_cols)
    schema_valid = 1 if cols_ok else 0

    if not cols_ok or df.empty:
        # Return a minimal payload if no rows
        return {
            "row_count": int(len(df)),
            "null_rate": 1.0 if not cols_ok else float(df[core_cols].isna().mean().mean()),
            "duplicate_rate": 0.0,
            "range_error_rate": 0.0,
            "schema_valid": schema_valid,
        }

    null_rate = float(df[core_cols].isna().mean().mean())
    dup_rate = float(df.duplicated(subset=["prev", "curr", "type"]).mean())
    range_err = float((df["n"] < 0).fillna(False).mean())

    return {
        "row_count": int(len(df)),
        "null_rate": null_rate,
        "duplicate_rate": dup_rate,
        "range_error_rate": range_err,
        "schema_valid": schema_valid,
    }


def upsert_monthly(conn: sqlite3.Connection, month: str, m: dict) -> None:
    """Upsert metrics into dq_monthly (acts as a cached summary)."""
    conn.execute(
        """
        INSERT INTO dq_monthly (
            load_month, row_count, null_rate, duplicate_rate, range_error_rate, schema_valid
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(load_month) DO UPDATE SET
            row_count = excluded.row_count,
            null_rate = excluded.null_rate,
            duplicate_rate = excluded.duplicate_rate,
            range_error_rate = excluded.range_error_rate,
            schema_valid = excluded.schema_valid
        """,
        (
            month,
            m["row_count"],
            m["null_rate"],
            m["duplicate_rate"],
            m["range_error_rate"],
            m["schema_valid"],
        ),
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute DQ metrics for a specific month.")
    p.add_argument("--month", required=True, help="YYYY-MM")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if not DB_PATH.exists():
        log.error("DB not found at %s. Run ingest first.", DB_PATH)
        return

    with sqlite3.connect(DB_PATH) as conn:
        df = read_month(conn, args.month)
        if df.empty:
            log.warning("No rows found for month=%s", args.month)

        df = inject_null_for_demo(df)
        metrics = compute_quality(df)
        upsert_monthly(conn, args.month, metrics)
        conn.commit()

    log.info("Validated month=%s -> %s", args.month, metrics)


if __name__ == "__main__":
    main()
