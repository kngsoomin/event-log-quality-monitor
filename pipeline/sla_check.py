import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_PATH = Path("data/processed/clickstream.db")
RAW_DIR = Path("data/raw")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("sla")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def file_present(month: str) -> bool:
    """Check if a raw TSV for the target month exists under data/raw/."""
    return any(month in p.name for p in RAW_DIR.glob("*.tsv"))


def month_rows(conn: sqlite3.Connection, month: str) -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM clickstream_raw WHERE load_month = ?",
        (month,),
    )
    return int(cur.fetchone()[0])


def prev_month(month: str) -> str:
    """Return previous month as YYYY-MM."""
    d = datetime.strptime(month, "%Y-%m")
    year = d.year - (1 if d.month == 1 else 0)
    mon = 12 if d.month == 1 else d.month - 1
    return f"{year:04d}-{mon:02d}"


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SLA checks for a specific month.")
    p.add_argument("--month", required=True, help="YYYY-MM")
    p.add_argument("--min_rows", type=int, default=1000, help="Minimum expected row count")
    p.add_argument(
        "--drop_threshold",
        type=float,
        default=0.20,
        help="Warn if volume drops by more than this fraction vs previous month (e.g., 0.2=20%)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Arrival SLA: raw file existence
    if not file_present(args.month):
        log.warning("Raw file for %s not found under %s", args.month, RAW_DIR)
        return

    if not DB_PATH.exists():
        log.error("DB not found at %s. Run ingest first.", DB_PATH)
        return

    with sqlite3.connect(DB_PATH) as conn:
        # Ingest SLA: minimum row count
        rows = month_rows(conn, args.month)
        if rows < args.min_rows:
            log.warning("Row count below minimum: month=%s rows=%s < min_rows=%s", args.month, rows, args.min_rows)
        else:
            log.info("Row count OK: month=%s rows=%s", args.month, rows)

        # Volume drop vs previous month
        pm = prev_month(args.month)
        prev_rows = month_rows(conn, pm)
        if prev_rows > 0:
            drop = 1 - (rows / prev_rows)
            if drop > args.drop_threshold:
                log.warning(
                    "Volume drop vs %s: %.1f%% (rows %s vs %s)",
                    pm,
                    drop * 100,
                    rows,
                    prev_rows,
                )
            else:
                log.info("Volume stable vs %s (rows %s vs %s)", pm, rows, prev_rows)
        else:
            log.info("No data for previous month %s (skipping drop check)", pm)


if __name__ == "__main__":
    main()
