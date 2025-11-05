import argparse
import logging
import sqlite3
from pathlib import Path

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_PATH = Path("data/processed/clickstream.db")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("anomaly")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def exec_and_count(conn: sqlite3.Connection, sql: str, params: tuple) -> int:
    """Execute a write statement and return number of affected rows (best-effort)."""
    before = conn.total_changes
    conn.execute(sql, params)
    return conn.total_changes - before


def ensure_db() -> None:
    if not DB_PATH.exists():
        log.error("DB not found at %s. Run ingestion first.", DB_PATH)
        raise SystemExit(1)


def pct_to_per_thousand(rate: float) -> int:
    """Map fraction (0.0~1.0) to 'per-thousand' integer for RANDOM() predicate"""
    if rate <= 0:
        return 0
    v = int(round(rate * 1000))
    return max(0, min(v, 1000))


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
def drop_volume(
        conn: sqlite3.Connection, 
        month: str, 
        keep_ratio: float
        ) -> int:
    """
    Randomly delete rows to simulate a volume drop.
    keep_ratio: fraction (0~1] to keep (e.g., 0.7 keeps ~70%).
    """
    if not (0.0 < keep_ratio <= 1.0):
        raise ValueError("keep_ratio must be in (0, 1].")
    # Delete approximately (1 - keep_ratio) * 100 percent of rows using RANDOM()
    delete_pct = int(round((1 - keep_ratio) * 100))
    log.info("Volume drop: month=%s keep_ratio=%.2f (~delete %d%%)", month, keep_ratio, delete_pct)
    changed = exec_and_count(
        conn,
        """
        DELETE FROM clickstream_raw
         WHERE load_month = ?
           AND ABS(RANDOM()) % 100 < ?
        """,
        (month, delete_pct),
    )
    log.info("Volume drop affected %d rows", changed)
    return changed


def inject_null_like(
        conn: sqlite3.Connection, 
        month: str, 
        rate: float, 
        target_col: str = "prev"
        ) -> int:
    """
    Inject 'null-like' values by setting column to '' (empty string),
    which bypasses NOT NULL constraints but is treated as null in validate.

    rate: fraction (0~1], e.g., 0.003 = 0.3%.
    target_col: one of prev/curr/type.
    """
    if target_col not in {"prev", "curr", "type"}:
        raise ValueError("target_col must be one of: prev, curr, type")
    if not (0.0 < rate <= 1.0):
        raise ValueError("null rate must be in (0, 1].")

    pt = pct_to_per_thousand(rate)
    log.info("Null-like injection: month=%s rate=%.4f target=%s (~%d per-thousand)", month, rate, target_col, pt)
    changed = exec_and_count(
        conn,
        f"""
        UPDATE clickstream_raw
           SET {target_col} = ''
         WHERE load_month = ?
           AND ABS(RANDOM()) % 1000 < ?
        """,
        (month, pt),
    )
    log.info("Null-like injection affected %d rows", changed)
    return changed


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject synthetic anomalies into SQLite for demo/testing.")
    p.add_argument("--month", required=True, help="Target month (YYYY-MM)")
    p.add_argument("--volume_keep", type=float, default=1.0, help="Keep fraction (0~1], e.g., 0.7 keeps ~70% (deletes ~30%)")
    p.add_argument("--null_rate", type=float, default=0.0, help="Fraction (0~1] to set NULL (prev/curr/type)")
    p.add_argument("--null_col", default="prev", choices=["prev", "curr", "type"], help="Column for NULL injection")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ensure_db()

    changed_total = 0
    with sqlite3.connect(DB_PATH) as conn:
        # Volume drop (delete) first so later injections operate on reduced set
        if args.volume_keep < 1.0:
            changed_total += drop_volume(conn, args.month, args.volume_keep)
        if args.null_rate > 0.0:
            changed_total += inject_null_like(conn, args.month, args.null_rate, args.null_col)

        conn.commit()

    if changed_total == 0:
        log.warning("No anomalies were injected (all parameters at default). Nothing changed.")
    else:
        log.info("Anomaly injection complete. Total affected rows (best-effort): %d", changed_total)
        log.info("Re-run validate/sla for month=%s to refresh metrics.", args.month)


if __name__ == "__main__":
    main()
