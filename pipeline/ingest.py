import argparse
import csv
import datetime as dt
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
DB_PATH = Path("data/processed/clickstream.db")
RAW_DIR = Path("data/raw")
SCHEMA_PATH = Path("db/schema.sql")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ingest")

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def detect_month_from_filename(p: Path) -> str:
    """
    Extract YYYY-MM from a filename like: clickstream-enwiki-2025-09.tsv
    Returns "unknown" if the pattern cannot be determined.
    """
    parts = p.stem.split("-")
    for i in range(len(parts) - 2):
        if parts[i + 1].isdigit() and len(parts[i + 1]) == 4:
            return f"{parts[i + 1]}-{parts[i + 2]}"
    return "unknown"


def apply_schema(conn: sqlite3.Connection) -> None:
    """Execute schema.sql to ensure tables and indexes exist."""
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(sql)


def clean_chunk(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """
    Normalize dtypes, drop bad rows, and attach the month.
    Keeps only valid types and non-negative counts.
    """
    # Normalize dtypes
    df["prev"] = df["prev"].astype("string")
    df["curr"] = df["curr"].astype("string")
    df["type"] = df["type"].astype("string")
    df["n"] = pd.to_numeric(df["n"], errors="coerce")

    # Basic cleaning
    df = df.dropna(subset=["prev", "curr", "type", "n"])
    df = df[df["n"] >= 0]
    df = df[df["type"].isin(["link", "external", "other"])]

    # Finalize types
    df["n"] = df["n"].astype("Int64")
    df["load_month"] = month
    return df


def read_tsv(
    path: Path,
    chunksize: Optional[int] = None,
) -> Iterable[pd.DataFrame]:
    """
    Yield DataFrames from a TSV file. Uses a tolerant parser and only the first 4 columns.
    """
    read_kwargs = dict(
        sep="\t",
        header=None,
        usecols=[0, 1, 2, 3],  
            # source: https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream
            # official schema: prev, curr, type, n 
        names=["prev", "curr", "type", "n"],
        engine="python",
        quoting=csv.QUOTE_NONE,
        on_bad_lines="skip",  # skip malformed lines
    )

    if chunksize:
        yield from pd.read_csv(path, chunksize=chunksize, **read_kwargs)
    else:
        yield pd.read_csv(path, **read_kwargs)


# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
def load_tsv_to_sqlite(tsv_path: Path, chunksize: Optional[int] = None) -> None:
    """
    Ingest one TSV into SQLite in an idempotent way (per month).
    Also logs a lightweight ingest audit row.
    """
    month = detect_month_from_filename(tsv_path)
    log.info("Loading %s (month=%s)", tsv_path.name, month)

    started_at = dt.datetime.utcnow().isoformat()
    inserted_total = 0
    skipped_total = 0
    status = "SUCCESS"

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sqlite3.connect(DB_PATH) as conn:
            apply_schema(conn)

            # Idempotency: replace existing month
            conn.execute("DELETE FROM clickstream_raw WHERE load_month = ?", (month,))

            for raw_chunk in read_tsv(tsv_path, chunksize=chunksize):
                before = len(raw_chunk)
                cleaned = clean_chunk(raw_chunk, month)
                kept = len(cleaned)
                skipped = before - kept

                if kept:
                    cleaned.to_sql("clickstream_raw", conn, if_exists="append", index=False)

                inserted_total += kept
                skipped_total += max(skipped, 0)

    except Exception as exc:  # noqa: BLE001
        status = "FAILED"
        log.exception("Ingestion failed for %s: %s", tsv_path.name, exc)

    finally:
        ended_at = dt.datetime.utcnow().isoformat()
        # Write a single audit row
        with sqlite3.connect(DB_PATH) as conn2:
            try:
                conn2.execute(
                    """
                    INSERT INTO ingest_audit (
                        load_month, source_file, inserted_rows, skipped_lines,
                        started_at, ended_at, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        month,
                        tsv_path.name,
                        inserted_total,
                        skipped_total,
                        started_at,
                        ended_at,
                        status,
                    ),
                )
                conn2.commit()
            except sqlite3.Error:
                # Do not crash the job just because audit logging failed.
                log.warning("Failed to write ingest audit for %s", tsv_path.name)

        log.info(
            "Done. month=%s rows_inserted=%s skipped=%s status=%s",
            month,
            inserted_total,
            skipped_total,
            status,
        )


def run_for_month(month: str, chunksize: Optional[int]) -> None:
    files = [p for p in RAW_DIR.glob("*.tsv") if detect_month_from_filename(p) == month]
    if not files:
        log.warning("No TSV found for month=%s under %s", month, RAW_DIR)
        return
    for f in sorted(files):
        load_tsv_to_sqlite(f, chunksize=chunksize)


def run_for_all(chunksize: Optional[int]) -> None:
    files = sorted(RAW_DIR.glob("*.tsv"))
    if not files:
        log.warning("No TSV files found under %s", RAW_DIR)
        return
    for f in files:
        load_tsv_to_sqlite(f, chunksize=chunksize)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Wikipedia Clickstream TSVs into SQLite.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="Load only a specific month (YYYY-MM)")
    group.add_argument("--all", action="store_true", help="Load all TSV files under data/raw/")
    parser.add_argument("--chunksize", type=int, default=None, help="Chunk size for large files (e.g., 500000)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.all:
        run_for_all(args.chunksize)
    else:
        run_for_month(args.month, args.chunksize)


if __name__ == "__main__":
    main()
