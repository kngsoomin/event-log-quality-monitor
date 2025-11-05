import argparse, gzip, shutil, time, logging
from datetime import datetime
from pathlib import Path
from typing import Iterable

import requests

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
RAW_DIR = Path("data/raw")
BASE = "https://dumps.wikimedia.org/other/clickstream"
UA = "Mozilla/5.0 (compatible; ClickstreamMonitor/1.0; +https://github.com/kngsoomin)"


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
def month_parts(ym: str) -> tuple[int, int]:
    y, m = ym.split("-")
    return int(y), int(m)


def url_for(month: str, lang: str = "enwiki") -> str:
    y, m = month_parts(month)
    return f"{BASE}/{y}-{m:02d}/clickstream-{lang}-{y}-{m:02d}.tsv.gz"


def download(url: str, dst_gz: Path, retries: int = 3, timeout: int = 30) -> None:
    dst_gz.parent.mkdir(parents=True, exist_ok=True)
    if dst_gz.exists():
        log.info("Exists: %s (skip)", dst_gz.name)
        return
    headers = {"User-Agent": UA}
    for i in range(retries):
        try:
            log.info("GET %s", url)
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(dst_gz, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            log.info("Saved: %s", dst_gz)
            return
        except Exception as e:
            log.warning("Retry %d/%d: %s", i + 1, retries, e)
            time.sleep(2 + i)
    raise RuntimeError(f"Failed to fetch {url}")


def gunzip(src_gz: Path, dst_tsv: Path) -> None:
    if dst_tsv.exists():
        log.info("Exists: %s (skip)", dst_tsv.name)
        return
    log.info("Decompress: %s -> %s", src_gz.name, dst_tsv.name)
    with gzip.open(src_gz, "rb") as f_in, open(dst_tsv, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def fetch_month(month: str, lang: str = "enwiki") -> None:
    u = url_for(month, lang)
    gz = RAW_DIR / f"clickstream-{lang}-{month}.tsv.gz"
    tsv = RAW_DIR / f"clickstream-{lang}-{month}.tsv"
    download(u, gz)
    gunzip(gz, tsv)


def iter_months_inclusive(start: str, end: str) -> Iterable[str]:
    """Yield YYYY-MM from start to end inclusive."""
    sd = datetime.strptime(start, "%Y-%m")
    ed = datetime.strptime(end, "%Y-%m")
    if sd > ed:
        sd, ed = ed, sd
    y, m = sd.year, sd.month
    while (y, m) <= (ed.year, ed.month):
        yield f"{y:04d}-{m:02d}"
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1

# -----------------------------------------------------------------------------
# Core
# -----------------------------------------------------------------------------
def run_for_month(month: str, lang: str) -> None:
    log.info("Fetch month=%s lang=%s", month, lang)
    fetch_month(month, lang)


def run_for_months(months_csv: str, lang: str) -> None:
    months = [s.strip() for s in months_csv.split(",") if s.strip()]
    if not months:
        log.error("No valid months in --months.")
        return
    for m in months:
        run_for_month(m, lang)


def run_for_range(start: str, end: str, lang: str) -> None:
    for m in iter_months_inclusive(start, end):
        run_for_month(m, lang)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and unzip Wikimedia Clickstream dumps.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--month", help="Fetch a single month (YYYY-MM)")
    group.add_argument("--months", help="Fetch a comma-separated list of months, e.g. 2025-06,2025-07")
    group.add_argument("--range", nargs=2, metavar=("START", "END"), help="Fetch inclusive range: START END (YYYY-MM YYYY-MM)")
    parser.add_argument("--lang", default="enwiki", help="Project code (default: enwiki)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.month:
        run_for_month(args.month, args.lang)
    elif args.months:
        run_for_months(args.months, args.lang)
    elif args.range:
        start, end = args.range
        run_for_range(start, end, args.lang)
    else:
        log.error("No action specified.")

if __name__ == "__main__":
    main()
