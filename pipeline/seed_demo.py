import argparse
import logging
import subprocess
from datetime import datetime
from typing import Iterable, List

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
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


def run(cmd: List[str]) -> None:
    log.info("RUN: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def expand_months(args: argparse.Namespace) -> List[str]:
    """Normalize months input from --months or --range into a list."""
    if args.months:
        months = [s.strip() for s in args.months.split(",") if s.strip()]
        if not months:
            raise SystemExit("No valid months in --months.")
        return months
    if args.range:
        start, end = args.range
        return list(iter_months_inclusive(start, end))
    # should not happen due to mutually exclusive group
    raise SystemExit("No months provided.")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed demo data: fetch → ingest → validate → sla.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--months", help="Comma-separated months, e.g. 2025-06,2025-07,2025-08,2025-09")
    g.add_argument("--range", nargs=2, metavar=("START", "END"), help="Inclusive month range (YYYY-MM YYYY-MM)")
    p.add_argument("--lang", default="enwiki", help="Project code (default: enwiki)")
    p.add_argument("--chunksize", type=int, default=500000, help="Chunk size for ingest (default: 500000)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    months = expand_months(args)

    # 1) Fetch raw dumps (tsv.gz -> tsv)
    run(["python", "pipeline/fetch_clickstream.py", "--months", ",".join(months), "--lang", args.lang])

    # 2) Ingest + Validate + SLA for each month
    for m in months:
        run(["python", "pipeline/ingest.py", "--month", m, "--chunksize", str(args.chunksize)])
        run(["python", "pipeline/validate.py", "--month", m])
        run(["python", "pipeline/sla_check.py", "--month", m])

    log.info("Seed demo completed for months: %s", ",".join(months))


if __name__ == "__main__":
    main()
