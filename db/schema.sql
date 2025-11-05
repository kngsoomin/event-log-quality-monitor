-- pragma for better write performance
PRAGMA journal_mode=WAL;
PRAGMA synchronous = NORMAL;

-- raw table (official Wikipedia Clickstream schema + month tag)
CREATE TABLE IF NOT EXISTS clickstream_raw (
  prev TEXT NOT NULL,
  curr TEXT NOT NULL,
  type TEXT NOT NULL,
  n    INTEGER NOT NULL,
  load_month TEXT NOT NULL
);

-- monthly data-quality cache
CREATE TABLE IF NOT EXISTS dq_monthly (
  load_month TEXT PRIMARY KEY,
  row_count INTEGER,
  null_rate REAL,
  duplicate_rate REAL,
  range_error_rate REAL,
  schema_valid INTEGER
);

-- ingestion audit
CREATE TABLE IF NOT EXISTS ingest_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  load_month TEXT,
  source_file TEXT,
  inserted_rows INTEGER,
  skipped_lines INTEGER,
  started_at TEXT,
  ended_at TEXT,
  status TEXT
);

-- indexes
CREATE INDEX IF NOT EXISTS idx_clickstream_raw_month
ON clickstream_raw(load_month);
