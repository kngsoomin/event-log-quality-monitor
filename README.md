# Event Log Quality Monitor (Wikipedia Clickstream)

> **A hands-on prototype for end-to-end data quality and SLA monitoring** </br>
> Stack: FastAPI · SQLite · Pandas · Streamlit\_

## Demo

[placeholder for video]()

---

## Objective

This project is an **end-to-end prototype** that continuously **checks, validates, and visualizes** the health of Wikipedia's Clickstream logs.

It was my way to think through, design, and solve the kinds of challenges engineers face when building a data pipeline from scratch —
from ingestion stability and idempotent ETL design to data validation and SLA monitoring.

Through this, I wanted to explore what it really means to build **a trustworthy and observable data system** from the ground up —
not just moving data, but ensuring its quality, reliability, and explainability.

---

## The Dataset

Wikipedia Clickstream tracks how users move between pages — essentially an event log of navigation patterns.
| Column | Description |
| ----------- |------------- |
| `prev` | Referrer page title |
| `curr` | Current page title |
| `type` | Type of link (`link`, `external`, or `other`) |
| `n` | Number of times users moved from `prev` to `curr`|

Each monthly file (e.g., clickstream-enwiki-2025-09.tsv) contains 30M+ rows,
large enough to surface real-world challenges such as slow ingestion, schema drift, and consistency management. [Learn more about the dataset](https://meta.wikimedia.org/wiki/Research:Wikipedia_clickstream).

---

## System Flow

```mermaid
flowchart;
    A[1️⃣ Fetch Clickstream TSV] --> B[2️⃣ Ingest → SQLite];
    B --> C[3️⃣ Validate → dq_monthly summary];
    C --> D[4️⃣ SLA Check → Logs & Metrics];
    D --> E[5️⃣ Streamlit Dashboard];
```

**1. Fetch**: Download monthly Clickstream TSVs automatically.

**2. Ingest**: Load and clean data in chunks (500k rows per batch), ensuring idempotent insertion by deleting old data for the same month.

**3. Validate**: Compute data-quality metrics (null rate, duplicate rate, range errors, schema validity) and store them in `dq_monthly`.

**4. SLA Check**: Compare metrics month-over-month and log warnings when thresholds (e.g., >30% volume drop) are breached.

**5. Dashboard**: Visualize trends and SLA compliance with Streamlit.

---

## Installation & Quick Run

```bash
# Install dependencies
make init

# Fetch, ingest, validate, and check SLA for demo months
make seed_demo MONTHS=2025-08,2025-09 LANG=enwiki

# Run FastAPI metrics server
make serve

# Open Streamlit dashboard
make dashboard
```

---

## Key Design Decisions

**Efficient Ingestion**

- **Chunked reads** keep memory usage stable on 30M+ rows.
- **B-tree index** on `load_month` speeds up time-partitioned queries.
- **WAL mode + NORMAL sync** improve write throughput safely.

**Idempotent Pipeline**

- Each run starts by deleting data for that month → safe re-runs, no duplication.
- Skipped or malformed lines are recorded in an `ingest_audit` table for traceability.
- `validate` and `sla_check` are fully re-runnable — results are deterministic.

**Data Quality Metrics**

- Calculated per month and stored in `dq_monthly`:
  - `null_rate` (empty-string and NaN handling)
  - `duplicate_rate` (based on (prev, curr, type) uniqueness)
  - `range_error_rate` (negative or invalid n values)
- Serves as a lightweight data observability layer.

**SLA Monitoring**

- SLA checks compare **volume, null rate, and schema validity** with previous month’s results.
- Any deviation beyond thresholds is **logged as a warning**.
- Streamlit dashboard highlights changes and trends visually.

**Observability & Transparency**

- Unified structured logging across all pipeline stages.
- FastAPI `/metrics` endpoint exposes current and historical DQ summaries.
- Streamlit caching avoids redundant re-computation on repeated views.

---

## Folder Structure

```bash
event-log-quality-monitor/
├── pipeline/
│   ├── fetch_clickstream.py
│   ├── ingest.py
│   ├── validate.py
│   ├── sla_check.py
│   ├── seed_demo.py
│   └── inject_anomaly.py
├── app/
│   ├── api.py
│   └── dashboard.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── demo/
├── tests/
├── db/
└── Makefile

```
