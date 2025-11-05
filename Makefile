MONTH        ?= 2025-09
MONTHS       ?= 2025-06,2025-07,2025-08,2025-09
RANGE_START  ?=
RANGE_END    ?=
LANG         ?= enwiki
CHUNKSIZE    ?= 500000
KEEP         ?= 1.00
NULLRATE     ?= 0.0000

.PHONY: help init fetch fetch_range seed_demo seed_demo_range ingest ingest_all validate sla serve dashboard anomaly test testv

help:
	@echo "Targets:"
	@echo "  init              - Install Python dependencies"
	@echo "  fetch             - Fetch raw dumps for MONTHS ($(MONTHS))"
	@echo "  fetch_range       - Fetch raw dumps for RANGE_START..RANGE_END ($(RANGE_START)~$(RANGE_END))"
	@echo "  seed_demo         - Fetch+Ingest+Validate+SLA for MONTHS"
	@echo "  seed_demo_range   - Fetch+Ingest+Validate+SLA for RANGE_START..RANGE_END"
	@echo "  ingest            - Ingest a specific month (MONTH=$(MONTH), CHUNKSIZE=$(CHUNKSIZE))"
	@echo "  ingest_all        - Ingest all TSVs under data/raw/"
	@echo "  validate          - Compute DQ metrics for MONTH"
	@echo "  sla               - Run SLA checks for MONTH"
	@echo "  serve             - Run FastAPI dev server"
	@echo "  dashboard         - Run Streamlit dashboard"
	@echo "  anomaly           - Inject anomalies for MONTH (KEEP=$(KEEP), NULLRATE=$(NULLRATE))"
	@echo "  test              - Run pytest (if configured)"

init:
	pip install -r requirements.txt

fetch:
	python pipeline/fetch_clickstream.py --months $(MONTHS) --lang $(LANG)

fetch_range:
	python pipeline/fetch_clickstream.py --range $(RANGE_START) $(RANGE_END) --lang $(LANG)

seed_demo: # seeding: fetch + ingest + validate + sla
	python pipeline/seed_demo.py --months $(MONTHS) --lang $(LANG) --chunksize $(CHUNKSIZE)

seed_demo_range:
	python pipeline/seed_demo.py --range $(RANGE_START) $(RANGE_END) --lang $(LANG) --chunksize $(CHUNKSIZE)

ingest:
	python pipeline/ingest.py --month $(MONTH) --chunksize $(CHUNKSIZE)

ingest_all:
	python pipeline/ingest.py --all --chunksize $(CHUNKSIZE)

validate:
	python pipeline/validate.py --month $(MONTH)

sla:
	python pipeline/sla_check.py --month $(MONTH)

serve:
	uvicorn app.api:app --reload

dashboard:
	streamlit run app/dashboard.py

anomaly: # optional
	python pipeline/inject_anomaly.py --month $(MONTH) --volume_keep $(KEEP) --null_rate $(NULLRATE)
	python pipeline/validate.py --month $(MONTH)
	python pipeline/sla_check.py --month $(MONTH)

test:
	pytest -q

testv:
	pytest -vv