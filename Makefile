.PHONY: install lint test ingest bronze silver gold quality snowflake pipeline dbt-deps dbt-build up down wheel

install:
	pip install -e ".[all]"

lint:
	ruff check src tests airflow

test:
	pytest -q

# ---- local end-to-end run (Spark + Delta on ./data) -------------------------------
ingest:
	python -m gril.ingestion.run_ingestion

bronze:
	python -m gril.bronze.ingest_bronze

silver:
	python -m gril.silver.transform_silver

gold:
	python -m gril.gold.build_gold

quality:
	python -m gril.quality.ge_checks

snowflake:
	python -m gril.warehouse.load_snowflake

pipeline: ingest bronze silver gold quality

# ---- dbt (needs SNOWFLAKE_* env vars) ----------------------------------------------
dbt-deps:
	cd dbt && dbt deps --profiles-dir .

dbt-build: dbt-deps
	cd dbt && dbt snapshot --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .

# ---- Docker / Airflow --------------------------------------------------------------
up:
	cp -n .env.example .env || true
	docker compose up --build -d

down:
	docker compose down

# ---- Databricks wheel --------------------------------------------------------------
wheel:
	pip wheel . --no-deps -w dist/
