"""Event-driven orchestration of the Global Risk Intelligence Lakehouse.

DAG 1  gril_ingestion          (cron)   pulls WHO + World Bank -> landing zone, then publishes the
                                        landing-manifest Dataset (the "event").
DAG 2  gril_lakehouse_pipeline (Dataset) fires whenever a new manifest lands:
          bronze -> silver (MERGE upserts) -> gold -> Great Expectations gate
          -> Snowflake load -> dbt deps -> dbt snapshot (SCD2) -> dbt run -> dbt test

Execution mode (env GRIL_EXECUTION_MODE):
  local       Spark steps run in the Airflow container via `python -m ...` (Docker Compose dev setup)
  databricks  Spark steps run on Databricks as python_wheel_tasks (needs GRIL_DATABRICKS_CLUSTER_ID,
              GRIL_WHEEL_PATH and an Airflow connection `databricks_default`)
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

STORAGE_ROOT = os.getenv("GRIL_STORAGE_ROOT", "/opt/airflow/data").replace("s3a://", "s3://").rstrip("/")
_base = STORAGE_ROOT if "://" in STORAGE_ROOT else f"file://{os.path.abspath(STORAGE_ROOT)}"
LANDING_MANIFESTS = Dataset(f"{_base}/landing/_manifests")

MODE = os.getenv("GRIL_EXECUTION_MODE", "local")
ENABLE_SNOWFLAKE = os.getenv("GRIL_ENABLE_SNOWFLAKE", "true").lower() == "true"
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
DBT_DIR = os.getenv("DBT_DIR", "/opt/airflow/dbt")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}


def spark_step(task_id: str, module: str, entry_point: str):
    """Same code path, two runtimes: local container or Databricks wheel task."""
    if MODE == "databricks":
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        return DatabricksSubmitRunOperator(
            task_id=task_id,
            databricks_conn_id="databricks_default",
            existing_cluster_id=os.environ["GRIL_DATABRICKS_CLUSTER_ID"],
            python_wheel_task={"package_name": "gril", "entry_point": entry_point},
            libraries=[{"whl": os.environ["GRIL_WHEEL_PATH"]}],
        )
    return BashOperator(task_id=task_id, bash_command=f"python -m {module}")


def dbt_step(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} {command} --profiles-dir . --target ${{DBT_TARGET:-dev}}",
    )


with DAG(
    dag_id="gril_ingestion",
    description="WHO GHO + World Bank -> landing zone (publishes the manifest Dataset)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gril", "ingestion"],
) as ingestion_dag:
    BashOperator(
        task_id="ingest_sources_to_landing",
        bash_command="python -m gril.ingestion.run_ingestion",
        outlets=[LANDING_MANIFESTS],
    )


with DAG(
    dag_id="gril_lakehouse_pipeline",
    description="Event-driven Bronze -> Silver -> Gold -> quality gate -> Snowflake -> dbt",
    schedule=[LANDING_MANIFESTS],
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gril", "lakehouse", "dbt"],
) as pipeline_dag:
    bronze = spark_step("bronze_ingest", "gril.bronze.ingest_bronze", "gril-bronze")
    silver = spark_step("silver_merge_upserts", "gril.silver.transform_silver", "gril-silver")
    gold = spark_step("gold_star_schema_and_risk", "gril.gold.build_gold", "gril-gold")
    quality = spark_step("great_expectations_gate", "gril.quality.ge_checks", "gril-quality")

    bronze >> silver >> gold >> quality

    if ENABLE_SNOWFLAKE:
        load = spark_step("load_snowflake_raw", "gril.warehouse.load_snowflake", "gril-snowflake-load")
        deps = dbt_step("dbt_deps", "deps")
        snapshot = dbt_step("dbt_snapshot_scd2", "snapshot")
        run = dbt_step("dbt_run", "run")
        test = dbt_step("dbt_test", "test")
        quality >> load >> deps >> snapshot >> run >> test
