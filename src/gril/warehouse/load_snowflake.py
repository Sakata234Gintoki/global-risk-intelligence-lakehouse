"""Export Gold Delta tables to Parquet on S3 and COPY them into Snowflake RAW.

Snowflake then owns the dimensional layer (dbt: staging -> SCD2 snapshot -> marts).
Requires GRIL_STORAGE_ROOT=s3://... and the stage/tables from sql/snowflake/*.sql.
"""
from __future__ import annotations

import argparse
import logging
import os

from gril.config import Storage, to_spark_uri
from gril.spark_session import get_spark

log = logging.getLogger("gril.snowflake")

# gold table -> Snowflake RAW table
TABLES = {
    "dim_country": "DIM_COUNTRY",
    "dim_indicator": "DIM_INDICATOR",
    "fact_indicator_observation": "FACT_INDICATOR_OBSERVATION",
    "fact_country_risk_score": "FACT_COUNTRY_RISK_SCORE",
}


def export_gold_to_parquet(storage: Storage) -> dict[str, int]:
    spark = get_spark("gril-snowflake-export")
    counts: dict[str, int] = {}
    for table in TABLES:
        df = spark.read.format("delta").load(to_spark_uri(storage.gold(table)))
        counts[table] = df.count()
        df.write.mode("overwrite").parquet(to_spark_uri(storage.gold_export(table)))
        log.info("exported %s (%d rows)", table, counts[table])
    return counts


def _connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "GRIL_TRANSFORMER"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "GRIL_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "GRIL"),
        schema="RAW",
        autocommit=False,
    )


def copy_into_snowflake(expected: dict[str, int]) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        for gold_table, sf_table in TABLES.items():
            cur.execute(f"DELETE FROM RAW.{sf_table}")
            cur.execute(
                f"""
                COPY INTO RAW.{sf_table}
                FROM @RAW.GOLD_EXPORT_STAGE/{gold_table}/
                FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                PATTERN = '.*[.]parquet'
                FORCE = TRUE
                ON_ERROR = ABORT_STATEMENT
                """
            )
            cur.execute(f"UPDATE RAW.{sf_table} SET _LOADED_AT = CURRENT_TIMESTAMP() WHERE _LOADED_AT IS NULL")
            cur.execute(f"SELECT COUNT(*) FROM RAW.{sf_table}")
            loaded = cur.fetchone()[0]
            if loaded != expected[gold_table]:
                raise RuntimeError(
                    f"{sf_table}: row count mismatch (delta={expected[gold_table]}, snowflake={loaded})"
                )
            log.info("loaded RAW.%s (%d rows)", sf_table, loaded)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run() -> None:
    storage = Storage.from_env()
    if not storage.is_s3:
        raise RuntimeError(
            "Snowflake COPY INTO reads from an S3 external stage; set GRIL_STORAGE_ROOT=s3://<bucket>/<prefix>."
        )
    counts = export_gold_to_parquet(storage)
    copy_into_snowflake(counts)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    argparse.ArgumentParser(description="Gold Delta -> Parquet on S3 -> Snowflake RAW").parse_args()
    run()


if __name__ == "__main__":
    main()
