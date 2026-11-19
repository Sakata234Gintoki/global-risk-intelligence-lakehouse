"""BRONZE: land raw JSON as Delta, untouched, plus lineage columns.

Idempotent per batch: re-running a batch replaces only that batch's rows (replaceWhere).
"""
from __future__ import annotations

import argparse
import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from gril.config import Storage, to_spark_uri
from gril.spark_session import get_spark
from gril.utils.manifest import latest_batch_id

log = logging.getLogger("gril.bronze")


def _all_strings(names: list[str]) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


WHO_SCHEMA = _all_strings(
    ["indicator_code", "spatial_dim_type", "spatial_dim", "time_dim", "dim1_type", "dim1",
     "numeric_value", "value", "low", "high", "source_date"]
)
WB_INDICATOR_SCHEMA = _all_strings(
    ["indicator_code", "indicator_name", "country_iso3", "country_name", "year", "value",
     "unit", "obs_status", "decimal"]
)
WB_COUNTRY_SCHEMA = _all_strings(
    ["country_iso3", "country_iso2", "country_name", "region_id", "region_name",
     "income_level_id", "income_level_name", "lending_type", "capital_city", "longitude", "latitude"]
)

# table -> (landing glob with {batch}, schema)
BRONZE_SOURCES = {
    "who_gho": ("who/*/batch_id={batch}/data.jsonl", WHO_SCHEMA),
    "wb_indicators": ("worldbank/indicators/*/batch_id={batch}/data.jsonl", WB_INDICATOR_SCHEMA),
    "wb_countries": ("worldbank/countries/batch_id={batch}/data.jsonl", WB_COUNTRY_SCHEMA),
}


def read_landing(
    spark: SparkSession, storage: Storage, pattern: str, schema: StructType, batch_id: str
) -> DataFrame:
    src = to_spark_uri(storage.landing(pattern.format(batch=batch_id)))
    return (
        spark.read.schema(schema).json(src)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze(spark: SparkSession, df: DataFrame, target: str, batch_id: str) -> None:
    writer = df.write.format("delta").mode("overwrite")
    if DeltaTable.isDeltaTable(spark, target):
        writer = writer.option("replaceWhere", f"_batch_id = '{batch_id}'")
    writer.save(target)


def run(batch_id: str | None = None) -> None:
    spark = get_spark("gril-bronze")
    storage = Storage.from_env()
    batch_id = batch_id or latest_batch_id(storage)
    for table, (pattern, schema) in BRONZE_SOURCES.items():
        df = read_landing(spark, storage, pattern, schema, batch_id)
        target = to_spark_uri(storage.bronze(table))
        write_bronze(spark, df, target, batch_id)
        log.info("bronze.%s batch=%s rows=%d", table, batch_id, spark.read.format("delta").load(target)
                 .filter(F.col("_batch_id") == batch_id).count())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description="Landing JSON -> Bronze Delta")
    parser.add_argument("--batch-id", default=None, help="defaults to the newest manifest")
    run(parser.parse_args().batch_id)


if __name__ == "__main__":
    main()
