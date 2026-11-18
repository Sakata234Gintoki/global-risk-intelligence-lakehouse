"""SILVER: typed, validated, de-duplicated tables, upserted into Delta with MERGE."""
from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gril.config import Storage, to_spark_uri
from gril.spark_session import get_spark
from gril.utils.delta_merge import latest_per_key, upsert_delta

log = logging.getLogger("gril.silver")

ISO3 = r"^[A-Z]{3}$"


def clean_who(bronze: DataFrame) -> DataFrame:
    """WHO GHO observations -> one row per (indicator, country, year, dimension)."""
    df = (
        bronze.filter(F.col("spatial_dim_type") == "COUNTRY")
        .select(
            F.trim("indicator_code").alias("indicator_code"),
            F.upper(F.trim("spatial_dim")).alias("country_iso3"),
            F.col("time_dim").cast("int").alias("year"),
            F.coalesce(F.nullif(F.trim("dim1"), F.lit("")), F.lit("TOTAL")).alias("dimension_value"),
            F.col("numeric_value").cast("double").alias("value"),
            F.col("low").cast("double").alias("value_low"),
            F.col("high").cast("double").alias("value_high"),
            "_batch_id",
            "_ingested_at",
        )
        .filter(F.col("year").isNotNull() & F.col("value").isNotNull())
        .filter(F.col("country_iso3").rlike(ISO3))
    )
    return latest_per_key(df, ["indicator_code", "country_iso3", "year", "dimension_value"])


def clean_wb_indicators(bronze: DataFrame) -> DataFrame:
    df = (
        bronze.select(
            F.trim("indicator_code").alias("indicator_code"),
            F.trim("indicator_name").alias("indicator_name"),
            F.upper(F.trim("country_iso3")).alias("country_iso3"),
            F.col("year").cast("int").alias("year"),
            F.col("value").cast("double").alias("value"),
            "_batch_id",
            "_ingested_at",
        )
        .filter(F.col("year").isNotNull() & F.col("value").isNotNull())
        .filter(F.col("country_iso3").rlike(ISO3))
    )
    return latest_per_key(df, ["indicator_code", "country_iso3", "year"])


def clean_countries(bronze: DataFrame) -> DataFrame:
    """Real countries only (drops World Bank aggregates such as 'World', 'Euro area')."""
    df = (
        bronze.select(
            F.upper(F.trim("country_iso3")).alias("country_iso3"),
            F.upper(F.trim("country_iso2")).alias("country_iso2"),
            F.trim("country_name").alias("country_name"),
            F.trim("region_name").alias("region_name"),
            F.trim("income_level_name").alias("income_level_name"),
            F.trim("lending_type").alias("lending_type"),
            F.trim("capital_city").alias("capital_city"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("latitude").cast("double").alias("latitude"),
            "_batch_id",
            "_ingested_at",
        )
        .filter(F.col("country_iso3").rlike(ISO3))
        .filter(F.col("region_name").isNotNull() & (F.col("region_name") != "Aggregates"))
    )
    return latest_per_key(df, ["country_iso3"])


def run() -> None:
    spark = get_spark("gril-silver")
    storage = Storage.from_env()

    def bronze(t: str) -> DataFrame:
        return spark.read.format("delta").load(to_spark_uri(storage.bronze(t)))

    jobs = [
        ("who_health_indicators", clean_who(bronze("who_gho")),
         ["indicator_code", "country_iso3", "year", "dimension_value"]),
        ("wb_financial_indicators", clean_wb_indicators(bronze("wb_indicators")),
         ["indicator_code", "country_iso3", "year"]),
        ("countries", clean_countries(bronze("wb_countries")), ["country_iso3"]),
    ]
    for table, df, keys in jobs:
        metrics = upsert_delta(spark, df, to_spark_uri(storage.silver(table)), keys)
        log.info("silver.%s merge metrics: %s", table, metrics)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    argparse.ArgumentParser(description="Bronze Delta -> Silver Delta (MERGE upserts)").parse_args()
    run()


if __name__ == "__main__":
    main()
