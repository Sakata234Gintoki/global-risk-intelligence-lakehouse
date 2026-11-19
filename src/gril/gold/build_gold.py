"""GOLD: star schema (dims + facts) and the composite country risk score."""
from __future__ import annotations

import argparse
import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from gril.config import Storage, load_config, to_spark_uri
from gril.spark_session import get_spark
from gril.utils.delta_merge import upsert_delta

log = logging.getLogger("gril.gold")


def build_dim_country(silver_countries: DataFrame) -> DataFrame:
    return silver_countries.select(
        F.xxhash64("country_iso3").alias("country_key"),
        "country_iso3", "country_iso2", "country_name", "region_name",
        "income_level_name", "lending_type", "capital_city", "longitude", "latitude",
    )


def build_dim_indicator(spark: SparkSession, cfg: dict[str, Any]) -> DataFrame:
    rows = []
    for system, cfg_key, domain in (("WHO", "who_indicators", "HEALTH"),
                                    ("WORLD_BANK", "worldbank_indicators", "FINANCIAL")):
        for ind in cfg[cfg_key]:
            rows.append((system, ind["code"], ind["name"], domain, ind["direction"]))
    df = spark.createDataFrame(
        rows, ["source_system", "indicator_code", "indicator_name", "domain", "direction"]
    )
    return df.select(
        F.xxhash64("source_system", "indicator_code").alias("indicator_key"),
        "indicator_code", "indicator_name", "source_system", "domain", "direction",
    )


def build_fact_observations(
    silver_who: DataFrame,
    silver_wb: DataFrame,
    dim_country: DataFrame,
    dim_indicator: DataFrame,
    who_total_dimensions: list[str],
) -> DataFrame:
    who = (
        silver_who.filter(F.col("dimension_value").isin(who_total_dimensions))
        .select("country_iso3", "indicator_code", "year", "value", F.lit("WHO").alias("source_system"))
    )
    wb = silver_wb.select(
        "country_iso3", "indicator_code", "year", "value", F.lit("WORLD_BANK").alias("source_system")
    )
    return (
        who.unionByName(wb)
        .join(dim_country.select("country_key", "country_iso3"), "country_iso3")
        .join(dim_indicator.select("indicator_key", "indicator_code", "source_system"),
              ["indicator_code", "source_system"])
        .groupBy("country_key", "indicator_key", "year")
        .agg(F.avg("value").alias("value"))
    )


def compute_risk_scores(
    obs: DataFrame,
    dim_indicator: DataFrame,
    weights: dict[str, float],
    tiers: dict[str, float],
    min_countries: int = 30,
) -> DataFrame:
    """Percentile-rank every indicator across countries per year -> 0..100 risk.

    higher_is_better indicators: high value = low risk;  lower_is_better: high value = high risk.
    Domain score = mean indicator risk; composite = weighted mean of the available domains.
    """
    df = obs.join(dim_indicator.select("indicator_key", "domain", "direction"), "indicator_key")
    by_ind_year = Window.partitionBy("indicator_key", "year")
    df = (
        df.withColumn("n_countries", F.count("*").over(by_ind_year))
        .filter(F.col("n_countries") >= min_countries)
        .withColumn("pct", F.percent_rank().over(by_ind_year.orderBy("value")))
        .withColumn(
            "indicator_risk",
            F.when(F.col("direction") == "lower_is_better", F.col("pct") * 100)
            .otherwise((F.lit(1.0) - F.col("pct")) * 100),
        )
    )
    agg = df.groupBy("country_key", "year").agg(
        F.avg(F.when(F.col("domain") == "HEALTH", F.col("indicator_risk"))).alias("health_risk_score"),
        F.avg(F.when(F.col("domain") == "FINANCIAL", F.col("indicator_risk"))).alias("financial_risk_score"),
        F.count("*").cast("int").alias("n_indicators"),
    )
    wh, wf = float(weights["health"]), float(weights["financial"])
    h, f = F.col("health_risk_score"), F.col("financial_risk_score")
    numerator = F.coalesce(h * F.lit(wh), F.lit(0.0)) + F.coalesce(f * F.lit(wf), F.lit(0.0))
    denominator = (F.when(h.isNotNull(), F.lit(wh)).otherwise(F.lit(0.0))
                   + F.when(f.isNotNull(), F.lit(wf)).otherwise(F.lit(0.0)))
    composite = F.round(numerator / denominator, 2)
    return (
        agg.withColumn("health_risk_score", F.round(h, 2))
        .withColumn("financial_risk_score", F.round(f, 2))
        .withColumn("composite_risk_score", composite)
        .withColumn(
            "risk_tier",
            F.when(F.col("composite_risk_score") < tiers["low"], "LOW")
            .when(F.col("composite_risk_score") < tiers["medium"], "MEDIUM")
            .otherwise("HIGH"),
        )
        .select("country_key", "year", "health_risk_score", "financial_risk_score",
                "composite_risk_score", "risk_tier", "n_indicators")
    )


def run() -> None:
    spark = get_spark("gril-gold")
    storage = Storage.from_env()
    cfg = load_config()

    def silver(t: str) -> DataFrame:
        return spark.read.format("delta").load(to_spark_uri(storage.silver(t)))

    def write_overwrite(df: DataFrame, table: str) -> None:
        (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .save(to_spark_uri(storage.gold(table))))

    dim_country = build_dim_country(silver("countries"))
    dim_indicator = build_dim_indicator(spark, cfg)
    write_overwrite(dim_country, "dim_country")
    write_overwrite(dim_indicator, "dim_indicator")

    facts = build_fact_observations(
        silver("who_health_indicators"), silver("wb_financial_indicators"),
        dim_country, dim_indicator, cfg["who_total_dimensions"],
    )
    log.info("fact_indicator_observation merge: %s", upsert_delta(
        spark, facts, to_spark_uri(storage.gold("fact_indicator_observation")),
        ["country_key", "indicator_key", "year"]))

    risk_cfg = cfg["risk"]
    scores = compute_risk_scores(
        facts, dim_indicator, risk_cfg["weights"], risk_cfg["tiers"],
        risk_cfg["min_countries_per_indicator_year"],
    )
    log.info("fact_country_risk_score merge: %s", upsert_delta(
        spark, scores, to_spark_uri(storage.gold("fact_country_risk_score")), ["country_key", "year"]))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    argparse.ArgumentParser(description="Silver Delta -> Gold star schema + risk scores").parse_args()
    run()


if __name__ == "__main__":
    main()
