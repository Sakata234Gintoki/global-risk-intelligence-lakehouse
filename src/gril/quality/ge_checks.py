"""Great Expectations quality gate on the Gold layer. Non-zero exit blocks the pipeline.

Uses the GE 0.18 legacy pandas API (`great_expectations.from_pandas`). Gold tables are small
(countries x years x indicators), so converting to pandas is fine.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable
from datetime import datetime, timezone

import fsspec

from gril.config import Storage, to_fs_uri, to_spark_uri
from gril.spark_session import get_spark

log = logging.getLogger("gril.quality")


def _dim_country(ds) -> None:
    ds.expect_table_row_count_to_be_between(min_value=150)
    ds.expect_column_values_to_not_be_null("country_key")
    ds.expect_column_values_to_be_unique("country_key")
    ds.expect_column_values_to_be_unique("country_iso3")
    ds.expect_column_value_lengths_to_equal("country_iso3", 3)
    ds.expect_column_values_to_not_be_null("region_name")


def _dim_indicator(ds) -> None:
    ds.expect_column_values_to_be_unique("indicator_key")
    ds.expect_column_values_to_be_in_set("domain", ["HEALTH", "FINANCIAL"])
    ds.expect_column_values_to_be_in_set("direction", ["higher_is_better", "lower_is_better"])


def _fact_obs(ds) -> None:
    for col in ("country_key", "indicator_key", "year", "value"):
        ds.expect_column_values_to_not_be_null(col)
    ds.expect_column_values_to_be_between("year", min_value=1960, max_value=2100)
    ds.expect_compound_columns_to_be_unique(["country_key", "indicator_key", "year"])


def _fact_risk(ds) -> None:
    for col in ("country_key", "year", "composite_risk_score", "risk_tier"):
        ds.expect_column_values_to_not_be_null(col)
    for col in ("health_risk_score", "financial_risk_score", "composite_risk_score"):
        ds.expect_column_values_to_be_between(col, min_value=0, max_value=100)
    ds.expect_column_values_to_be_in_set("risk_tier", ["LOW", "MEDIUM", "HIGH"])
    ds.expect_compound_columns_to_be_unique(["country_key", "year"])


SUITES: dict[str, Callable] = {
    "dim_country": _dim_country,
    "dim_indicator": _dim_indicator,
    "fact_indicator_observation": _fact_obs,
    "fact_country_risk_score": _fact_risk,
}


def run() -> bool:
    import great_expectations as gx

    spark = get_spark("gril-quality")
    storage = Storage.from_env()
    report: dict[str, dict] = {}
    all_ok = True

    for table, suite in SUITES.items():
        pdf = spark.read.format("delta").load(to_spark_uri(storage.gold(table))).toPandas()
        ds = gx.from_pandas(pdf)
        suite(ds)
        result = ds.validate()
        failed = [
            {"expectation": r.expectation_config.expectation_type,
             "kwargs": {k: v for k, v in r.expectation_config.kwargs.items() if k != "result_format"}}
            for r in result.results if not r.success
        ]
        report[table] = {"success": bool(result.success), "rows": int(len(pdf)), "failed": failed}
        all_ok &= bool(result.success)
        log.info("GE %-28s success=%s rows=%d failed=%d", table, result.success, len(pdf), len(failed))

    out = storage.quality(f"ge_results_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json")
    uri = to_fs_uri(out)
    if "://" not in uri:
        import os

        os.makedirs(os.path.dirname(uri), exist_ok=True)
    with fsspec.open(uri, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    log.info("GE report written to %s", out)
    return all_ok


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    argparse.ArgumentParser(description="Great Expectations gate on Gold tables").parse_args()
    if not run():
        log.error("Data quality gate FAILED - blocking downstream load")
        sys.exit(1)


if __name__ == "__main__":
    main()
