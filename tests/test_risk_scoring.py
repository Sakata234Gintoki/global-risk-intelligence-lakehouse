import pytest

from gril.gold.build_gold import compute_risk_scores

WEIGHTS = {"health": 0.5, "financial": 0.5}
TIERS = {"low": 33.0, "medium": 66.0}


@pytest.fixture()
def dim_indicator(spark):
    return spark.createDataFrame(
        [(1, "HEALTH", "higher_is_better"), (2, "FINANCIAL", "lower_is_better")],
        ["indicator_key", "domain", "direction"],
    )


def test_percentile_risk_direction_and_composite(spark, dim_indicator):
    # 4 countries, 2020.
    # Health (higher is better): country 4 best. Financial (lower is better): country 1 best.
    obs = spark.createDataFrame(
        [(c, 1, 2020, float(c)) for c in (1, 2, 3, 4)] + [(c, 2, 2020, float(c)) for c in (1, 2, 3, 4)],
        ["country_key", "indicator_key", "year", "value"],
    )
    rows = {r.country_key: r for r in
            compute_risk_scores(obs, dim_indicator, WEIGHTS, TIERS, min_countries=3).collect()}

    assert rows[4].health_risk_score == pytest.approx(0.0)
    assert rows[1].health_risk_score == pytest.approx(100.0)
    assert rows[1].financial_risk_score == pytest.approx(0.0)
    assert rows[4].financial_risk_score == pytest.approx(100.0)
    # both domains present -> equal-weight composite
    assert rows[2].composite_risk_score == pytest.approx(50.0, abs=0.01)
    assert rows[2].risk_tier == "MEDIUM"
    assert all(0 <= r.composite_risk_score <= 100 for r in rows.values())


def test_missing_domain_renormalises_weights(spark, dim_indicator):
    obs = spark.createDataFrame(
        [(c, 1, 2020, float(c)) for c in (1, 2, 3, 4)], ["country_key", "indicator_key", "year", "value"]
    )
    rows = {r.country_key: r for r in
            compute_risk_scores(obs, dim_indicator, WEIGHTS, TIERS, min_countries=3).collect()}
    assert rows[1].financial_risk_score is None
    assert rows[1].composite_risk_score == pytest.approx(100.0)  # health only
    assert rows[1].risk_tier == "HIGH"
    assert rows[4].risk_tier == "LOW"


def test_small_samples_are_excluded(spark, dim_indicator):
    obs = spark.createDataFrame([(1, 1, 2020, 1.0), (2, 1, 2020, 2.0)],
                                ["country_key", "indicator_key", "year", "value"])
    assert compute_risk_scores(obs, dim_indicator, WEIGHTS, TIERS, min_countries=30).count() == 0
