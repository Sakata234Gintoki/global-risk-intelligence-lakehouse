from datetime import datetime

from gril.silver.transform_silver import clean_countries, clean_who

TS1, TS2 = datetime(2026, 1, 1), datetime(2026, 1, 2)
WHO_SCHEMA = ("indicator_code string, spatial_dim_type string, spatial_dim string, time_dim string, "
              "dim1 string, numeric_value string, low string, high string, "
              "_batch_id string, _ingested_at timestamp")
COUNTRY_SCHEMA = ("country_iso3 string, country_iso2 string, country_name string, region_name string, "
                  "income_level_name string, lending_type string, capital_city string, longitude string, "
                  "latitude string, _batch_id string, _ingested_at timestamp")


def test_clean_who_filters_types_and_dedups_latest(spark):
    rows = [
        ("WHOSIS_000001", "COUNTRY", "ind", "2019", "SEX_BTSX", "70.0", None, None, "b1", TS1),
        ("WHOSIS_000001", "COUNTRY", "IND", "2019", "SEX_BTSX", "70.8", None, None, "b2", TS2),  # newer
        ("WHOSIS_000001", "REGION", "SEAR", "2019", "SEX_BTSX", "69.0", None, None, "b1", TS1),   # not a country
        ("WHOSIS_000001", "COUNTRY", "XX", "2019", "SEX_BTSX", "1.0", None, None, "b1", TS1),     # bad iso3
        ("WHOSIS_000001", "COUNTRY", "FRA", "2019", None, None, None, None, "b1", TS1),            # no value
        ("WHS4_100", "COUNTRY", "FRA", "2018", None, "95", None, None, "b1", TS1),                 # dim -> TOTAL
    ]
    df = clean_who(spark.createDataFrame(rows, WHO_SCHEMA)).collect()
    got = {(r.indicator_code, r.country_iso3, r.year, r.dimension_value): r.value for r in df}
    assert got == {
        ("WHOSIS_000001", "IND", 2019, "SEX_BTSX"): 70.8,
        ("WHS4_100", "FRA", 2018, "TOTAL"): 95.0,
    }


def test_clean_countries_drops_aggregates(spark):
    rows = [
        ("IND", "IN", "India", "South Asia", "Lower middle income", "IBRD", "New Delhi", "77.2", "28.6", "b1", TS1),
        ("WLD", "1W", "World", "Aggregates", "Aggregates", "Aggregates", None, None, None, "b1", TS1),
    ]
    out = clean_countries(spark.createDataFrame(rows, COUNTRY_SCHEMA)).collect()
    assert [r.country_iso3 for r in out] == ["IND"]
    assert out[0].longitude == 77.2
