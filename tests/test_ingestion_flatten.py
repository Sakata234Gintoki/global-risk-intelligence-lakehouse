from gril.ingestion.clients import flatten_wb_country, flatten_wb_observation, flatten_who_record


def test_flatten_who_record():
    rec = {"IndicatorCode": "WHOSIS_000001", "SpatialDimType": "COUNTRY", "SpatialDim": "IND",
           "TimeDim": 2019, "Dim1": "SEX_BTSX", "NumericValue": 70.8, "Low": None}
    out = flatten_who_record(rec, "WHOSIS_000001")
    assert out["spatial_dim"] == "IND"
    assert out["time_dim"] == 2019
    assert out["dim1"] == "SEX_BTSX"
    assert out["numeric_value"] == 70.8


def test_flatten_who_falls_back_to_requested_code():
    assert flatten_who_record({}, "X")["indicator_code"] == "X"


def test_flatten_wb_observation():
    rec = {"indicator": {"id": "FP.CPI.TOTL.ZG", "value": "Inflation"},
           "country": {"id": "IN", "value": "India"}, "countryiso3code": "IND",
           "date": "2020", "value": 6.62}
    out = flatten_wb_observation(rec)
    assert out["indicator_code"] == "FP.CPI.TOTL.ZG"
    assert out["country_iso3"] == "IND"
    assert out["year"] == "2020"
    assert out["value"] == 6.62


def test_flatten_wb_country_handles_missing_nested_objects():
    out = flatten_wb_country({"id": "IND", "iso2Code": "IN", "name": "India",
                              "region": {"id": "SAS", "value": "South Asia"},
                              "incomeLevel": {"id": "LMC", "value": "Lower middle income"}})
    assert out["region_name"] == "South Asia"
    assert out["income_level_name"] == "Lower middle income"
    assert out["lending_type"] is None
