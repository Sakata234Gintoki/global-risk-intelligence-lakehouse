"""Source API clients + record flatteners (pure functions, unit-tested)."""
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WHO_BASE = "https://ghoapi.azureedge.net/api"
WB_BASE = "https://api.worldbank.org/v2"


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "gril-lakehouse/1.0"
    return session


# ----------------------------------------------------------------------------- WHO


def fetch_who_indicator(
    session: requests.Session, code: str, year_start: int, year_end: int
) -> list[dict[str, Any]]:
    odata_filter = (
        f"SpatialDimType eq 'COUNTRY' and TimeDim ge {year_start} and TimeDim le {year_end}"
    )
    resp = session.get(f"{WHO_BASE}/{code}", params={"$filter": odata_filter}, timeout=180)
    resp.raise_for_status()
    return resp.json().get("value", [])


def flatten_who_record(rec: dict[str, Any], indicator_code: str) -> dict[str, Any]:
    return {
        "indicator_code": rec.get("IndicatorCode") or indicator_code,
        "spatial_dim_type": rec.get("SpatialDimType"),
        "spatial_dim": rec.get("SpatialDim"),
        "time_dim": rec.get("TimeDim"),
        "dim1_type": rec.get("Dim1Type"),
        "dim1": rec.get("Dim1"),
        "numeric_value": rec.get("NumericValue"),
        "value": rec.get("Value"),
        "low": rec.get("Low"),
        "high": rec.get("High"),
        "source_date": rec.get("Date"),
    }


# ------------------------------------------------------------------------ World Bank


def _wb_pages(session: requests.Session, url: str, params: dict[str, Any]) -> Iterator[dict[str, Any]]:
    page = 1
    while True:
        resp = session.get(
            url, params={**params, "format": "json", "per_page": 1000, "page": page}, timeout=180
        )
        resp.raise_for_status()
        payload = resp.json()
        # error responses come back as [{"message": [...]}]
        if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
            return
        meta, rows = payload[0], payload[1]
        yield from rows
        if page >= int(meta.get("pages", 1)):
            return
        page += 1


def fetch_wb_countries(session: requests.Session) -> list[dict[str, Any]]:
    return list(_wb_pages(session, f"{WB_BASE}/country", {}))


def fetch_wb_indicator(
    session: requests.Session, code: str, year_start: int, year_end: int
) -> list[dict[str, Any]]:
    url = f"{WB_BASE}/country/all/indicator/{code}"
    return list(_wb_pages(session, url, {"date": f"{year_start}:{year_end}"}))


def flatten_wb_observation(rec: dict[str, Any]) -> dict[str, Any]:
    indicator = rec.get("indicator") or {}
    country = rec.get("country") or {}
    return {
        "indicator_code": indicator.get("id"),
        "indicator_name": indicator.get("value"),
        "country_iso3": rec.get("countryiso3code"),
        "country_name": country.get("value"),
        "year": rec.get("date"),
        "value": rec.get("value"),
        "unit": rec.get("unit"),
        "obs_status": rec.get("obs_status"),
        "decimal": rec.get("decimal"),
    }


def flatten_wb_country(rec: dict[str, Any]) -> dict[str, Any]:
    region = rec.get("region") or {}
    income = rec.get("incomeLevel") or {}
    lending = rec.get("lendingType") or {}
    return {
        "country_iso3": rec.get("id"),
        "country_iso2": rec.get("iso2Code"),
        "country_name": rec.get("name"),
        "region_id": region.get("id"),
        "region_name": region.get("value"),
        "income_level_id": income.get("id"),
        "income_level_name": income.get("value"),
        "lending_type": lending.get("value"),
        "capital_city": rec.get("capitalCity"),
        "longitude": rec.get("longitude"),
        "latitude": rec.get("latitude"),
    }
