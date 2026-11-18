select
    country_key,
    upper(country_iso3)  as country_iso3,
    upper(country_iso2)  as country_iso2,
    country_name,
    region_name,
    income_level_name,
    lending_type,
    capital_city,
    longitude,
    latitude
from {{ source('raw', 'dim_country') }}
