{#- Type-2 country dimension: one row per country *version*. -#}
select
    {{ dbt_utils.generate_surrogate_key(['country_iso3', 'dbt_valid_from']) }} as country_sk,
    country_iso3,
    country_iso2,
    country_name,
    region_name,
    income_level_name,
    lending_type,
    capital_city,
    longitude,
    latitude,
    dbt_valid_from                                              as valid_from,
    coalesce(dbt_valid_to, '9999-12-31'::timestamp_ntz)         as valid_to,
    (dbt_valid_to is null)                                      as is_current
from {{ ref('snap_dim_country') }}
