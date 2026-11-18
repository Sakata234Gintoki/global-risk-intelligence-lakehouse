{% snapshot snap_dim_country %}

{#-
  SCD Type 2 on the country dimension. Every run compares the freshly loaded RAW state with the
  last snapshot; if any tracked attribute changed (e.g. the World Bank reclassifies a country's
  income group), the old row is closed (dbt_valid_to) and a new current row is inserted.
-#}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='country_iso3',
        strategy='check',
        check_cols=['country_name', 'region_name', 'income_level_name', 'lending_type', 'capital_city'],
    )
}}

select
    country_iso3,
    country_iso2,
    country_name,
    region_name,
    income_level_name,
    lending_type,
    capital_city,
    longitude,
    latitude
from {{ ref('stg_dim_country') }}

{% endsnapshot %}
