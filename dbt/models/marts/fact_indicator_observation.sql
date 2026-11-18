{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['country_iso3', 'indicator_code', 'year'],
        merge_update_columns=['value', 'updated_at'],
    )
}}

{#-
  Point-in-time join: a fact row is bound to the country *version* (SCD2) that was valid when the
  row was first inserted. On later runs MERGE only refreshes `value`, so the dimension key stays
  pinned to the historical version.
-#}

select
    d.country_sk,
    f.country_iso3,
    f.indicator_key,
    f.indicator_code,
    f.year,
    f.value,
    to_timestamp_ntz(current_timestamp()) as loaded_at,
    to_timestamp_ntz(current_timestamp()) as updated_at
from {{ ref('stg_fact_indicator_observation') }} f
join {{ ref('dim_country') }} d
  on  d.country_iso3 = f.country_iso3
  and to_timestamp_ntz(current_timestamp()) >= d.valid_from
  and to_timestamp_ntz(current_timestamp()) <  d.valid_to
