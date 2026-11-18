{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['country_iso3', 'year'],
        merge_update_columns=[
            'health_risk_score', 'financial_risk_score', 'composite_risk_score',
            'risk_tier', 'n_indicators', 'updated_at'
        ],
    )
}}

select
    d.country_sk,
    r.country_iso3,
    r.year,
    r.health_risk_score,
    r.financial_risk_score,
    r.composite_risk_score,
    r.risk_tier,
    r.n_indicators,
    to_timestamp_ntz(current_timestamp()) as loaded_at,
    to_timestamp_ntz(current_timestamp()) as updated_at
from {{ ref('stg_fact_country_risk_score') }} r
join {{ ref('dim_country') }} d
  on  d.country_iso3 = r.country_iso3
  and to_timestamp_ntz(current_timestamp()) >= d.valid_from
  and to_timestamp_ntz(current_timestamp()) <  d.valid_to
