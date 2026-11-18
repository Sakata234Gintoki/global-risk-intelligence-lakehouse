select
    r.country_key,
    c.country_iso3,
    r.year,
    r.health_risk_score,
    r.financial_risk_score,
    r.composite_risk_score,
    r.risk_tier,
    r.n_indicators
from {{ source('raw', 'fact_country_risk_score') }} r
join {{ ref('stg_dim_country') }} c on c.country_key = r.country_key
