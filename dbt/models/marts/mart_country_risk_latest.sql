{#- BI-ready: latest risk score per country, with the *current* country attributes. -#}
with latest as (
    select
        country_iso3,
        max(year) as latest_year
    from {{ ref('fact_country_risk_score') }}
    group by country_iso3
)

select
    c.country_iso3,
    c.country_name,
    c.region_name,
    c.income_level_name,
    r.year as risk_year,
    r.health_risk_score,
    r.financial_risk_score,
    r.composite_risk_score,
    r.risk_tier,
    rank() over (order by r.composite_risk_score desc) as global_risk_rank
from {{ ref('fact_country_risk_score') }} r
join latest l on l.country_iso3 = r.country_iso3 and l.latest_year = r.year
join {{ ref('dim_country') }} c on c.country_iso3 = r.country_iso3 and c.is_current
