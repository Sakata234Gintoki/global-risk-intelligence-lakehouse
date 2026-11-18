select
    f.country_key,
    c.country_iso3,
    f.indicator_key,
    i.indicator_code,
    i.source_system,
    f.year,
    f.value
from {{ source('raw', 'fact_indicator_observation') }} f
join {{ ref('stg_dim_country') }}   c on c.country_key   = f.country_key
join {{ ref('stg_dim_indicator') }} i on i.indicator_key = f.indicator_key
