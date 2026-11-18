select distinct
    year,
    floor(year / 10) * 10 as decade
from {{ ref('stg_fact_indicator_observation') }}
