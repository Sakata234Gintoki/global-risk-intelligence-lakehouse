-- SCD2 integrity: every country has exactly one current version.
select country_iso3, count(*) as current_versions
from {{ ref('dim_country') }}
where is_current
group by country_iso3
having count(*) <> 1
