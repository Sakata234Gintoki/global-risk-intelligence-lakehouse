-- SCD2 integrity: validity windows of the same country must never overlap.
-- Returns offending rows; the test passes when zero rows come back.
select a.country_iso3, a.valid_from, a.valid_to
from {{ ref('dim_country') }} a
join {{ ref('dim_country') }} b
  on  a.country_iso3 = b.country_iso3
  and a.country_sk  <> b.country_sk
  and a.valid_from  <  b.valid_to
  and b.valid_from  <  a.valid_to
