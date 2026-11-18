select
    indicator_key,
    indicator_code,
    indicator_name,
    source_system,
    domain,
    direction
from {{ ref('stg_dim_indicator') }}
