select
    indicator_key,
    indicator_code,
    indicator_name,
    source_system,
    domain,
    direction
from {{ source('raw', 'dim_indicator') }}
