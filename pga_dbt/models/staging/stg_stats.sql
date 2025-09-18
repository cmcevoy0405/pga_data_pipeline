with source as (
    select *
    from {{ source('pga_dataset', 'raw_pga_stats') }}
)
select 
    cast(trim(player_id) as int),
    cast(trim(year) as int),
    cast(trim(rank) as int),
    trim(stat_id),
    trim(stat_name), 
    trim(stat_value),
    trim(scraped_at)
from source
