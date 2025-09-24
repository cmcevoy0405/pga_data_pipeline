with source as (
    select *
    from {{ source('pga_dataset', 'raw_pga_stats') }}
)
select 
    cast((player_id) as int) as player_id,
    cast((year) as int) as year,
    cast((rank) as int) as rank,
    cast(stat_id as int) as stat_id,
    trim(cast(stat_name as string)) as stat_name, 
    case when right(stat_value, 1) = '%' then cast(replace(stat_value, '%', '') as float64) / 100
    else cast(stat_value as float64)
    end as stat_value,
    cast(scraped_at as datetime) as scraped_at
from source
