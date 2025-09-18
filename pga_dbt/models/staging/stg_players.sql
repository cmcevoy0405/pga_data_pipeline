with source as (
    select *
    from {{ source('pga_dataset', 'raw_pga_stats') }}
)
select distinct 
    player_id,
    initcap(trim(player_name)),
    trim(country)
from source 
