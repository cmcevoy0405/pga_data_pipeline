with source as (
    select *
    from {{ source('pga_dataset', 'raw_pga_stats') }}
),
rank as (
    select 
        player_id,
        player_name,
        country,
        row_number() over (partition by player_id order by scraped_at desc) as rn
    from source
)
select distinct
    player_id,
    lower(trim(player_name)) as player_name,
    lower(trim(country)) as country
from rank
where rn = 1
