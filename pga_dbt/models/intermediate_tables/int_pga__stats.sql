with staging_stats as (
    select * 
    from {{ ref('stg_pga__stats')}}
)
select 
    player_id,
    year,
    MAX(CASE WHEN stat_name = 'avg_score' then stat_value end) as avg_score,
    COALESCE(MAX(CASE WHEN stat_name = 'top_10' then stat_value end), 0) as top_10,
    MAX(CASE WHEN stat_name = 'drive_dist' then stat_value end) as drive_dist,
    MAX(CASE WHEN stat_name = 'drive_acc' then stat_value end) as drive_acc,
    MAX(CASE WHEN stat_name = 'putts_per_round' then stat_value end) as putts_per_round,
    MAX(CASE WHEN stat_name = 'scramble' then stat_value end) as scramble,
    MAX(CASE WHEN stat_name = 'gir' then stat_value end) as gir,
    MAX(CASE WHEN stat_name = 'bounce_back' then stat_value end) as bounce_back,
    MAX(CASE WHEN stat_name = 'strokes_gained' then stat_value end) as strokes_gained,
    MAX(CASE WHEN stat_name = 'strokes_gained_ot' then stat_value end) as strokes_gained_ot,
    MAX(CASE WHEN stat_name = 'strokes_gained_arg' then stat_value end) as strokes_gained_arg,
    MAX(CASE WHEN stat_name = 'strokes_gained_ttg' then stat_value end) as strokes_gained_ttg,
    MAX(CASE WHEN stat_name = 'strokes_gained_atg' then stat_value end) as strokes_gained_atg,
    MAX(CASE WHEN stat_name = 'strokes_gained_putt' then stat_value end) as strokes_gained_putt,
    MAX(CASE WHEN stat_name = 'par3_score' then stat_value end) as par3_score,
    MAX(CASE WHEN stat_name = 'par4_score' then stat_value end) as par4_score,
    MAX(CASE WHEN stat_name = 'par5_score' then stat_value end) as par5_score
from staging_stats
group by player_id, year