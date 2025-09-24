with stats as (
    select *
    from {{ref('int_pga__stats')}}
),
players as (
    select *
    from {{ref('dim_pga__players')}}
)
select 
    p.player_name,
    p.player_id,
    s.year,
    s.avg_score,
    s.strokes_gained,
    s.strokes_gained_arg,
    s.strokes_gained_atg,
    s.strokes_gained_ot,
    s.strokes_gained_ttg,
    s.strokes_gained_putt,
    s.drive_dist,
    s.drive_acc,
    s.putts_per_round,
    s.scramble,
    s.gir,
    s.bounce_back,
    s.par3_score,
    s.par4_score,
    s.par5_score,
    s.top_10
from stats s
left join players p 
on s.player_id = p.player_id