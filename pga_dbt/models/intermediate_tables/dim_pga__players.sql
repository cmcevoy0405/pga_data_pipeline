with source as (
    select *
    from {{ref('stg_pga__players')}}
)
Select *
From source