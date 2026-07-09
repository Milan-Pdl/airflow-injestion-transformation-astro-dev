{{ config(
    materialized='table'
) }}

with history as (
    select *
    from {{ ref('fct_stock_history') }}
),

aggregated as (
    select
        stock_symbol,
        company_name,
        count(distinct trade_date) as no_of_day,
        sum(coalesce(volume,0)) as total_volume,
        round(avg(coalesce(volume,0))::numeric, 2) as avg_volume,
        max(coalesce(volume,0)) as max_volume
    from history
    group by stock_symbol, company_name
),

ranked as (
    select *, row_number() over (order by total_volume desc nulls last) as vol_rank from aggregated
)

select
    stock_symbol,
    company_name,
    no_of_day,
    total_volume,
    avg_volume,
    max_volume,
    vol_rank as rank
from ranked
where vol_rank <= 10
order by total_volume desc, rank
