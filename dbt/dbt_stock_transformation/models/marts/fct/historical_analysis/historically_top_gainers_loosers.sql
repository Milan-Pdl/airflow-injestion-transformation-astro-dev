{{ config(
    materialized='table'
) }}

with history as (
    select *
    from {{ ref('intermediate_share_history') }}
),

aggregated as (
    select
        stock_symbol,
        company_name,
        count(distinct trade_date) as no_of_day,
        round(avg(percent_change)::numeric, 6) as avg_percent_change,
        sum(case when percent_change > 0 then 1 else 0 end) as gain_days,
        sum(case when percent_change < 0 then 1 else 0 end) as loss_days,
        round(avg(difference_rs)::numeric, 6) as avg_difference_rs,
        round(avg(volume)::numeric, 2) as avg_volume
    from history
    group by stock_symbol, company_name
),

ranked as (
    select
        *,
        row_number() over (order by avg_percent_change desc nulls last) as gain_rank,
        row_number() over (order by avg_percent_change asc nulls last) as loss_rank
    from aggregated
)

select
    stock_symbol,
    company_name,
    no_of_day,
    avg_percent_change,
    gain_days,
    loss_days,
    avg_difference_rs as change_rs,
    avg_volume,
    gain_rank as rank,
    'gainer' as side
from ranked
where gain_rank <= 100

union all

select
    stock_symbol,
    company_name,
    no_of_day,
    avg_percent_change,
    gain_days,
    loss_days,
    avg_difference_rs as change_rs,
    avg_volume,
    loss_rank as rank,
    'loser' as side
from ranked
where loss_rank <= 100

order by avg_percent_change desc, side, rank
