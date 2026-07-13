{{ config(
    materialized='table'
) }}

with history as (
    select *
    from {{ ref('intermediate_share_history') }}
),

summary as (
    select
        count(distinct stock_symbol) as total_stocks,
        count(distinct trade_date) as no_of_day,
        sum(coalesce(volume,0)) as total_volume,
        sum(coalesce(amount,0)) as total_amount,
        round(avg(percent_change)::numeric, 6) as avg_return,
        sum(case when percent_change > 0 then 1 else 0 end) as gain_stocks,
        sum(case when percent_change < 0 then 1 else 0 end) as loss_stocks
    from history
)

select * from summary
