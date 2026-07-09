{{ config(
    materialized='table'
) }}

with history as (
    select *
    from {{ ref('fct_stock_history') }}
),

volatility as (
    select
        stock_symbol,
        company_name,
        trade_date,
        max_price,
        min_price,
        previous_closing,
        case when previous_closing is not null and previous_closing <> 0
            then ((max_price - min_price) / previous_closing) * 100
            else null
        end as volatility_pct,
        (max_price - min_price) as true_range
    from history
    where max_price is not null and min_price is not null
),

aggregated as (
    select
        stock_symbol,
        company_name,
        count(distinct trade_date) as no_of_day,
        round(avg(volatility_pct)::numeric, 6) as avg_volatility_pct,
        round(avg(true_range)::numeric, 6) as avg_true_range,
        max(volatility_pct) as max_volatility_pct
    from volatility
    group by stock_symbol, company_name
),

ranked as (
    select *, row_number() over (order by avg_volatility_pct desc nulls last) as vol_rank from aggregated
)

select
    stock_symbol,
    company_name,
    no_of_day,
    avg_true_range as true_range,
    avg_volatility_pct as volatility_pct,
    max_volatility_pct,
    vol_rank as rank
from ranked
where vol_rank <= 10
order by avg_volatility_pct desc, rank
