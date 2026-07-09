{{ config(
    materialized='table'
) }}

with latest as (
    select *
    from {{ ref('intermediate_liveShare') }}
    where trade_date = (
        select max(trade_date) from {{ ref('intermediate_liveShare') }}
    )
),

volatility as (
    select
        stock_symbol,
        company_name,
        trade_date,
        max_price,
        min_price,
        previous_closing,
        closing_price,
        (max_price - min_price) as true_range,
        case when previous_closing is not null and previous_closing <> 0
            then ((max_price - min_price) / previous_closing) * 100
            else null
        end as volatility_pct,
        percent_change,
        volume
    from latest
    where max_price is not null and min_price is not null
),

ranked as (
    select
        *,
        row_number() over (order by volatility_pct desc nulls last) as vol_rank
    from volatility
)

select
    stock_symbol,
    company_name,
    trade_date,
    max_price,
    min_price,
    true_range,
    round(volatility_pct, 4) as volatility_pct,
    percent_change,
    volume,
    vol_rank as rank
from ranked
where vol_rank <= 10
order by trade_date desc, rank
