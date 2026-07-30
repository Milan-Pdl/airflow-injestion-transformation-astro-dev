{{ config(
    materialized='table'
) }}

with volatility as (

    select
        stock_symbol,
        company_name,
        trade_date,
        max_price,
        min_price,
        previous_closing,
        closing_price,
        (max_price - min_price) as true_range,
        case 
            when previous_closing is not null and previous_closing <> 0
            then ((max_price - min_price) / previous_closing) * 100
            else null
        end as volatility_pct,
        percent_change,
        volume
    from {{ ref('intermediate_liveShare') }}
    where max_price is not null 
      and min_price is not null

),

ranked as (

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
        row_number() over (order by volatility_pct desc nulls last) as rank
    from volatility

)

select
    stock_symbol,
    company_name,
    trade_date,
    max_price,
    min_price,
    true_range,
    volatility_pct,
    percent_change,
    volume,
    rank
from ranked
where rank <= 10
order by rank