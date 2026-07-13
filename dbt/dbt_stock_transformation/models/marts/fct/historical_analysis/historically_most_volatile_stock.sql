{{ config(
    materialized='table'
) }}

with history as (
    select 
*
    from {{ ref('intermediate_share_history') }}

),

-- Step 1: Aggregate the data using Standard Deviation and annualize it
aggregated as (
    select
        stock_symbol,
        company_name,
        count(distinct trade_date) as total_trading_days,
        
        -- 1. Calculate standard deviation of daily percent changes
        round(stddev(percent_change)::numeric, 6) as daily_volatility_sd,
        
        -- 2. Annualize the daily volatility using the square root of 252 trading days
        round((stddev(percent_change) * sqrt(252))::numeric, 6) as annualized_volatility_sd,
        
        -- 3. Accompanying metrics for context
        round(avg(percent_change)::numeric, 6) as avg_daily_return_pct,
        max(percent_change) as max_single_day_gain_pct
    from history
    where percent_change is not null
    group by stock_symbol, company_name
    having count(distinct trade_date) > 1 -- Prevents math errors on single-day assets
),

-- Step 2: Rank the stocks by their Annualized Volatility (highest risk/reward first)
ranked as (
    select 
        *, 
        row_number() over (order by annualized_volatility_sd desc nulls last) as vol_rank 
    from aggregated
)

-- Step 3: Output the top 10 most volatile stocks
select
    stock_symbol,
    company_name,
    total_trading_days,
    daily_volatility_sd,
    annualized_volatility_sd,
    avg_daily_return_pct,
    max_single_day_gain_pct,
    vol_rank as rank
from ranked
where vol_rank <= 10
order by annualized_volatility_sd desc, rank