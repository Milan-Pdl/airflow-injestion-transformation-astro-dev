{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with raw as(
select
    -- Generating the surrogate key
    stock_symbol,
    company_name,
    no_of_transactions,
    max_price,
    min_price,
    opening_price,
    closing_price,
    amount,
    previous_closing,
    difference_rs,
    percent_change,
    volume,
    ltv,
    as_of_date,
    as_of_date_string,
    trade_date,
    current_timestamp as loaded_at
from {{ source('stock', 'stock_market_data') }}
where trade_date::date = (select max(trade_date::date) from {{ source('stock', 'stock_market_data') }})
),

dedup as(
select 
    stock_symbol,
    company_name,
    no_of_transactions,
    max_price,
    min_price,
    opening_price,
    closing_price,
    amount,
    previous_closing,
    difference_rs,
    percent_change,
    volume,
    ltv,
    as_of_date,
    as_of_date_string,
    trade_date,
    loaded_at ,
    rank() over (partition by stock_symbol,trade_date order by loaded_at desc) as rank
from raw
)

select 

    stock_symbol,
    company_name,
    no_of_transactions,
    max_price,
    min_price,
    opening_price,
    closing_price,
    amount,
    previous_closing,
    difference_rs,
    percent_change,
    volume,
    ltv,
    as_of_date,
    as_of_date_string,
    trade_date,
    current_timestamp as loaded_at 

from dedup where rank = 1