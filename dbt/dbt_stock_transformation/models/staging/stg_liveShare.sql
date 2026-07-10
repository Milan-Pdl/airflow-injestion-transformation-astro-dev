{{ config(
    materialized='table',
) }}

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
from {{ source('stock', 'stock_market_data') }}
where as_of_date::date = (select max(as_of_date::date) from {{ source('stock', 'stock_market_data') }})