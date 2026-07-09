-- Fact table for stock history

{{ config(
    materialized='incremental',
    incremental_strategy = 'append',
    unique_key='stock_date_key'
) }}

select
    stock_date_key,
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
from {{ ref('intermediate_liveShare') }}

{% if is_incremental() %}
    -- Keeps the lookback efficient
    where as_of_date::timestamp > (select max(as_of_date) from {{ this }})
{% endif %}