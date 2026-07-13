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
    
    -- New Columns --
    -- 1. Extracts the ISO week number (1-53) of the year
    date_part('week', as_of_date::timestamp) as no_of_weeks,
    
    -- 2. Identifies the specific trading day of that week.
    -- In standard SQL/Postgres, date_part('dow', ...) returns 0 for Sunday, 1 for Monday... 5 for Friday.
    case date_part('dow', as_of_date::timestamp)
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        else 'Weekend/Holiday'
    end as no_of_days,

    current_timestamp as loaded_at
from {{ ref('intermediate_liveShare') }}

{% if is_incremental() %}
    -- Keeps the lookback efficient
    where as_of_date::timestamp > (select max(as_of_date) from {{ this }})
{% endif %}