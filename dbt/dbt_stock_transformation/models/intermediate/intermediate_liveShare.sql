{{ config(
    materialized='table'
) }}

with cleaned as (

    select
        *,
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['stock_symbol', 'as_of_date']) }} as stock_date_key,
        
        -- Safely cast date/timestamp fields once
        nullif(as_of_date, '')::timestamp as as_of_date_parsed,
        nullif(trade_date, '')::date as trade_date_parsed,

        -- Deduplicate records
        row_number() over (
            partition by stock_symbol, as_of_date
            order by loaded_at desc 
        ) as rnk_id
    from {{ ref('stg_liveShare') }}

)

select
    stock_date_key,
    stock_symbol::text as stock_symbol,
    company_name::text as company_name,
    nullif(no_of_transactions, '')::bigint as no_of_transactions,
    nullif(max_price, '')::numeric as max_price,
    nullif(min_price, '')::numeric as min_price,
    nullif(opening_price, '')::numeric as opening_price,
    nullif(closing_price, '')::numeric as closing_price,
    nullif(amount, '')::numeric as amount,
    nullif(previous_closing, '')::numeric as previous_closing,
    nullif(difference_rs, '')::numeric as difference_rs,
    nullif(percent_change, '')::numeric as percent_change,
    nullif(volume, '')::bigint as volume,
    nullif(ltv, '')::numeric as ltv,
    as_of_date_parsed as as_of_date,
    as_of_date_string::text as as_of_date_string,
    trade_date_parsed as trade_date,
    current_timestamp as loaded_at
from cleaned
where rnk_id = 1
  and as_of_date_parsed = (select max(as_of_date_parsed) from cleaned)