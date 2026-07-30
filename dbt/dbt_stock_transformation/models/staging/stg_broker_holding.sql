{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with source as (

    select * from {{ source('raw', 'broker_holding') }}

),

renamed as (

    select
        broker::varchar as broker_id,
        symbol::varchar as symbol,
        type::varchar as transaction_type,
        
        -- Safely convert quantity into positive integer
        abs(nullif(quantity::text, '')::numeric)::bigint as quantity,
        
        -- Safely convert scraped_at text string to timestamp and date
        nullif(scraped_at, '')::timestamp as scraped_at,
        nullif(scraped_at, '')::date as scraped_date,
        
        period_range::varchar as period_range

    from source

)

select * 
from renamed

{% if is_incremental() %}
  -- Filter after timestamps are cleanly parsed
  -- Using >= ensures same-second or same-day batch loads aren't skipped
  where scraped_date = (select coalesce(max(scraped_date), '1900-01-01'::timestamp) from renamed)
{% endif %}