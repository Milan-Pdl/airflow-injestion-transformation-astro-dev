{{ config(
    materialized='incremental',
    incremental_strategy='append',
    incremental_unique_key='scraped_date'
) }}

select 
*,
current_timestamp as loaded_at 
from {{ ref('fct_weekly_broker_holding_analysis_summary') }}

{% if is_incremental() %}
    -- Keeps the lookback efficient
    where scraped_date::timestamp > (select max(scraped_date) from {{ this }})

{% endif %}