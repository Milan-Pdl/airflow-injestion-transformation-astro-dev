{{ config(materialized='table') }}

select distinct
    md5(coalesce(as_of_date_string, 'UNKNOWN')) as market_key,
    as_of_date_string as market_session_label
from {{ ref('intermediate_liveShare') }}