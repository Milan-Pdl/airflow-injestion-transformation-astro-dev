{{ config(
    materialized='table'
) }}

select 
    stock_symbol as stock_symbol,
    company_name as company_name
from {{ ref('intermediate_liveShare') }}
where stock_symbol is not null
group by stock_symbol,company_name
