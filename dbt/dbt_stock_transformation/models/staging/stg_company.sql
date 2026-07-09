{{ config(materialized='table') }}

select
    company_id,
    company_name,
    stock_symbol,
    sector_id,
    sector_name
from {{ source('stock', 'company') }}
