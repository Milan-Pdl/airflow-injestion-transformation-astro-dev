{{ config(
    materialized='table'
) }}

with raw as (
    select
        company_sk,
        business_company_id,
        company_name,
        stock_symbol,
        sector_id,
        sector_name
    from {{ ref('intermediate_company') }}
),

live_share as (
    select
        stock_symbol
    from {{ ref('intermediate_liveShare') }}
)

select
    company_sk,
    business_company_id,
    company_name,
    stock_symbol,
    sector_id,
    sector_name
from raw
where stock_symbol not in (
    select stock_symbol
    from live_share
)