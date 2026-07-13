-- Materialize this model as a standard table
{{ config(
    materialized='table'
) }}

select * 
from {{ ref('intermediate_company') }}
where stock_symbol not in (
    select stock_symbol 
    from {{ ref('intermediate_liveShare') }}
)