{{ config(
    materialized='table'
) }}

with ranked as (

    select
        stock_symbol,
        company_name,
        trade_date,
        amount,
        closing_price,
        percent_change,
        difference_rs as change_rs,
        row_number() over (order by amount desc nulls last) as rank
    from {{ ref('intermediate_liveShare') }}
    where amount is not null

)

select
    stock_symbol,
    company_name,
    trade_date,
    amount,
    closing_price,
    percent_change,
    change_rs,
    rank
from ranked
where rank <= 10
order by rank