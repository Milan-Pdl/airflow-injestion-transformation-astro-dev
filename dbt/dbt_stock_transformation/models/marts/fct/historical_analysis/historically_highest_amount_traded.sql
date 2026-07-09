{{ config(
    materialized='table'
) }}

with history as (
    select *
    from {{ ref('fct_stock_history') }}
),

aggregated as (
    select
        stock_symbol,
        company_name,
        count(distinct trade_date) as no_of_day,
        sum(coalesce(amount,0)) as total_amount,
        round(avg(coalesce(amount,0))::numeric, 2) as avg_amount,
        max(coalesce(amount,0)) as max_amount
    from history
    group by stock_symbol, company_name
),

ranked as (
    select *, row_number() over (order by total_amount desc nulls last) as amount_rank from aggregated
)

select
    stock_symbol,
    company_name,
    no_of_day,
    total_amount,
    avg_amount,
    max_amount,
    amount_rank as rank
from ranked
where amount_rank <= 10
order by total_amount desc, rank
