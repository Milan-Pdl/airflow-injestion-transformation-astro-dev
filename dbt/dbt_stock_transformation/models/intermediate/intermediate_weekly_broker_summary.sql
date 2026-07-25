{{ config(
    materialized='table'
) }}

with staged as (

    select * from {{ ref('stg_broker_holding') }}

),

-- Take only the latest scrape run for today's (max) scraped_date
latest_scrape as (

    select
        *,
        row_number() over (
            partition by scraped_date, symbol, broker_id, transaction_type, period_range
            order by scraped_at desc
        ) as rn
    from staged
    where scraped_date = (select max(scraped_date) from staged)

),

deduped as (

    select 
    *,
    current_timestamp as loaded_at 
    from latest_scrape 
    where rn = 1

)

select
    scraped_date,
    symbol,
    broker_id,
    period_range,
    sum(case when trim(lower(transaction_type)) = 'buy' then coalesce(quantity, 0) else 0 end) as buy_qty,
    sum(case when trim(lower(transaction_type)) = 'sell' then coalesce(quantity, 0) else 0 end) as sell_qty,
    sum(
        case 
            when trim(lower(transaction_type)) = 'buy' then coalesce(quantity, 0) 
            when trim(lower(transaction_type)) = 'sell' then -coalesce(quantity, 0)
            else 0 
        end
    ) as net_qty
from deduped
group by 
    scraped_date, 
    symbol, 
    broker_id,
    period_range