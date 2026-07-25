{{ config(
    materialized='table'
) }}

with broker_summary as (

    select * from {{ ref('intermediate_weekly_broker_summary') }}

),

-- Aggregate stock-level totals
stock_totals as (

    select
        scraped_date,
        symbol,
        period_range,
        sum(buy_qty) as total_buy_qty,
        sum(sell_qty) as total_sell_qty,
        sum(buy_qty + sell_qty) as total_traded_vol,
        current_timestamp as caluculated_at
    from broker_summary
    group by 1, 2, 3

),

-- Rank top 3 buying and selling brokers per stock
broker_ranks as (

    select
        scraped_date,
        symbol,
        broker_id,
        period_range,
        net_qty,
        row_number() over (partition by scraped_date, symbol order by net_qty desc) as top_buyer_rank,
        row_number() over (partition by scraped_date, symbol order by net_qty asc) as top_dumper_rank
    from broker_summary

),

top_brokers_agg as (

    select
        scraped_date,
        symbol,
        period_range,
        max(case when top_buyer_rank = 1 then broker_id || ' (' || net_qty || ')' end) as rank_1_buyer,
        max(case when top_buyer_rank = 2 then broker_id || ' (' || net_qty || ')' end) as rank_2_buyer,
        max(case when top_buyer_rank = 3 then broker_id || ' (' || net_qty || ')' end) as rank_3_buyer,

        max(case when top_dumper_rank = 1 then broker_id || ' (' || abs(net_qty) || ')' end) as rank_1_dumper,
        max(case when top_dumper_rank = 2 then broker_id || ' (' || abs(net_qty) || ')' end) as rank_2_dumper,
        max(case when top_dumper_rank = 3 then broker_id || ' (' || abs(net_qty) || ')' end) as rank_3_dumper
    from broker_ranks
    group by 1, 2, 3
)

select
    t.scraped_date,
    t.period_range,
    t.symbol,
    t.total_buy_qty,
    t.total_sell_qty,
    round(100.0 * t.total_buy_qty / nullif(t.total_traded_vol, 0), 2) as weekly_buy_pressure_pct,
    round(100.0 * t.total_sell_qty / nullif(t.total_traded_vol, 0), 2) as weekly_sell_pressure_pct,

    -- Top Ranked Brokers
    b.rank_1_buyer,
    b.rank_2_buyer,
    b.rank_3_buyer,
    b.rank_1_dumper,
    b.rank_2_dumper,
    b.rank_3_dumper,

    case 
        when (100.0 * t.total_buy_qty / nullif(t.total_traded_vol, 0)) >= 60.0 then 'BULLISH'
        when (100.0 * t.total_sell_qty / nullif(t.total_traded_vol, 0)) >= 60.0 then 'BEARISH'
        else 'NEUTRAL'
    end as sentiment_status

from stock_totals t
left join top_brokers_agg b 
    on t.scraped_date = b.scraped_date 
   and t.symbol = b.symbol
   and t.period_range = b.period_range