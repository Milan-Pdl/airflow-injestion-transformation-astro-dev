{{ config(
	materialized='table'
) }}

    with latest as (
        select *
        from {{ ref('intermediate_liveShare') }}
        where trade_date = (
            select max(trade_date) from {{ ref('intermediate_liveShare') }}
        )
    ),

    ranked as (
        select
            stock_symbol,
            company_name,
            trade_date,
            volume,
            closing_price,
            percent_change,
            difference_rs,
            row_number() over (order by volume desc nulls last) as vol_rank
        from latest
        where volume is not null
    )

    select
        stock_symbol,
        company_name,
        trade_date,
        volume,
        closing_price,
        percent_change,
        difference_rs as change_rs,
        vol_rank as rank
    from ranked
    where vol_rank <= 10
    order by trade_date desc, rank

