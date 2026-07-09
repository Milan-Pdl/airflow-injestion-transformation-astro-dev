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
		amount,
		closing_price,
		percent_change,
		difference_rs,
		row_number() over (order by amount desc nulls last) as amount_rank
	from latest
	where amount is not null
)

select
	stock_symbol,
	company_name,
	trade_date,
	amount,
	closing_price,
	percent_change,
	difference_rs as change_rs,
	amount_rank as rank
from ranked
where amount_rank <= 10
order by trade_date desc, rank

