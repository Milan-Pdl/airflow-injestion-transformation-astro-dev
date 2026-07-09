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
		previous_closing,
		closing_price,
		percent_change,
		difference_rs,
		volume,
		row_number() over (order by percent_change desc nulls last) as gain_rank,
		row_number() over (order by percent_change asc nulls last) as loss_rank
	from latest
	where percent_change is not null
)

select
	stock_symbol,
	company_name,
	trade_date,
	previous_closing,
	closing_price,
	percent_change,
	difference_rs as change_rs,
	volume,
	gain_rank as rank,
	'gainer' as side
from ranked
where gain_rank <= 10

union all

select
	stock_symbol,
	company_name,
	trade_date,
	previous_closing,
	closing_price,
	percent_change,
	difference_rs as change_rs,
	volume,
	loss_rank as rank,
	'loser' as side
from ranked
where loss_rank <= 10

order by trade_date desc, side, rank

