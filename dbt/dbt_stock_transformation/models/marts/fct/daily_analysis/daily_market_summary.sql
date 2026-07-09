{{ config(
	materialized='table'
) }}


select
	trade_date,
	count(distinct stock_symbol) as total_stocks,
	coalesce(sum(volume), 0) as total_volume,
	coalesce(sum(amount), 0) as total_amount,
	round(avg(percent_change)::numeric, 6) as avg_return,
	sum(case when percent_change > 0 then 1 else 0 end) as gainers,
	sum(case when percent_change < 0 then 1 else 0 end) as losers
from {{ ref('intermediate_liveShare') }}
group by trade_date

