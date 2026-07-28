-- checks that the latest-day row count matches between the intermediate and history fact models.
with source_daily as (
    select count(*) as row_count
    from "Stock"."intermediate"."intermediate_liveShare"
    where trade_date = (
        select max(trade_date)
        from "Stock"."intermediate"."intermediate_liveShare"
    )
),

destination_daily as (
    select count(*) as row_count
    from "Stock"."intermediate"."intermediate_share_history"
    where trade_date = (
        select max(trade_date)
        from "Stock"."intermediate"."intermediate_share_history"
    )
)

select
    source_daily.row_count as source_rows,
    destination_daily.row_count as dest_rows
from source_daily
cross join destination_daily
where source_daily.row_count != destination_daily.row_count