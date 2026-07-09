{{ config(
    materialized='table'
) }}

with date_series as (
    select generate_series(
        '2020-01-01'::date, 
        '2030-12-31'::date, 
        '1 day'::interval
    )::date as date_day
)
select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day as trade_date,
    extract(year from date_day)::integer as year,
    extract(quarter from date_day)::integer as quarter,
    extract(month from date_day)::integer as month,
    to_char(date_day, 'Month') as month_name,
    extract(isodow from date_day)::integer as day_of_week,
    to_char(date_day, 'Day') as day_name,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from date_series