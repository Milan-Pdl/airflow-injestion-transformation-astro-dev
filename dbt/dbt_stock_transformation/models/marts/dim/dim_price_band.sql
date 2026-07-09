{{ config(materialized='table') }}

select
    1 as price_band_key, 'Penny Stock' as band_name, 0.00 as min_price, 10.00 as max_price
union all
select 2, 'Low Tier', 10.01, 50.00
union all
select 3, 'Mid Tier', 50.01, 200.00
union all
select 4, 'High Tier', 200.01, 1000.00
union all
select 5, 'Ultra High Tier', 1000.01, 9999999.99
