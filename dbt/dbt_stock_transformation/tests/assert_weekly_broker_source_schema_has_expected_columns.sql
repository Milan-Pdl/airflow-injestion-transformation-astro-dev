with expected_columns as (
    select unnest(array[
        'broker',
        'quantity',
        'type',
        'symbol',
        'period_range',
        'scraped_at'
    ]::text[]) as column_name
),
actual_columns as (
    select column_name
    from information_schema.columns
    where table_catalog = current_database()
      and table_schema = 'raw'
      and table_name = 'broker_holding'
)
select e.column_name as missing_column
from expected_columns e
left join actual_columns a on e.column_name = a.column_name
where a.column_name is null
