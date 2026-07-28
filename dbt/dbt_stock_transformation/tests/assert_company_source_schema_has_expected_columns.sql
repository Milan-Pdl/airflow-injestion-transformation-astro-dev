-- checking if there is any schema drift comming
--eg like change in cols

with expected_columns as (
    select unnest(array[
        'company_id',
        'company_name',
        'stock_symbol',
        'sector_id',
        'sector_name'
    ]::text[]) as column_name
),
actual_columns as (
    select column_name
    from information_schema.columns
    where table_catalog = current_database()
      and table_schema = 'raw'
      and table_name = 'company'
)
select e.column_name as missing_column
from expected_columns e
left join actual_columns a on e.column_name = a.column_name
where a.column_name is null
