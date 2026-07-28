with expected_columns as (
    select unnest(array[
        'stock_symbol',
        'company_name',
        'no_of_transactions',
        'max_price',
        'min_price',
        'opening_price',
        'closing_price',
        'amount',
        'previous_closing',
        'difference_rs',
        'percent_change',
        'volume',
        'ltv',
        'as_of_date',
        'as_of_date_string',
        'trade_date',
        'data_type',
        'loaded_at'
    ]::text[]) as column_name
),
actual_columns as (
    select column_name
    from information_schema.columns
    where table_catalog = current_database()
      and table_schema = 'raw'
      and table_name = 'stock_market_data'
)
select e.column_name as missing_column
from expected_columns e
left join actual_columns a on e.column_name = a.column_name
where a.column_name is null
