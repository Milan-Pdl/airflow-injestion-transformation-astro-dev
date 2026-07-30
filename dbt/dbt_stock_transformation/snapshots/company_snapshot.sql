{% snapshot snp_company %}

{{
    config(
      target_schema='snapshots',
      unique_key='stock_symbol',
      strategy='check',
      check_cols=['company_name', 'sector_id', 'sector_name'],
      hard_deletes='invalidate'
    )
}}

select
    source_company_id,
    company_name,
    stock_symbol,
    sector_id,
    sector_name,
    loaded_at
from {{ ref('intermediate_company') }}

{% endsnapshot %}

-- Strategic Key Highlights:
-- unique_key: stock_symbol: Ensures each deduplicated stock symbol acts as the primary business entity key.

-- strategy: check: Monitors specific columns (company_name, sector_id, sector_name) for changes. When any of these values change between runs, dbt automatically expires the current record and inserts a new active row with updated timestamps.

-- hard_deletes: invalidate: If a company/symbol is removed from upstream staging data, dbt sets dbt_valid_to to the current timestamp rather than ignoring the deletion.