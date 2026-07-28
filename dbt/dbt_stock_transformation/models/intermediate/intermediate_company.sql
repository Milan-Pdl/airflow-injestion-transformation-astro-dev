{{ config(
    materialized = 'table'
) }}

with raw as (

    select
        company_id as source_company_id,
        company_name,
        stock_symbol,
        sector_id,
        sector_name,
        current_timestamp as loaded_at
    from {{ ref('stg_company') }}
    where sector_name is not null
      and trim(sector_name) <> ''
      and lower(trim(sector_name)) not in ('corporate debenture', 'mutual fund')

),

ranked as (

    select
        *,
        -- Deduplicate strictly by the API's company identifier
        row_number() over (
            partition by source_company_id
            order by company_name desc -- pick the row variation you want to keep
        ) as rnk_id

    from raw

),

final as (

    select
        -- Generates a clean internal surrogate key for the SCD Type 2 version
        row_number() over (
            order by source_company_id
        )::bigint as company_id,
        source_company_id,
        company_name,
        stock_symbol,
        sector_id,
        sector_name,
        loaded_at as valid_from,
        null::timestamp as valid_to,
        true as is_current,
        loaded_at

    from ranked
    where rnk_id = 1

)

select *
from final