{{ config(
    materialized = 'table'
) }}

with raw as (

    select * from {{ ref('stg_company') }}

),

ranked as (

    select
        *,
        -- Deduplicate strictly by the API's company identifier
        row_number() over (
            partition by company_id
            order by company_name desc -- pick the row variation you want to keep
        ) as rnk_id

    from raw

),

final as (

    select
        -- Generates a clean internal surrogate key
        row_number() over (
            order by company_id
        )::bigint as company_id,    
        company_name,
        stock_symbol,
        sector_id,
        sector_name,
        current_timestamp as loaded_at

    from ranked
    where rnk_id = 1

)

select *
from final