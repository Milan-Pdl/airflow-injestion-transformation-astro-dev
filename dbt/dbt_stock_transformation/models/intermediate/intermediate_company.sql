{{ config(
    materialized = 'table'
) }}

with raw as (

    select 
        
        *

    from {{ ref('stg_company') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by company_name
            order by company_name desc
        ) as rnk_name,

        row_number() over (
            partition by stock_symbol
            order by stock_symbol desc
        ) as rnk_symbol

    from raw

),

final as (

    select
        row_number() over (
            order by company_id
        )::bigint as company_id,
        company_id as source_company_id,
        company_name,
        stock_symbol,
        sector_id,
        sector_name,
        current_timestamp as loaded_at

    from ranked
    where rnk_name = 1 and rnk_symbol=1

)

select *
from final