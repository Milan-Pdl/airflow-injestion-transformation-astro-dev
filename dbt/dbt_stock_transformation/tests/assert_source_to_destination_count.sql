with source_count as (
    select count(*) as row_count 
    from {{ ref('stg_liveShare') }}
    where loaded_at = (select max(loaded_at) from {{ ref('stg_liveShare') }})
),

destination_count as (
    select count(*) as row_count 
    from {{ ref('intermediate_liveShare') }}
)

select 
    source_count.row_count as source_rows,
    destination_count.row_count as dest_rows
from source_count
cross join destination_count
where source_count.row_count != destination_count.row_count