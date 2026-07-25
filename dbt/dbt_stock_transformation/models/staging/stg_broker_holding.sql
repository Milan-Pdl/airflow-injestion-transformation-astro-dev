with source as (

    select * from {{ source('raw', 'broker_holding') }}

),

renamed as (

    select
        broker::varchar as broker_id,
        symbol::varchar as symbol,
        type::varchar as transaction_type,
        
        -- Convert string/numeric quantity into positive integer
        abs(quantity)::bigint as quantity,
        
        -- Convert scraped_at text string to Postgres timestamp and date
        scraped_at::timestamp as scraped_at,
        scraped_at::date as scraped_date,
        
        period_range::varchar as period_range

    from source

)

select * from renamed