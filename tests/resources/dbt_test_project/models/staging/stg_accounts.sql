with 

source as (
    select * from {{ source('raw', 'accounts') }}
),

renamed as (
    select
        cast(id as integer) as account_id,
        holder as account_holder,
        cast(country_id as integer) as country_id
    from source
)

select * from renamed 