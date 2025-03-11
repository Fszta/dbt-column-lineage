with 

source as (
    select * from {{ source('raw', 'accounts') }}
),

renamed as (
    select
        id as account_id,
        holder as account_holder,
        country_id
    from source
)

select * from renamed 