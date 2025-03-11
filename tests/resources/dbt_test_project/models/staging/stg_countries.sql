with 

source as (
    select * from {{ source('raw', 'countries') }}
),

renamed as (
    select
        id as country_id,
        code as country_code,
        name as country_name
    from source
)

select * from renamed 