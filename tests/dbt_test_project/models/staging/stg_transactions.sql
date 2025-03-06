with 

source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        id as transaction_id,
        account_id,
        amount,
        status,
        transaction_date
    from source
)

select * from renamed 