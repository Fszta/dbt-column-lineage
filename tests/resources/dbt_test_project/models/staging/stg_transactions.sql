with 

source as (
    select * from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        cast(id as integer) as transaction_id,
        cast(account_id as integer) as account_id,
        cast(amount as float) as amount,
        status,
        cast(transaction_date as date) as transaction_date
    from source
)

select * from renamed 