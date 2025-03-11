with 

transactions as (
    select * from {{ ref('stg_transactions') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

countries as (
    select * from {{ ref('stg_countries') }}
),

final as (
    select
        transactions.transaction_id,
        transactions.transaction_date,
        transactions.amount,
        transactions.status,
        accounts.account_id,
        accounts.account_holder,
        countries.country_id,
        countries.country_code,
        countries.country_name
    from transactions
    inner join accounts on transactions.account_id = accounts.account_id
    inner join countries on accounts.country_id = countries.country_id 
)

select * from final
