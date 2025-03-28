with 

stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

stg_accounts as (
    select * from {{ ref('stg_accounts') }}
),

enriched as (
    select
        stg_transactions.*,  -- This should maintain column lineage
        stg_accounts.account_holder,
        stg_accounts.country_id,
        date_trunc('month', transaction_date) as transaction_month,
        case 
            when amount >= 1000 then 'HIGH'
            when amount >= 100 then 'MEDIUM'
            else 'LOW'
        end as amount_category
    from stg_transactions
    inner join stg_accounts on stg_transactions.account_id = stg_accounts.account_id
)

select * from enriched 