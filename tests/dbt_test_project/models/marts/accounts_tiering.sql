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

account_totals as (
    select 
        account_id,
        sum(case when status = 'EXECUTED' then amount else 0 end) as total_executed_amount
    from transactions
    group by 1
),

final as (
    select
        accounts.account_id,
        accounts.account_holder,
        countries.country_code,
        countries.country_name,
        account_totals.total_executed_amount,
        case 
            when account_totals.total_executed_amount >= 500 then 'TIER_1'
            when account_totals.total_executed_amount >= 100 then 'TIER_2'
            else 'TIER_3'
        end as account_tier
    from accounts
    left join countries on accounts.country_id = countries.country_id
    left join account_totals on accounts.account_id = account_totals.account_id 
)
