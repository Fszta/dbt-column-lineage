with 

monthly_metrics as (
    select * from {{ ref('int_monthly_account_metrics') }}
),

countries as (
    select * from {{ ref('stg_countries') }}
),

account_totals as (
    select 
        account_id,
        account_holder,
        country_id,
        sum(executed_amount) as total_executed_amount,
        avg(transaction_count) as avg_monthly_transactions,
        sum(high_value_transactions) as total_high_value_transactions
    from monthly_metrics
    group by 1, 2, 3
),

final as (
    select
        account_totals.*,
        countries.country_code,
        countries.country_name,
        case 
            when total_executed_amount >= 500 and total_high_value_transactions >= 3 then 'PLATINUM'
            when total_executed_amount >= 500 then 'GOLD'
            when total_executed_amount >= 100 then 'SILVER'
            else 'BRONZE'
        end as account_tier
    from account_totals
    left join countries on account_totals.country_id = countries.country_id
)

select * from final
