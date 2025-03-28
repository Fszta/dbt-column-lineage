with 

transactions_enriched as (
    select * from {{ ref('int_transactions_enriched') }}
),

monthly_metrics as (
    select
        account_id,
        account_holder,
        country_id,
        transaction_month,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(case when status = 'EXECUTED' then amount else 0 end) as executed_amount,
        sum(case when status = 'PENDING' then amount else 0 end) as pending_amount,
        sum(case when amount_category = 'HIGH' then 1 else 0 end) as high_value_transactions
    from transactions_enriched
    group by 1, 2, 3, 4
)

select * from monthly_metrics 