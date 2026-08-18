-- A filter/join-only consumer: it uses `transactions.status` ONLY in a WHERE predicate
-- and never projects it. Its output (a count) changes when status logic changes, so it is
-- a row-set (filter) dependency of transactions.status — invisible to column-value lineage.
with

transactions as (
    select * from {{ ref('transactions') }}
),

final as (
    select
        transactions.account_id,
        count(*) as flagged_transaction_count
    from transactions
    where transactions.status = 'flagged'
    group by 1
)

select * from final
