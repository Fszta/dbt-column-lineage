-- Transitive row-set (QUALIFY) case: transaction_date reaches here only through
-- int_txn_dates (by value), then is used ONLY in a window ORDER BY inside a QUALIFY to pick
-- the latest transaction per account — it is never projected. So it's a row-set dependency
-- two hops from stg_transactions.transaction_date, invisible to column-value lineage and
-- missed unless predicate dependents are propagated along the value chain.
with txns as (
    select * from {{ ref('int_txn_dates') }}
),

latest as (
    select
        account_id,
        amount as latest_amount
    from txns
    qualify row_number() over (partition by account_id order by transaction_date desc) = 1
)

select * from latest
