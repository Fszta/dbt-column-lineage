with

-- A chain of star-passthrough CTEs: the derived column below reads columns that
-- flow through two intermediate CTEs before reaching the base model. This mirrors
-- the real-world "select * from a; select * from b; select <derived> from c" idiom
-- and exercises transitive CTE-alias resolution (a CTE must never leak into lineage
-- as if it were an upstream model).
passthrough_one as (
    select * from {{ ref('stg_transactions') }}
),

passthrough_two as (
    select * from passthrough_one
),

flagged as (
    select
        transaction_id,
        case
            when status = 'EXECUTED' then amount
            else 0
        end as executed_amount
    from passthrough_two
)

select * from flagged
