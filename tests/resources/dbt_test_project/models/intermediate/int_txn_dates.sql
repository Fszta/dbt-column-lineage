-- Carries transaction_date forward BY VALUE (a plain projection). It exists so the
-- row-set consumer below sits two hops from stg_transactions.transaction_date, exercising
-- transitive predicate propagation (the predicate is on THIS model's column, not the stg one).
select
    account_id,
    transaction_date,
    amount
from {{ ref('stg_transactions') }}
