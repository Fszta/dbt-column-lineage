"""Transitive row-set (predicate) impact, end-to-end on real dbt artifacts.

A column can reach a downstream model purely as a row-set dependency several hops away: its
value flows through intermediate models and is then used ONLY in a WHERE / JOIN / QUALIFY
(e.g. a window ORDER BY that picks "the latest row"). Column-value lineage never projects it,
so the consumer is invisible unless predicate dependents are propagated along the value chain.

Fixture chain (see the models):
  stg_transactions.transaction_date  --value-->  int_txn_dates.transaction_date
    --QUALIFY row_number() ORDER BY transaction_date-->  latest_txn_per_account

Querying the *upstream* stg column must surface latest_txn_per_account as a row-set impact,
even though the predicate is on int_txn_dates' column, not the stg one.
"""

from pathlib import Path

from dbt_column_lineage.lineage.service import LineageService

_CONSUMER = "latest_txn_per_account"


def _impact(dbt_artifacts):
    svc = LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))
    return svc.get_column_impact("stg_transactions", "transaction_date")


def test_transitive_qualify_consumer_is_a_rowset_impact(dbt_artifacts):
    impact = _impact(dbt_artifacts)

    reached = {m["name"] for m in impact["affected_models"]}
    assert _CONSUMER in reached, (
        "a QUALIFY consumer two hops downstream must be reported as a row-set impact of the "
        "upstream column — predicate dependents propagate along the value chain"
    )

    rows = [c for c in impact["affected_columns"] if c["model"] == _CONSUMER]
    assert rows, f"expected a row-set impact row for {_CONSUMER}"
    row = rows[0]
    assert row["severity"] == "filter"
    assert row["transformation_type"] == "filter"
    assert row["column"] == "(row-set)"
    # The 'why' is the predicate the value-reached column appears in (the window ORDER BY).
    assert "transaction_date" in (row["sql_expression"] or "").lower()
    assert impact["summary"]["filter_count"] >= 1


def test_direct_value_consumer_is_not_double_counted_as_rowset(dbt_artifacts):
    """int_txn_dates projects transaction_date by value — it must be a value impact, not a
    row-set one (guards against the propagation flagging value consumers as filters)."""
    impact = _impact(dbt_artifacts)
    int_rows = [c for c in impact["affected_columns"] if c["model"] == "int_txn_dates"]
    assert int_rows, "int_txn_dates should be a value impact"
    assert all(r["severity"] != "filter" for r in int_rows)
