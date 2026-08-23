"""Integration tests for the Impact Report absorptions, end-to-end on real dbt artifacts.

Two cheap, high-value additions to the impact payload (see the "always-on baseline"
proposal): every affected exposure carries the dbt ``owner`` who must sign off, and the
summary gains a ``by_mechanism`` breakdown — the machine-readable twin of the markdown's
derived-recompute / row-set-filter / pass-through split, so an agent can reason over *how*
a change propagates, not just how many nodes it reaches.
"""

from pathlib import Path

from parrant.lineage.service import LineageService


def _service(dbt_artifacts) -> LineageService:
    return LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))


def test_affected_exposures_carry_owner(dbt_artifacts):
    impact = _service(dbt_artifacts).get_column_impact("transactions", "account_id")

    exposures = impact["affected_exposures"]
    assert exposures, "expected transactions.account_id to reach exposures"
    # Every exposure exposes an owner key; the fixture declares owner.name on each.
    for exposure in exposures:
        assert "owner" in exposure
    names = {(e.get("owner") or {}).get("name") for e in exposures}
    assert "Platform Team" in names or "Analytics Team" in names


def test_summary_has_mechanism_breakdown_that_totals_columns(dbt_artifacts):
    impact = _service(dbt_artifacts).get_column_impact("transactions", "account_id")

    by_mechanism = impact["summary"].get("by_mechanism")
    assert isinstance(by_mechanism, dict) and by_mechanism
    # Labels are the plain-language mechanism names, never raw transformation types.
    assert set(by_mechanism).issubset(
        {"derived_recompute", "rowset_filter", "renamed_passthrough", "direct_passthrough"}
    )
    # The breakdown partitions affected_columns exactly — nothing dropped or double-counted.
    assert sum(by_mechanism.values()) == len(impact["affected_columns"])
