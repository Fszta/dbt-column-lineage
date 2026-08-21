"""End-to-end provable-break classification against the real fixture artifacts.

Constructs a genuine two-manifest diff: the head drops a whole tested model (removed from
BOTH the manifest and the catalog, so the catalog-first registry can't resurrect it). The
change removes columns that carry dbt tests in base, so the classifier must return
BREAK-TEST findings and the verdict must be BLOCK.
"""

import json
from pathlib import Path

from dbt_column_lineage.lineage.changeset import ChangesetBuilder
from dbt_column_lineage.lineage.service import LineageService
from dbt_column_lineage.lineage.verdict import classify_provable_breaks, decide_verdict

# A staging model that carries not_null/unique tests in the fixture (see the tests added to
# models/staging/models.yml) and whose columns are therefore provable-break candidates.
_DROPPED_MODEL = "stg_transactions"


def _write_head_without_model(dbt_artifacts, tmp_path: Path) -> tuple:
    manifest = json.loads(Path(dbt_artifacts["manifest_path"]).read_text())
    catalog = json.loads(Path(dbt_artifacts["catalog_path"]).read_text())

    # Drop the model node from the manifest.
    m_nodes = manifest.get("nodes", {})
    for uid in [u for u in m_nodes if m_nodes[u].get("name") == _DROPPED_MODEL]:
        del m_nodes[uid]
    # Drop it from the catalog too, else the catalog-first registry re-creates it in head.
    c_nodes = catalog.get("nodes", {})
    for uid in [u for u in c_nodes if u.split(".")[-1] == _DROPPED_MODEL]:
        del c_nodes[uid]

    head_manifest = tmp_path / "manifest.json"
    head_catalog = tmp_path / "catalog.json"
    head_manifest.write_text(json.dumps(manifest))
    head_catalog.write_text(json.dumps(catalog))
    return head_catalog, head_manifest


def test_dropping_a_tested_model_yields_block(dbt_artifacts, tmp_path):
    base = LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))
    head_catalog, head_manifest = _write_head_without_model(dbt_artifacts, tmp_path)
    head = LineageService(head_catalog, head_manifest)

    # Sanity: the model really is gone from head, present in base.
    assert _DROPPED_MODEL in base.registry.get_models()
    assert _DROPPED_MODEL not in head.registry.get_models()

    changes = ChangesetBuilder(base.registry, head.registry).build()
    removed_cols = {c.column for c in changes if c.model == _DROPPED_MODEL}
    assert removed_cols, "expected the dropped model's columns to be reported removed"

    breaks = classify_provable_breaks(changes, head.registry, base.registry)

    assert breaks, "dropping a tested model must produce provable test breaks"
    assert all(b.break_kind == "break_test" for b in breaks)
    assert {b.test_name for b in breaks} & {"not_null", "unique"}
    # Every break points at the model we removed.
    assert all(b.change_model == _DROPPED_MODEL for b in breaks)

    summary = {"affected_exposures": 0, "critical_count": 0, "filter_count": 0}
    assert decide_verdict(breaks, summary) == "block"


def test_no_break_when_nothing_removed(dbt_artifacts):
    """Diffing the fixture against itself removes nothing → no provable breaks, not BLOCK."""
    svc = LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))
    changes = ChangesetBuilder(svc.registry, svc.registry).build()
    breaks = classify_provable_breaks(changes, svc.registry, svc.registry)
    assert breaks == []


def test_filter_only_changeset_is_review_not_safe(dbt_artifacts):
    """A row-set (filter) change must carry filter_count in the changeset summary so the
    JSON verdict agrees with the markdown banner (regression: filter_count was missing)."""
    from dbt_column_lineage.lineage.changeset import ChangeKind, ColumnChange

    svc = LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))
    # transactions.status is used ONLY in a WHERE in flagged_transaction_metrics (row-set).
    changes = [ColumnChange("transactions", "status", ChangeKind.LOGIC_CHANGED)]
    aggregated = svc.get_changeset_impact(changes)

    summary = aggregated["summary"]
    assert "filter_count" in summary, "changeset summary must expose filter_count"
    assert summary["filter_count"] >= 1
    # No provable break (logic change), but a row-set shift → REVIEW, never SAFE.
    assert decide_verdict([], summary) == "review"
