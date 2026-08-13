"""Integration tests for the diff-driven ``impact`` command.

Uses the real dbt test-project artifacts as *head* and synthesizes a mutated
*base* (retyped / removed columns, changed compiled SQL) in a temp dir, then
drives the CLI end-to-end through the ChangesetBuilder + aggregation + output.
"""

import copy
import json

import pytest
from click.testing import CliRunner

from dbt_column_lineage.cli.main import impact


def _load(path):
    with open(path) as f:
        return json.load(f)


def _find_catalog_node(catalog, model_name):
    for node_id, node in catalog["nodes"].items():
        if node.get("metadata", {}).get("name", "").lower() == model_name:
            return node_id
    raise AssertionError(f"{model_name} not in catalog")


@pytest.fixture
def base_artifacts(dbt_artifacts, tmp_path):
    """A mutated copy of the head artifacts to act as the diff base."""
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))

    # In the BASE, stg_accounts.account_id has a different type and there is an
    # extra column that head no longer has -> head shows type_changed + removed.
    node_id = _find_catalog_node(catalog, "stg_accounts")
    catalog["nodes"][node_id]["columns"]["account_id"]["type"] = "DECIMAL(38,2)"
    catalog["nodes"][node_id]["columns"]["legacy_col"] = {
        "name": "legacy_col",
        "type": "TEXT",
    }

    base_catalog = tmp_path / "catalog.json"
    base_manifest = tmp_path / "manifest.json"
    base_catalog.write_text(json.dumps(catalog))
    base_manifest.write_text(json.dumps(manifest))
    return {"catalog": str(base_catalog), "manifest": str(base_manifest)}


def _run_impact(args):
    return CliRunner().invoke(impact, args)


def test_impact_requires_a_change_source(dbt_artifacts):
    result = _run_impact(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
        ]
    )
    assert result.exit_code == 1
    assert "base-manifest" in result.output or "git-base" in result.output


def test_impact_two_manifest_json_report(dbt_artifacts, base_artifacts):
    result = _run_impact(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            base_artifacts["manifest"],
            "--base-catalog",
            base_artifacts["catalog"],
            "--format",
            "json",
        ]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    changeset = payload["changeset"]
    assert changeset["source"] == "two-manifest"
    assert changeset["by_kind"].get("type_changed", 0) >= 1
    # legacy_col existed in base only -> removed
    assert changeset["by_kind"].get("removed", 0) >= 1

    # Superset of the single-column impact block.
    for key in ("summary", "affected_models", "affected_columns", "affected_exposures"):
        assert key in payload
    for metric in (
        "affected_models",
        "affected_columns",
        "affected_exposures",
        "critical_count",
        "low_impact_count",
        "unresolved_changes",
    ):
        assert metric in payload["summary"]

    # account_id is consumed widely downstream, so retyping it must have impact.
    assert payload["summary"]["affected_models"] >= 1
    assert "by_change" in payload


def test_impact_two_manifest_markdown_default(dbt_artifacts, base_artifacts):
    result = _run_impact(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            base_artifacts["manifest"],
            "--base-catalog",
            base_artifacts["catalog"],
        ]
    )
    assert result.exit_code == 0, result.output
    assert "Column-level impact" in result.output
    assert "Affected columns" in result.output


def test_impact_identical_manifests_report_no_change(dbt_artifacts):
    result = _run_impact(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            str(dbt_artifacts["manifest_path"]),
            "--base-catalog",
            str(dbt_artifacts["catalog_path"]),
        ]
    )
    assert result.exit_code == 0, result.output
    assert "No column changes detected" in result.output
