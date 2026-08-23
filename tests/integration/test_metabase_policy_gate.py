"""+ killer integration test — a breaking dbt column change that reaches an EXECUTIVE
Metabase dashboard trips the policy gate.

Wires the whole cross-boundary path against the bundled dbt_test_project:

    stg_accounts.account_holder  --(dbt reach)-->  transactions.account_holder
        --(metabase_lineage.json: card 501)-->  dashboard 55 (meta.tier=executive)

Proves:
  * a BREAKING change reaching the executive dashboard BLOCKS (exit 1 under --fail-on policy),
  * an EQUIVALENT change reaching the SAME dashboard does NOT block (breaking-aware),
  * the same breaking change reaching only a NON-executive dashboard does NOT block,
  * the dashboard surfaces as an exposure-kind reached object carrying its artifact meta,
  * everything is backward-compatible when --metabase is absent.
"""

import copy
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_column_lineage.cli.main import impact
from dbt_column_lineage.lineage.changeset import ChangeKind, ColumnChange
from dbt_column_lineage.lineage.policy import evaluate_policy, load_policy
from dbt_column_lineage.lineage.service import LineageService
from dbt_column_lineage.metabase.artifact import load_metabase_lineage
from dbt_column_lineage.metabase.join import build_relation_index
from dbt_column_lineage.metabase.reach import MetabaseReach
from dbt_column_lineage.models.schema import SemanticChangeKind

_RESOURCES = Path(__file__).parent.parent / "resources"
_METABASE_ARTIFACT = str(_RESOURCES / "metabase" / "joined_lineage.json")
_EXEC_POLICY = str(_RESOURCES / "policies" / "breaking_reaches_executive_dashboard.yml")

_REACHED_DASHBOARD = "metabase.dashboard.55"  # Executive KPIs
_OPS_DASHBOARD = "metabase.dashboard.77"  # Ops daily


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
    """A mutated base that retypes ``stg_accounts.account_holder`` so the two-manifest diff
    yields a BREAKING (type_changed) change that flows to ``transactions.account_holder``."""
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    node_id = _find_catalog_node(catalog, "stg_accounts")
    catalog["nodes"][node_id]["columns"]["account_holder"]["type"] = "DECIMAL(38,2)"

    base_catalog = tmp_path / "catalog.json"
    base_manifest = tmp_path / "manifest.json"
    base_catalog.write_text(json.dumps(catalog))
    base_manifest.write_text(json.dumps(manifest))
    return {"catalog": str(base_catalog), "manifest": str(base_manifest)}


@pytest.fixture
def no_gh_env(monkeypatch):
    for var in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        monkeypatch.delenv(var, raising=False)


def _run(args):
    return CliRunner().invoke(impact, args)


def _diff_args(dbt_artifacts, base_artifacts, *extra):
    return [
        "--manifest",
        str(dbt_artifacts["manifest_path"]),
        "--catalog",
        str(dbt_artifacts["catalog_path"]),
        "--base-manifest",
        base_artifacts["manifest"],
        "--base-catalog",
        base_artifacts["catalog"],
        *extra,
    ]


def _ops_only_artifact(tmp_path):
    """A copy of the joined artifact whose reached dashboard is downgraded to tier=operational,
    so the executive policy must NOT fire against it."""
    data = _load(_METABASE_ARTIFACT)
    for dashboard in data["dashboards"]:
        if dashboard["dashboard_id"] == 55:
            dashboard["meta"]["tier"] = "operational"
    path = tmp_path / "metabase_ops.json"
    path.write_text(json.dumps(data))
    return str(path)


# --- THE KILLER: full CLI gate ----------------------------------------------------------


def test_breaking_change_reaching_executive_dashboard_blocks(
    dbt_artifacts, base_artifacts, no_gh_env
):
    result = _run(
        _diff_args(
            dbt_artifacts,
            base_artifacts,
            "--metabase",
            _METABASE_ARTIFACT,
            "--policy",
            _EXEC_POLICY,
            "--ci",
            "--fail-on",
            "policy",
        )
    )
    assert result.exit_code == 1, result.output
    assert "fail-on policy" in result.output


def test_block_verdict_json_names_the_executive_dashboard(dbt_artifacts, base_artifacts):
    result = _run(
        _diff_args(
            dbt_artifacts,
            base_artifacts,
            "--metabase",
            _METABASE_ARTIFACT,
            "--policy",
            _EXEC_POLICY,
            "-f",
            "json",
        )
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)

    # The dashboard surfaces as an exposure in the blast radius, tagged source=metabase.
    exposures = {e["name"]: e for e in report["affected_exposures"]}
    assert _REACHED_DASHBOARD in exposures
    assert exposures[_REACHED_DASHBOARD]["source"] == "metabase"
    assert exposures[_REACHED_DASHBOARD]["meta"]["tier"] == "executive"
    # F4: the exposure carries the column-precise chain — WHICH warehouse column of the
    # dashboard the change hits (transactions.account_holder via card 501), not just the name.
    via_columns = exposures[_REACHED_DASHBOARD].get("via_columns") or []
    assert any(
        v["model"] == "transactions" and v["column"] == "account_holder" for v in via_columns
    ), via_columns

    # The policy blocked, naming the reached dashboard on the hit.
    verdict = report["policy_verdict"]
    assert verdict["decision"] == "block"
    hit = next(h for h in verdict["hits"] if h["rule_id"] == "breaking-reaches-executive-dashboard")
    # Precision: matched_reach reports ONLY the reached objects that satisfied the reach `where`
    # (the executive dashboard), not every reached exposure — the reported reach must equal the
    # objects the notification counts.
    assert hit["matched_reach"] == [_REACHED_DASHBOARD]
    assert verdict["notifications"]

    # Cross-boundary honesty block is attached.
    assert report["metabase"]["dashboards_reached"] >= 1


def test_markdown_surfaces_metabase_dashboard(dbt_artifacts, base_artifacts):
    result = _run(
        _diff_args(
            dbt_artifacts,
            base_artifacts,
            "--metabase",
            _METABASE_ARTIFACT,
            "--policy",
            _EXEC_POLICY,
        )
    )
    assert result.exit_code == 0, result.output
    assert "via **Metabase**" in result.output
    assert "Policy verdict — BLOCK" in result.output
    # F4: the report names the exact affected field, not just the dashboard.
    assert "affects `transactions.account_holder`" in result.output


def test_non_executive_dashboard_does_not_block(dbt_artifacts, base_artifacts, tmp_path, no_gh_env):
    ops_artifact = _ops_only_artifact(tmp_path)
    result = _run(
        _diff_args(
            dbt_artifacts,
            base_artifacts,
            "--metabase",
            ops_artifact,
            "--policy",
            _EXEC_POLICY,
            "--ci",
            "--fail-on",
            "policy",
        )
    )
    # The dashboard is still reached, but tier != executive -> the exec rule cannot fire.
    assert result.exit_code == 0, result.output


def test_backward_compatible_without_metabase(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts, "--policy", _EXEC_POLICY, "-f", "json"))
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    # No --metabase: no dashboard reach, no metabase block, no honesty block.
    assert "metabase" not in report
    assert not any(e["name"] == _REACHED_DASHBOARD for e in report["affected_exposures"])
    assert report["policy_verdict"]["decision"] != "block"


# --- Service+policy level: the breaking-vs-equivalent distinction ------------------------


def _reach_and_service(dbt_artifacts):
    head = LineageService(Path(dbt_artifacts["catalog_path"]), Path(dbt_artifacts["manifest_path"]))
    lineage = load_metabase_lineage(_METABASE_ARTIFACT)
    relation_index = build_relation_index(head.registry)
    reach = MetabaseReach.build(lineage, relation_index)
    return head, reach


def _decision_for(change, dbt_artifacts):
    head, reach = _reach_and_service(dbt_artifacts)
    aggregated = head.get_changeset_impact([change], metabase=reach)
    # The dashboard is reached regardless of breaking-ness (reach is structural).
    assert any(e["name"] == _REACHED_DASHBOARD for e in aggregated["affected_exposures"])
    policy = load_policy(_EXEC_POLICY)
    verdict = evaluate_policy([change], aggregated, head.registry, policy, [], metabase_reach=reach)
    return verdict


def test_breaking_logic_change_blocks(dbt_artifacts):
    change = ColumnChange(
        "stg_accounts",
        "account_holder",
        ChangeKind.LOGIC_CHANGED,
        semantic=SemanticChangeKind.MEANING_CHANGED,
    )
    assert _decision_for(change, dbt_artifacts).decision.value == "block"


def test_equivalent_change_reaching_executive_dashboard_does_not_block(dbt_artifacts):
    # Same reach, but a PROVEN-equivalent refactor: change.breaking is False -> no block.
    change = ColumnChange(
        "stg_accounts",
        "account_holder",
        ChangeKind.LOGIC_CHANGED,
        semantic=SemanticChangeKind.EQUIVALENT,
    )
    assert _decision_for(change, dbt_artifacts).decision.value == "allow"
