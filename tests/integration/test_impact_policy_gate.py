"""Integration tests for the- policy-engine CLI/CI wiring.

Drives the real ``impact`` command end-to-end (ChangesetBuilder + aggregation + policy engine
+ output/gate) against the dbt test-project artifacts, with a synthesized mutated base so there
is a real changeset for the policy to evaluate. Proves:

* ``--fail-on policy`` exit codes (BLOCK -> non-zero, safe -> 0),
* the ``report["policy_verdict"]`` JSON shape,
* the Markdown policy section,
* backward compatibility (existing gates + default behaviour unchanged),
* a present-but-broken policy fails LOUDLY.
"""

import copy
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from dbt_column_lineage.cli.main import impact

_POLICIES = Path(__file__).parent.parent / "resources" / "policies"
_BLOCK_ON_REMOVED = str(_POLICIES / "block_on_removed.yml")
_BROKEN = str(_POLICIES / "broken_version.yml")


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
    """A mutated base producing (a) a REMOVED change on ``legacy_col`` for the policy to block
    on, and (b) a retype of the widely-consumed ``account_id`` so the legacy ``--fail-on any``
    gate has real downstream impact to trip on."""
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    node_id = _find_catalog_node(catalog, "stg_accounts")
    catalog["nodes"][node_id]["columns"]["legacy_col"] = {"name": "legacy_col", "type": "TEXT"}
    catalog["nodes"][node_id]["columns"]["account_id"]["type"] = "DECIMAL(38,2)"

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


# --- JSON shape --------------------------------------------------------------------------


def test_policy_verdict_json_shape(dbt_artifacts, base_artifacts):
    result = _run(
        _diff_args(dbt_artifacts, base_artifacts, "--policy", _BLOCK_ON_REMOVED, "-f", "json")
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)

    assert "policy_verdict" in report
    verdict = report["policy_verdict"]
    # Full PolicyVerdict dump — decision + provenance + sets + notifications + honesty counters.
    for key in (
        "decision",
        "hits",
        "build_set",
        "test_set",
        "notifications",
        "evaluated_rules",
        "fired_rules",
        "unresolved_reach_count",
        "skipped_missing_meta",
    ):
        assert key in verdict, key
    # The REMOVED change trips the block-on-removed rule.
    assert verdict["decision"] == "block"
    assert verdict["fired_rules"] >= 1
    assert any(hit["rule_id"] == "block-on-removed" for hit in verdict["hits"])
    assert verdict["notifications"]


def test_no_policy_leaves_report_unchanged(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts, "-f", "json"))
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)
    # Backward compatible: no policy -> no policy_verdict key, legacy verdict still present.
    assert "policy_verdict" not in report
    assert report["verdict"] in ("safe", "review", "block")


# --- Markdown ----------------------------------------------------------------------------


def test_policy_section_rendered_in_markdown(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts, "--policy", _BLOCK_ON_REMOVED))
    assert result.exit_code == 0, result.output
    assert "Policy verdict — BLOCK" in result.output
    assert "block-on-removed" in result.output


def test_no_policy_no_markdown_section(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts))
    assert result.exit_code == 0, result.output
    assert "Policy verdict" not in result.output


# --- Exit-code gate ----------------------------------------------------------------------


def test_fail_on_policy_blocks(dbt_artifacts, base_artifacts, no_gh_env):
    result = _run(
        _diff_args(
            dbt_artifacts,
            base_artifacts,
            "--ci",
            "--fail-on",
            "policy",
            "--policy",
            _BLOCK_ON_REMOVED,
        )
    )
    assert result.exit_code == 1, result.output
    assert "fail-on policy" in result.output


def test_fail_on_policy_safe_passes(dbt_artifacts, no_gh_env):
    # Diff identical manifests -> no changes -> allow -> exit 0, even with a blocking policy.
    result = _run(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            str(dbt_artifacts["manifest_path"]),
            "--base-catalog",
            str(dbt_artifacts["catalog_path"]),
            "--ci",
            "--fail-on",
            "policy",
            "--policy",
            _BLOCK_ON_REMOVED,
        ]
    )
    assert result.exit_code == 0, result.output


def test_fail_on_policy_without_policy_warns_and_passes(dbt_artifacts, base_artifacts, no_gh_env):
    result = _run(_diff_args(dbt_artifacts, base_artifacts, "--ci", "--fail-on", "policy"))
    assert result.exit_code == 0, result.output
    assert "--fail-on policy needs a resolvable policy" in result.output


# --- Backward compatibility --------------------------------------------------------------


def test_legacy_fail_on_any_unchanged_with_policy_present(dbt_artifacts, base_artifacts, no_gh_env):
    # A blocking policy is present but --fail-on any must gate on impact, not the policy.
    result = _run(
        _diff_args(
            dbt_artifacts, base_artifacts, "--ci", "--fail-on", "any", "--policy", _BLOCK_ON_REMOVED
        )
    )
    assert result.exit_code == 1, result.output
    assert "fail-on any" in result.output


def test_default_no_policy_no_ci_unchanged(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts))
    assert result.exit_code == 0, result.output
    assert "Column-level impact" in result.output


# --- Loud failure on a broken policy -----------------------------------------------------


def test_broken_policy_fails_loudly(dbt_artifacts, base_artifacts):
    result = _run(_diff_args(dbt_artifacts, base_artifacts, "--policy", _BROKEN, "-f", "json"))
    assert result.exit_code == 1, result.output
    assert "unsupported version" in result.output.lower() or "invalid" in result.output.lower()
