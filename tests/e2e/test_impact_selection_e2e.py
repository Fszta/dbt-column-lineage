"""End-to-end: ``impact --emit-selector`` writes the policy-free rebuild selection to
``$GITHUB_OUTPUT`` and carries the same ``selection`` block in the JSON report.

Runs the installed console script as a subprocess so the real side-channel behaviour is
proven: a genuine change emits ``has_rebuild=true`` + a non-empty space-joined node-name
selector; a no-op diff emits ``has_rebuild=false`` + an empty selector; both exit 0 (the flag
never gates). A broad unanalyzable change widens to all reachable with an empty skippable set.
"""

import copy
import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
_STG_UID = "model.test_project.stg_transactions"
_BLIND_COUNT = 120


def _load(path):
    with open(path) as f:
        return json.load(f)


def _find_catalog_node(catalog, model_name):
    for node_id, node in catalog["nodes"].items():
        if node.get("metadata", {}).get("name", "").lower() == model_name:
            return node_id
    raise AssertionError(f"{model_name} not in catalog")


@pytest.fixture
def removed_col_base(dbt_artifacts, tmp_path):
    """A base whose stg_transactions carries an extra column -> head shows a REMOVED change
    that fans downstream, so the selection has a non-empty rebuild set."""
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    node_id = _find_catalog_node(catalog, "stg_transactions")
    catalog["nodes"][node_id]["columns"]["legacy_col"] = {"name": "legacy_col", "type": "TEXT"}
    base_catalog = tmp_path / "base_catalog.json"
    base_manifest = tmp_path / "base_manifest.json"
    base_catalog.write_text(json.dumps(catalog))
    base_manifest.write_text(json.dumps(manifest))
    return {"catalog": str(base_catalog), "manifest": str(base_manifest)}


@pytest.fixture
def broad_unanalyzable(dbt_artifacts, tmp_path):
    """Head manifest with 120 column-less models downstream of stg_transactions (absent from
    the catalog, no compiled SQL) + a base with an extra column on stg_transactions, so head
    shows a REMOVED change that fans into all 120 blind (unanalyzable) models."""
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))

    template = manifest["nodes"][_STG_UID]
    blind_names = []
    for i in range(_BLIND_COUNT):
        name = f"blind_{i:03d}"
        blind_names.append(name)
        uid = f"model.test_project.{name}"
        node = copy.deepcopy(template)
        node.update(
            unique_id=uid,
            name=name,
            alias=name,
            fqn=["test_project", "staging", name],
            path=f"staging/{name}.sql",
            original_file_path=f"models/staging/{name}.sql",
        )
        node["depends_on"] = {"macros": [], "nodes": [_STG_UID]}
        node["columns"] = {}
        node.pop("compiled_code", None)
        node.pop("raw_code", None)
        node["compiled"] = False
        manifest["nodes"][uid] = node

    head_manifest = tmp_path / "head_manifest.json"
    head_manifest.write_text(json.dumps(manifest))
    head_catalog = tmp_path / "head_catalog.json"
    head_catalog.write_text(json.dumps(catalog))

    base_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    base_catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    stg_catalog_id = _find_catalog_node(base_catalog, "stg_transactions")
    base_catalog["nodes"][stg_catalog_id]["columns"]["legacy_col"] = {
        "name": "legacy_col",
        "type": "TEXT",
    }
    base_manifest_path = tmp_path / "base_manifest.json"
    base_manifest_path.write_text(json.dumps(base_manifest))
    base_catalog_path = tmp_path / "base_catalog.json"
    base_catalog_path.write_text(json.dumps(base_catalog))

    return {
        "manifest": str(head_manifest),
        "catalog": str(head_catalog),
        "base_manifest": str(base_manifest_path),
        "base_catalog": str(base_catalog_path),
        "blind_names": blind_names,
    }


def _run(args, github_output=None):
    env = {k: v for k, v in os.environ.items()}
    for var in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        env.pop(var, None)
    if github_output is not None:
        env["GITHUB_OUTPUT"] = str(github_output)
    else:
        env.pop("GITHUB_OUTPUT", None)
    return subprocess.run(
        ["poetry", "run", "parrant", "impact", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )


def _parse_output_file(path):
    return dict(line.split("=", 1) for line in Path(path).read_text().splitlines() if "=" in line)


def test_emit_selector_writes_rebuild_on_a_change(dbt_artifacts, removed_col_base, tmp_path):
    gh_output = tmp_path / "gh_output"
    result = _run(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            removed_col_base["manifest"],
            "--base-catalog",
            removed_col_base["catalog"],
            "--emit-selector",
            "--format",
            "json",
        ],
        github_output=gh_output,
    )
    # The flag never gates: a change with no --fail-on still exits 0.
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    payload = json.loads(result.stdout)
    selection = payload["selection"]
    assert selection["has_rebuild"] is True
    assert selection["rebuild_selector"] != ""
    # The selector is a space-join of the (sorted) rebuild models.
    assert selection["rebuild_selector"].split() == selection["rebuild_models"]

    written = _parse_output_file(gh_output)
    assert written["has_rebuild"] == "true"
    # The $GITHUB_OUTPUT projection is byte-identical to the JSON report.
    assert written["rebuild_selector"] == selection["rebuild_selector"]


def test_emit_selector_no_op_diff_emits_false_sentinel(dbt_artifacts, tmp_path):
    gh_output = tmp_path / "gh_output"
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
            "--emit-selector",
            "--format",
            "json",
        ],
        github_output=gh_output,
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    payload = json.loads(result.stdout)
    selection = payload["selection"]
    assert selection["has_rebuild"] is False
    assert selection["rebuild_selector"] == ""

    written = _parse_output_file(gh_output)
    # The sentinel says false and the selector is empty — CI branches, never runs
    # ``dbt build --select ""``.
    assert written["has_rebuild"] == "false"
    assert written["rebuild_selector"] == ""


def test_broad_unanalyzable_widens_to_all_reachable(broad_unanalyzable):
    result = _run(
        [
            "--manifest",
            broad_unanalyzable["manifest"],
            "--catalog",
            broad_unanalyzable["catalog"],
            "--base-manifest",
            broad_unanalyzable["base_manifest"],
            "--base-catalog",
            broad_unanalyzable["base_catalog"],
            "--format",
            "json",
        ]
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    payload = json.loads(result.stdout)
    selection = payload["selection"]
    # A broad unanalyzable change is partial confidence -> widen fail-closed.
    assert selection["widened_to_all_reachable"] is True
    assert selection["skippable_models"] == []
    # Every injected blind model parrant could not analyze is in the rebuild set.
    missing = set(broad_unanalyzable["blind_names"]) - set(selection["rebuild_models"])
    assert not missing, f"{len(missing)} unanalyzable models dropped from rebuild_models"


def test_resolution_summary_bounds_forced_rebuilds(broad_unanalyzable):
    result = _run(
        [
            "--manifest",
            broad_unanalyzable["manifest"],
            "--catalog",
            broad_unanalyzable["catalog"],
            "--base-manifest",
            broad_unanalyzable["base_manifest"],
            "--base-catalog",
            broad_unanalyzable["base_catalog"],
            "--format",
            "json",
        ]
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    payload = json.loads(result.stdout)
    selection = payload["selection"]
    summary = payload["resolution_summary"]
    forced = summary["rebuild_forced_by_nonresolution"]

    # The non-resolution metric is present and bounded by the rebuild set size.
    assert 0 <= forced <= len(selection["rebuild_models"])
    # All 120 injected blind models are unanalyzable and in the rebuild set, so they are each
    # counted as a rebuild forced by non-resolution.
    assert forced >= _BLIND_COUNT
    # Per-model resolution covers exactly the reachable set and reconciles with the counts.
    resolution = payload["resolution"]
    assert summary["reachable"] == len(resolution)
    confidence = payload["confidence"]
    assert summary["no_column_info"] + summary["unresolved"] == confidence["no_column_info"]
    assert summary["parse_failed"] == confidence["parse_failed"]
