"""End-to-end: the installed ``dbt-col-lineage impact`` console script honours the policy gate.

Runs the real entry point as a subprocess (not the in-process CliRunner) so the actual process
exit code under ``--fail-on policy`` is proven: a BLOCK verdict -> non-zero, a no-change run -> 0.
"""

import copy
import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
_POLICY = str(REPO_ROOT / "tests" / "resources" / "policies" / "block_on_removed.yml")


def _load(path):
    with open(path) as f:
        return json.load(f)


def _find_catalog_node(catalog, model_name):
    for node_id, node in catalog["nodes"].items():
        if node.get("metadata", {}).get("name", "").lower() == model_name:
            return node_id
    raise AssertionError(f"{model_name} not in catalog")


@pytest.fixture
def mutated_base(dbt_artifacts, tmp_path):
    """A base whose stg_accounts still has ``legacy_col`` -> head shows a REMOVED change."""
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    node_id = _find_catalog_node(catalog, "stg_accounts")
    catalog["nodes"][node_id]["columns"]["legacy_col"] = {"name": "legacy_col", "type": "TEXT"}
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return {"catalog": str(tmp_path / "catalog.json"), "manifest": str(tmp_path / "manifest.json")}


def _run(args, env_clean=True):
    env = None
    if env_clean:
        import os

        env = {k: v for k, v in os.environ.items()}
        for var in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
            env.pop(var, None)
    return subprocess.run(
        ["poetry", "run", "dbt-col-lineage", "impact", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )


def test_fail_on_policy_block_exits_nonzero(dbt_artifacts, mutated_base):
    result = _run(
        [
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            mutated_base["manifest"],
            "--base-catalog",
            mutated_base["catalog"],
            "--ci",
            "--fail-on",
            "policy",
            "--policy",
            _POLICY,
        ]
    )
    assert result.returncode == 1, f"stdout={result.stdout}\nstderr={result.stderr}"


def test_fail_on_policy_no_change_exits_zero(dbt_artifacts):
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
            _POLICY,
        ]
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
