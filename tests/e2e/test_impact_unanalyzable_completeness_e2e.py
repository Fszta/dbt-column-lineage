"""End-to-end: ``impact --format json`` on a broad change with >100 unanalyzable models
carries the COMPLETE unanalyzable name lists, so a fail-closed force-rebuild set derived
from the JSON covers every model parrant couldn't analyze.

Runs the installed console script as a subprocess against a synthetic head manifest that
injects 120 column-less models downstream of a real staging model. The change on that
staging model fans into all 120, none of which parrant can resolve — the whole point is
that the emitted lists are not silently truncated at 100.
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


@pytest.fixture
def broad_unanalyzable(dbt_artifacts, tmp_path):
    """Head manifest with 120 column-less models downstream of stg_transactions (absent
    from the catalog, no compiled SQL) + a base whose stg_transactions carries an extra
    column, so head shows a REMOVED change that fans into all 120 blind models."""
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

    # Base: original manifest, plus an extra column on stg_transactions in the catalog so
    # head reads as a REMOVED change on that model (the change that reaches the blind set).
    base_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    base_catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    stg_catalog_id = next(
        node_id
        for node_id, node in base_catalog["nodes"].items()
        if node.get("metadata", {}).get("name", "").lower() == "stg_transactions"
    )
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


def _run(args):
    env = {k: v for k, v in os.environ.items()}
    for var in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        env.pop(var, None)
    return subprocess.run(
        ["poetry", "run", "parrant", "impact", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        check=False,
    )


def test_json_carries_complete_unanalyzable_lists(broad_unanalyzable):
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
    confidence = payload["confidence"]

    assert confidence["level"] == "partial"
    # The honesty invariant: the machine lists equal their counts exactly, never capped.
    assert len(confidence["no_column_info_models"]) == confidence["no_column_info"]
    assert len(confidence["parse_failed_models"]) == confidence["parse_failed"]
    assert confidence["no_column_info_truncated"] is False
    assert confidence["parse_failed_truncated"] is False

    # The fail-closed force-rebuild floor, reconstructed purely from the emitted lists,
    # covers every one of the 120 injected models parrant could not analyze.
    force_build = set(confidence["no_column_info_models"]) | set(confidence["parse_failed_models"])
    assert len(force_build) == confidence["unanalyzable_models"]
    missing = set(broad_unanalyzable["blind_names"]) - force_build
    assert not missing, f"{len(missing)} unanalyzable models dropped from the JSON lists"
