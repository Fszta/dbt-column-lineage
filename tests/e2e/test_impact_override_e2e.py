"""End-to-end: the installed ``parrant impact`` console script honours override pragmas.

Runs the real entry point as a subprocess so the actual process exit code is proven: an
``allow-break`` pragma acknowledging a provable break clears the ``--fail-on tests`` gate
(exit 0), while ``--no-overrides`` re-arms it (exit 1). Also asserts the markdown surfaces the
"Overrides applied" section.
"""

import copy
import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent


def _load(path):
    with open(path) as f:
        return json.load(f)


def _model_uid(manifest, name):
    for uid, node in manifest["nodes"].items():
        if node.get("name") == name and node.get("resource_type") == "model":
            return uid
    raise AssertionError(f"{name} not in manifest")


def _catalog_uid(catalog, name):
    for uid in catalog["nodes"]:
        if uid.split(".")[-1] == name:
            return uid
    raise AssertionError(f"{name} not in catalog")


@pytest.fixture
def allow_break_head(dbt_artifacts, tmp_path):
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))
    catalog["nodes"][_catalog_uid(catalog, "stg_transactions")]["columns"].pop(
        "transaction_id", None
    )
    man_uid = _model_uid(manifest, "stg_transactions")
    manifest["nodes"][man_uid]["compiled_code"] = (
        '-- lineage:allow-break column=transaction_id reason="test yml updated in follow-up PR"\n'
        + manifest["nodes"][man_uid]["compiled_code"]
    )
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (tmp_path / "catalog.json").write_text(json.dumps(catalog))
    return {
        "manifest": str(tmp_path / "manifest.json"),
        "catalog": str(tmp_path / "catalog.json"),
    }


def _run(args):
    import os

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


def test_allow_break_clears_tests_gate_end_to_end(dbt_artifacts, allow_break_head):
    base = [
        "--manifest",
        allow_break_head["manifest"],
        "--catalog",
        allow_break_head["catalog"],
        "--base-manifest",
        str(dbt_artifacts["manifest_path"]),
        "--base-catalog",
        str(dbt_artifacts["catalog_path"]),
    ]

    # Acknowledged break -> the tests gate does NOT fire.
    honored = _run([*base, "--ci", "--fail-on", "tests"])
    assert honored.returncode == 0, f"stdout={honored.stdout}\nstderr={honored.stderr}"

    # --no-overrides re-arms the raw gate -> it fails.
    raw = _run([*base, "--ci", "--fail-on", "tests", "--no-overrides"])
    assert raw.returncode == 1, f"stdout={raw.stdout}\nstderr={raw.stderr}"

    # The markdown report surfaces the honored override + its reason.
    md = _run(base)
    assert md.returncode == 0, md.stderr
    assert "Overrides applied" in md.stdout
    assert "allow-break" in md.stdout
    assert "test yml updated in follow-up PR" in md.stdout
