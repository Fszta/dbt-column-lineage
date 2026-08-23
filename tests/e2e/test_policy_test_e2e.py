"""End-to-end: the installed ``parrant policy test`` console script backtests a policy
over the real repo's recent git history against the bundled dbt project.

Runs the actual entry point as a subprocess (not the in-process runner) so the CLI wiring,
table/json rendering, and the --fail-on exit codes are proven against a genuine process.
"""

import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
_POLICY = str(REPO_ROOT / "tests" / "resources" / "policies" / "block_on_removed.yml")


def _run(args):
    return subprocess.run(
        ["poetry", "run", "parrant", "policy", "test", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def _common(dbt_artifacts):
    return [
        "--manifest",
        str(dbt_artifacts["manifest_path"]),
        "--catalog",
        str(dbt_artifacts["catalog_path"]),
        "--policy",
        _POLICY,
        "--repo-dir",
        str(REPO_ROOT),
    ]


def test_last_5_table_output_exits_zero(dbt_artifacts):
    result = _run([*_common(dbt_artifacts), "--last", "5", "--format", "table"])
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    assert "Policy backtest" in result.stdout
    assert "Fidelity:" in result.stdout


def test_last_5_json_output_is_wellformed(dbt_artifacts):
    result = _run([*_common(dbt_artifacts), "--last", "5", "--format", "json"])
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    report = json.loads(result.stdout)
    assert report["mode"] == "git-diff"
    assert "rule_stats" in report
    assert "points" in report
    assert report["head"] == "HEAD"


def test_regression_without_baseline_fails_loudly(dbt_artifacts):
    result = _run([*_common(dbt_artifacts), "--last", "5", "--fail-on", "regression"])
    assert result.returncode == 1
    assert "requires --baseline" in result.stderr


def test_two_sources_is_rejected(dbt_artifacts):
    result = _run([*_common(dbt_artifacts), "--last", "5", "--git-range", "HEAD~2..HEAD"])
    assert result.returncode == 1
    assert "mutually exclusive" in result.stderr or "exactly one" in result.stderr
