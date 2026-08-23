"""End-to-end: the installed ``dbt-col-lineage policy init --stdout`` console script scaffolds a
policy from the bundled dbt project.

Runs the actual entry point as a subprocess (not the in-process runner) so the CLI wiring and
the ``policy`` group dispatch are proven against a genuine process. Asserts on the key structural
markers of the emitted policy rather than a brittle full-text snapshot.
"""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent


def _run(args):
    return subprocess.run(
        ["poetry", "run", "dbt-col-lineage", "policy", "init", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def test_init_stdout_emits_safe_scaffold(dbt_artifacts):
    result = _run(
        [
            "--stdout",
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
        ]
    )
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    out = result.stdout
    # Structural markers of a well-formed, safe scaffold.
    assert "version: 1" in out
    assert "provable-break-block" in out
    assert "exposure-guard" in out
    # The honesty-brand pointer at `policy test` (byte-consistent with the real subcommand + flag).
    assert "policy test --last 20" in out
    # The core honesty invariant: the scaffold never authors an open-when-unsure gate.
    assert "fail_open" not in out
