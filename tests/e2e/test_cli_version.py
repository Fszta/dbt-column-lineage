"""End-to-end test for the CLI --version flag."""

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent


def test_version_flag_prints_version_and_exits_zero() -> None:
    """`parrant --version` prints a semver-ish string and exits 0."""
    result = subprocess.run(
        ["poetry", "run", "parrant", "--version"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert (
        result.returncode == 0
    ), f"Expected exit 0, got {result.returncode}. STDERR: {result.stderr}"
    assert re.search(
        r"\d+\.\d+\.\d+", result.stdout
    ), f"Expected a version string in stdout, got: {result.stdout!r}"


def test_legacy_alias_still_works_and_warns_on_stderr() -> None:
    """The deprecated `dbt-col-lineage` alias delegates to `parrant`, printing the rename
    notice to STDERR only — stdout stays clean so JSON/CI output is never corrupted."""
    result = subprocess.run(
        ["poetry", "run", "dbt-col-lineage", "--version"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, f"alias exited {result.returncode}. STDERR: {result.stderr}"
    # Version still lands on stdout (uncorrupted); the rename notice goes to stderr.
    assert re.search(r"\d+\.\d+\.\d+", result.stdout)
    assert "renamed to `parrant`" in result.stderr
    assert "renamed" not in result.stdout
