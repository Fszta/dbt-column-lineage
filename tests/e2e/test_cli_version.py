"""End-to-end test for the CLI --version flag."""

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent


def test_version_flag_prints_version_and_exits_zero() -> None:
    """`dbt-col-lineage --version` prints a semver-ish string and exits 0."""
    result = subprocess.run(
        ["poetry", "run", "dbt-col-lineage", "--version"],
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
