import subprocess
import sys
from pathlib import Path


def type_check() -> int:
    """Run type checking with mypy."""
    project_root = Path(__file__).parent.parent
    cmd = [
        "mypy",
        "dbt_column_lineage",
        "tests",
        "--show-error-codes",
        "--pretty",
        "--explicit-package-bases",
    ]
    result = subprocess.run(cmd, cwd=project_root)
    return result.returncode


if __name__ == "__main__":
    sys.exit(type_check())
