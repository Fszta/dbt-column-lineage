import subprocess
import sys


def type_check() -> int:
    """Run type checking with mypy."""
    cmd = [
        "mypy",
        "dbt_column_lineage",
        "tests",
        "--show-error-codes",
        "--pretty",
        "--explicit-package-bases",
    ]
    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    sys.exit(type_check())
