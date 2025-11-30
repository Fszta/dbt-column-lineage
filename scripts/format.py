import subprocess
import sys
from pathlib import Path


def format_code() -> int:
    """Format code with black and ruff."""
    project_root = Path(__file__).parent.parent
    black_cmd = ["black", ".", "--line-length=100"]
    ruff_cmd = ["ruff", "check", "--fix", "--unsafe-fixes", "--exit-non-zero-on-fix", "."]

    black_result = subprocess.run(black_cmd, cwd=project_root)
    if black_result.returncode != 0:
        return black_result.returncode

    ruff_result = subprocess.run(ruff_cmd, cwd=project_root)
    return ruff_result.returncode


if __name__ == "__main__":
    sys.exit(format_code())
