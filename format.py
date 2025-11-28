import subprocess
import sys


def format_code() -> int:
    """Format code with black and ruff."""
    black_cmd = ["black", ".", "--line-length=100"]
    ruff_cmd = ["ruff", "check", "--fix", "--unsafe-fixes", "--exit-non-zero-on-fix", "."]

    black_result = subprocess.run(black_cmd)
    if black_result.returncode != 0:
        return black_result.returncode

    ruff_result = subprocess.run(ruff_cmd)
    return ruff_result.returncode


if __name__ == "__main__":
    sys.exit(format_code())
