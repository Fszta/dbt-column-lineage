#!/usr/bin/env python
import subprocess
import sys
import os
from pathlib import Path
from typing import Optional

def run_tests(test_type: Optional[str] = None) -> int:
    """Run tests with pytest."""
    os.environ["PYTHONPATH"] = os.pathsep.join([
        os.environ.get("PYTHONPATH", ""),
        str(Path(__file__).parent)
    ])
    
    if test_type == "unit":
        cmd = ["pytest", "tests/unit", "-v"]
    elif test_type == "integration":
        cmd = ["pytest", "tests/integration", "-v"]
    elif test_type == "e2e":
        cmd = ["pytest", "tests/e2e", "-v"]
    else:
        cmd = ["pytest", "tests", "-v"]
        
    result = subprocess.run(cmd)
    return result.returncode

def run_tests_unit() -> int:
    return run_tests("unit")

def run_tests_integration() -> int:
    return run_tests("integration")

def run_tests_e2e() -> int:
    return run_tests("e2e")

if __name__ == "__main__":
    test_type = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(run_tests(test_type)) 