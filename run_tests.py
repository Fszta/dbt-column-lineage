#!/usr/bin/env python
import subprocess
import sys
import os
from pathlib import Path

def run_tests(test_type=None):
    """Run tests with pytest."""
    os.environ["PYTHONPATH"] = os.pathsep.join([
        os.environ.get("PYTHONPATH", ""),
        str(Path(__file__).parent)
    ])
    
    if test_type == "unit":
        cmd = ["pytest", "tests/unit", "-v"]
    elif test_type == "integration":
        cmd = ["pytest", "tests/integration", "-v"]
    else:
        cmd = ["pytest", "tests", "-v"]
        
    result = subprocess.run(cmd)
    return result.returncode

def run_tests_unit():
    return run_tests("unit")

def run_tests_integration():
    return run_tests("integration")

if __name__ == "__main__":
    test_type = sys.argv[1] if len(sys.argv) > 1 else None
    sys.exit(run_tests(test_type)) 