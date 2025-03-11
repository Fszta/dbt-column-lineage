import pytest
import os
import sys
from pathlib import Path


@pytest.fixture(scope="session")
def dbt_project_dir():
    """Get the path to the dbt test project."""
    return Path(__file__).parent / "resources" / "dbt_test_project"

