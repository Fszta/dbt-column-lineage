"""Structural offline guardrail.

The join/reach/artifact modules — everything the OFFLINE gate imports to consume a Metabase
snapshot — must never pull in the credentialed HTTP client. Enforced structurally so the
guarantee can't silently rot: importing the consume path leaves ``metabase.client`` unloaded,
and the modules carry no textual reference to it.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[3] / "dbt_column_lineage" / "metabase"
_CONSUME_MODULES = ["artifact.py", "join.py", "reach.py"]


def _imported_names(source_file: Path) -> set:
    tree = ast.parse(source_file.read_text(encoding="utf-8"))
    names: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_consume_modules_do_not_import_the_client():
    for module in _CONSUME_MODULES:
        names = _imported_names(_PKG / module)
        assert not any(
            "metabase.client" in name or name in ("requests", "httpx") for name in names
        ), f"{module} must not import the credentialed client / an HTTP lib; got {sorted(names)}"


def test_importing_reach_does_not_load_the_client():
    # A fresh interpreter: importing the whole consume path must not transitively load the
    # credentialed client module.
    code = (
        "import sys\n"
        "import dbt_column_lineage.metabase.artifact\n"
        "import dbt_column_lineage.metabase.join\n"
        "import dbt_column_lineage.metabase.reach\n"
        "assert 'dbt_column_lineage.metabase.client' not in sys.modules, "
        "'client leaked into the offline consume path'\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
