"""Compiled-SQL recovery when the manifest's original_file_path drifts from target/compiled.

Real manifests (dbt parse, no embedded compiled_code) rely on the on-disk ``target/compiled``
tree. When a model was moved/refactored between the build that produced the manifest and the
one that produced ``compiled/``, the exact reconstructed path misses even though the compiled
file is still on disk under its old sub-path. These tests cover the by-filename fallback.
"""

import json

from dbt_column_lineage.artifacts.manifest import ManifestReader


def _write_manifest(tmp_path, node):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"nodes": {node["unique_id"]: node}}))
    return manifest


def _model_node(name, original_file_path, package="pkg"):
    return {
        "name": name,
        "unique_id": f"model.{package}.{name}",
        "resource_type": "model",
        "package_name": package,
        "original_file_path": original_file_path,
        # No embedded compiled_code / compiled_path -> forces on-disk resolution.
    }


def _write_compiled(tmp_path, rel_path, sql):
    path = tmp_path / "compiled" / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sql)
    return path


def test_recovers_compiled_sql_when_path_drifted(tmp_path):
    # Manifest says the model lives at models/old/loc; compiled/ has it at models/new/loc.
    node = _model_node("orders", "models/old/loc/orders.sql")
    manifest = _write_manifest(tmp_path, node)
    _write_compiled(tmp_path, "pkg/models/new/loc/orders.sql", "select 1 as id")

    reader = ManifestReader(str(manifest))
    reader.load()
    assert reader.get_compiled_sql("orders") == "select 1 as id"


def test_exact_path_still_preferred_when_present(tmp_path):
    node = _model_node("orders", "models/marts/orders.sql")
    manifest = _write_manifest(tmp_path, node)
    _write_compiled(tmp_path, "pkg/models/marts/orders.sql", "select 'exact' as x")

    reader = ManifestReader(str(manifest))
    reader.load()
    assert reader.get_compiled_sql("orders") == "select 'exact' as x"


def test_ambiguous_basename_is_not_guessed(tmp_path):
    # Two compiled files named orders.sql in different packages, none at the exact path,
    # and neither under the model's own package -> decline to guess (honest miss).
    node = _model_node("orders", "models/old/orders.sql", package="pkg")
    manifest = _write_manifest(tmp_path, node)
    _write_compiled(tmp_path, "other_a/models/x/orders.sql", "select 1")
    _write_compiled(tmp_path, "other_b/models/y/orders.sql", "select 2")

    reader = ManifestReader(str(manifest))
    reader.load()
    assert reader.get_compiled_sql("orders") is None


def test_package_scoped_match_wins_over_ambiguity(tmp_path):
    # Same basename in several packages, but exactly one under the model's own package -> use it.
    node = _model_node("orders", "models/old/orders.sql", package="pkg")
    manifest = _write_manifest(tmp_path, node)
    _write_compiled(tmp_path, "pkg/models/new/orders.sql", "select 'mine' as x")
    _write_compiled(tmp_path, "other/models/z/orders.sql", "select 'theirs' as x")

    reader = ManifestReader(str(manifest))
    reader.load()
    assert reader.get_compiled_sql("orders") == "select 'mine' as x"


def test_no_compiled_file_returns_none(tmp_path):
    node = _model_node("orders", "models/old/orders.sql")
    manifest = _write_manifest(tmp_path, node)
    (tmp_path / "compiled").mkdir()

    reader = ManifestReader(str(manifest))
    reader.load()
    assert reader.get_compiled_sql("orders") is None
