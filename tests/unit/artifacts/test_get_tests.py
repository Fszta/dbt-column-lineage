"""Unit tests for ``ManifestReader.get_tests`` — reading dbt test nodes.

The manifest fixtures below mirror the exact shape dbt 1.10 emits for generic column
tests (verified against the regenerated bundled ``dbt_test_project`` manifest): a
top-level ``column_name`` and ``attached_node``, plus ``test_metadata.name`` /
``test_metadata.kwargs`` (with ``to`` / ``field`` for ``relationships``).
"""

import json

from dbt_column_lineage.artifacts.manifest import ManifestReader


def _generic_test_node(name, model, column, extra_kwargs=None, extra_deps=None):
    """Build a generic column-test node in the dbt 1.10 manifest shape."""
    kwargs = {
        "column_name": column,
        "model": f"{{{{ get_where_subquery(ref('{model}')) }}}}",
    }
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    unique_id = f"test.pkg.{name}_{model}_{column}.abc123"
    deps = [f"model.pkg.{model}"]
    if extra_deps:
        deps = extra_deps
    return unique_id, {
        "name": f"{name}_{model}_{column}",
        "resource_type": "test",
        "unique_id": unique_id,
        "column_name": column,
        "attached_node": f"model.pkg.{model}",
        "original_file_path": "models/staging/models.yml",
        "test_metadata": {"name": name, "kwargs": kwargs, "namespace": None},
        "depends_on": {"nodes": deps},
    }


def _write_manifest(tmp_path, nodes):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({"metadata": {"adapter_type": "duckdb"}, "nodes": nodes}))
    return str(manifest_path)


def test_get_tests_parses_generic_column_tests(tmp_path):
    nodes = {}
    nodes.update([_generic_test_node("not_null", "stg_transactions", "transaction_id")])
    nodes.update([_generic_test_node("unique", "stg_transactions", "transaction_id")])
    # A plain model node must be ignored.
    nodes["model.pkg.stg_transactions"] = {
        "name": "stg_transactions",
        "resource_type": "model",
        "depends_on": {"nodes": []},
    }

    reader = ManifestReader(_write_manifest(tmp_path, nodes))
    reader.load()

    tests = reader.get_tests()
    assert len(tests) == 2
    by_name = {t.test_name: t for t in tests}
    assert set(by_name) == {"not_null", "unique"}
    for t in tests:
        assert t.target_model == "stg_transactions"
        assert t.target_column == "transaction_id"
        assert t.resource_path == "models/staging/models.yml"
        assert t.referenced_model is None
        assert t.referenced_column is None


def test_get_tests_parses_relationships_with_referenced_side(tmp_path):
    # relationships depends on BOTH the tested model and the referenced model.
    uid, node = _generic_test_node(
        "relationships",
        "stg_transactions",
        "account_id",
        extra_kwargs={"to": "ref('stg_accounts')", "field": "account_id"},
        extra_deps=["model.pkg.stg_accounts", "model.pkg.stg_transactions"],
    )
    reader = ManifestReader(_write_manifest(tmp_path, {uid: node}))
    reader.load()

    tests = reader.get_tests()
    assert len(tests) == 1
    t = tests[0]
    assert t.test_name == "relationships"
    assert t.target_model == "stg_transactions"
    assert t.target_column == "account_id"
    assert t.referenced_model == "stg_accounts"
    assert t.referenced_column == "account_id"


def test_get_tests_column_name_falls_back_to_kwargs(tmp_path):
    # Some dbt versions omit the top-level column_name; kwargs still carries it.
    uid, node = _generic_test_node("not_null", "orders", "customer_id")
    del node["column_name"]
    reader = ManifestReader(_write_manifest(tmp_path, {uid: node}))
    reader.load()

    (t,) = reader.get_tests()
    assert t.target_column == "customer_id"


def test_get_tests_marks_unattributable_column_as_none(tmp_path):
    # A generic test with no discoverable column (e.g. a model-level test) keeps the
    # column as None rather than guessing.
    uid, node = _generic_test_node("my_custom_check", "orders", "irrelevant")
    del node["column_name"]
    del node["test_metadata"]["kwargs"]["column_name"]
    reader = ManifestReader(_write_manifest(tmp_path, {uid: node}))
    reader.load()

    (t,) = reader.get_tests()
    assert t.target_model == "orders"
    assert t.target_column is None


def test_get_tests_skips_singular_tests_without_metadata(tmp_path):
    nodes = {
        "test.pkg.assert_something.deadbeef": {
            "name": "assert_something",
            "resource_type": "test",
            "unique_id": "test.pkg.assert_something.deadbeef",
            "original_file_path": "tests/assert_something.sql",
            "depends_on": {"nodes": ["model.pkg.orders"]},
            # No test_metadata: a singular/custom SQL test.
        }
    }
    reader = ManifestReader(_write_manifest(tmp_path, nodes))
    reader.load()

    assert reader.get_tests() == []
