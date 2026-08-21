"""Unit tests for the ``(model, column) -> tests`` reverse index on ``ModelRegistry``.

Built from synthetic catalog + manifest artifacts (no dbt run needed), replicating the
dbt 1.10 test-node shape. The integration suite additionally exercises this against the
real regenerated ``dbt_test_project`` manifest.
"""

import json

from dbt_column_lineage.artifacts.registry import ModelRegistry


def _catalog_node(name, columns, schema="main", database="main"):
    return {
        "unique_id": f"model.pkg.{name}",
        "metadata": {"name": name, "schema": schema, "database": database, "type": "BASE TABLE"},
        "columns": {c: {"name": c, "type": "TEXT"} for c in columns},
    }


def _model_node(name, columns, depends_on=None):
    cols = ", ".join(columns)
    return {
        "name": name,
        "unique_id": f"model.pkg.{name}",
        "resource_type": "model",
        "language": "sql",
        "schema": "main",
        "database": "main",
        "config": {"materialized": "table"},
        "compiled_code": f"select {cols} from raw",
        "depends_on": {"nodes": [f"model.pkg.{d}" for d in (depends_on or [])]},
    }


def _generic_test_node(name, model, column, extra_kwargs=None, extra_deps=None):
    kwargs = {"column_name": column, "model": f"{{{{ get_where_subquery(ref('{model}')) }}}}"}
    if extra_kwargs:
        kwargs.update(extra_kwargs)
    unique_id = f"test.pkg.{name}_{model}_{column}.abc123"
    deps = extra_deps or [f"model.pkg.{model}"]
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


def _build_registry(tmp_path):
    catalog_nodes = {
        "model.pkg.stg_transactions": _catalog_node(
            "stg_transactions", ["transaction_id", "account_id"]
        ),
        "model.pkg.stg_accounts": _catalog_node("stg_accounts", ["account_id"]),
    }
    manifest_nodes = {
        "model.pkg.stg_transactions": _model_node(
            "stg_transactions", ["transaction_id", "account_id"]
        ),
        "model.pkg.stg_accounts": _model_node("stg_accounts", ["account_id"]),
    }
    manifest_nodes.update([_generic_test_node("not_null", "stg_transactions", "transaction_id")])
    manifest_nodes.update([_generic_test_node("unique", "stg_transactions", "transaction_id")])
    manifest_nodes.update(
        [
            _generic_test_node(
                "relationships",
                "stg_transactions",
                "account_id",
                extra_kwargs={"to": "ref('stg_accounts')", "field": "account_id"},
                extra_deps=["model.pkg.stg_accounts", "model.pkg.stg_transactions"],
            )
        ]
    )
    # An unattributable test: no discoverable column.
    uid, node = _generic_test_node("my_check", "stg_accounts", "x")
    del node["column_name"]
    del node["test_metadata"]["kwargs"]["column_name"]
    manifest_nodes[uid] = node

    catalog_path = tmp_path / "catalog.json"
    manifest_path = tmp_path / "manifest.json"
    catalog_path.write_text(
        json.dumps({"metadata": {"adapter_type": "duckdb"}, "nodes": catalog_nodes})
    )
    manifest_path.write_text(
        json.dumps({"metadata": {"adapter_type": "duckdb"}, "nodes": manifest_nodes})
    )
    registry = ModelRegistry(str(catalog_path), str(manifest_path))
    registry.load()
    return registry


def test_get_column_tests_returns_not_null_and_unique(tmp_path):
    registry = _build_registry(tmp_path)
    tests = registry.get_column_tests("stg_transactions", "transaction_id")
    assert {t.test_name for t in tests} == {"not_null", "unique"}


def test_get_column_tests_is_case_insensitive(tmp_path):
    registry = _build_registry(tmp_path)
    tests = registry.get_column_tests("STG_TRANSACTIONS", "Transaction_ID")
    assert {t.test_name for t in tests} == {"not_null", "unique"}


def test_get_column_tests_relationships_carries_referenced_side(tmp_path):
    registry = _build_registry(tmp_path)
    (rel,) = registry.get_column_tests("stg_transactions", "account_id")
    assert rel.test_name == "relationships"
    assert rel.referenced_model == "stg_accounts"
    assert rel.referenced_column == "account_id"


def test_get_column_tests_unknown_pair_returns_empty(tmp_path):
    registry = _build_registry(tmp_path)
    assert registry.get_column_tests("does_not_exist", "nope") == []
    assert registry.get_column_tests("stg_transactions", "no_such_column") == []


def test_unattributable_tests_are_counted_not_indexed(tmp_path):
    registry = _build_registry(tmp_path)
    assert registry.get_unattributable_test_count() == 1
    (unattr,) = registry.get_unattributable_tests()
    assert unattr.target_column is None


def test_get_tests_referencing_returns_relationships_on_parent_key(tmp_path):
    """The referenced (parent) side of a relationships test is indexed for parent-key removal."""
    registry = _build_registry(tmp_path)
    (rel,) = registry.get_tests_referencing("stg_accounts", "account_id")
    assert rel.test_name == "relationships"
    assert rel.referenced_model == "stg_accounts"
    # Case-insensitive, and a non-referenced column returns nothing.
    assert registry.get_tests_referencing("STG_ACCOUNTS", "ACCOUNT_ID")
    assert registry.get_tests_referencing("stg_transactions", "transaction_id") == []


def test_get_test_unique_ids_covers_all_declared_tests(tmp_path):
    registry = _build_registry(tmp_path)
    ids = registry.get_test_unique_ids()
    # not_null + unique + relationships + the unattributable custom test = 4 declared nodes.
    assert len(ids) == 4
    assert any("relationships" in i for i in ids)
