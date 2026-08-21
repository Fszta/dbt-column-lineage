"""Integration tests: ingest dbt test nodes from the REAL regenerated manifest.

These drive the actual ``dbt_test_project`` artifacts produced by ``dbt docs generate``
(via the session ``dbt_artifacts`` fixture), so they lock in the real dbt 1.10 test-node
shape rather than a hand-written approximation.
"""

from dbt_column_lineage.artifacts.manifest import ManifestReader
from dbt_column_lineage.artifacts.registry import ModelRegistry


def test_manifest_contains_test_nodes(dbt_artifacts):
    reader = ManifestReader(str(dbt_artifacts["manifest_path"]))
    reader.load()

    tests = reader.get_tests()
    assert tests, "expected the regenerated manifest to contain test nodes"

    names = {t.test_name for t in tests}
    assert {"not_null", "unique", "relationships"}.issubset(names)

    # Every attributed test must name a lowercase model + column; unattributed keep None.
    for t in tests:
        if t.target_model is not None:
            assert t.target_model == t.target_model.lower()
        if t.target_column is not None:
            assert t.target_column == t.target_column.lower()


def test_relationships_test_carries_referenced_side(dbt_artifacts):
    reader = ManifestReader(str(dbt_artifacts["manifest_path"]))
    reader.load()

    rels = [t for t in reader.get_tests() if t.test_name == "relationships"]
    assert rels, "expected at least one relationships test in the fixture"

    # stg_transactions.account_id -> stg_accounts.account_id
    match = [
        t for t in rels if t.target_model == "stg_transactions" and t.target_column == "account_id"
    ]
    assert match, "expected the stg_transactions.account_id relationships test"
    t = match[0]
    assert t.referenced_model == "stg_accounts"
    assert t.referenced_column == "account_id"


def test_registry_column_tests_reverse_index(dbt_artifacts):
    registry = ModelRegistry(
        str(dbt_artifacts["catalog_path"]), str(dbt_artifacts["manifest_path"])
    )
    registry.load()

    not_null_unique = registry.get_column_tests("stg_transactions", "transaction_id")
    assert {t.test_name for t in not_null_unique} == {"not_null", "unique"}

    # Case-insensitive lookup.
    assert registry.get_column_tests("STG_TRANSACTIONS", "TRANSACTION_ID")

    # Unknown (model, column) returns empty.
    assert registry.get_column_tests("stg_transactions", "not_a_column") == []
    assert registry.get_column_tests("no_such_model", "whatever") == []

    # Coverage honesty: unattributable count is available (0 for this fixture).
    assert registry.get_unattributable_test_count() == 0
