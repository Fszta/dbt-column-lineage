"""Regression tests for the manifest-seeded model universe.

Before this fix the registry built its model universe *only* from ``catalog.json``,
so any model absent from the catalog was invisible to impact analysis and, in the
two-manifest diff, misreported as removed or "not built in the warehouse yet". This
is the norm under a deferred / partial CI build (``dbt docs generate --defer`` after
building just the ``state:modified+`` cone) and for non-table relations such as
semantic views.

These tests pin the corrected behaviour end to end:

- a built-but-uncatalogued downstream that projects an upstream column IS reported as
  affected (not dropped, not ``removed``, not counted as "not built");
- a model absent from the catalog but present in the manifest is seeded and analyzed
  via its compiled SQL (its column types merely unknown);
- a genuinely unanalyzable model (no catalog columns AND no parseable SQL — e.g. a
  semantic view) is honestly labelled, never claimed to be "not built";
- a model truly absent from the head *manifest* is still reported as removed.
"""

import json

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.lineage.service import LineageService
from dbt_column_lineage.lineage.changeset import ChangeKind, ChangesetBuilder
from dbt_column_lineage.lineage.display.markdown import _confidence_reasons


# --- fixture helpers -------------------------------------------------------


def _catalog_node(name, columns, schema="s", database="d"):
    return {
        "unique_id": f"model.p.{name}",
        "metadata": {"name": name, "schema": schema, "database": database, "type": "BASE TABLE"},
        "columns": {c: {"name": c, "type": "TEXT"} for c in columns},
    }


def _manifest_node(name, compiled=None, depends_on=None, schema="s", database="d", language="sql"):
    node = {
        "name": name,
        "unique_id": f"model.p.{name}",
        "resource_type": "model",
        "language": language,
        "schema": schema,
        "database": database,
        "depends_on": {"nodes": [f"model.p.{d}" for d in (depends_on or [])]},
    }
    if compiled is not None:
        node["compiled_code"] = compiled
    return node


def _write(tmp_path, tag, catalog_nodes, manifest_nodes):
    catalog_path = tmp_path / f"{tag}_catalog.json"
    manifest_path = tmp_path / f"{tag}_manifest.json"
    catalog_path.write_text(json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": catalog_nodes}))
    manifest_path.write_text(
        json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": manifest_nodes})
    )
    return str(catalog_path), str(manifest_path)


# accounts: a mart whose compiled SQL changes (logic edit) between base and head.
_ACCOUNTS_BASE = (
    "select dim.account_status as account_status, "
    "dim.account_holder_id as account_holder_id, "
    "dim.first_verified_at as first_verified_at from d.s.dim_accounts as dim"
)
_ACCOUNTS_HEAD = (
    "select coalesce(dim.account_status, dim.fallback_status) as account_status, "
    "dim.account_holder_id as account_holder_id, "
    "dim.first_verified_at as first_verified_at from d.s.dim_accounts as dim"
)

# int_breach: a DIRECT consumer that projects an accounts-derived column. Its OWN
# compiled SQL is identical base vs head, so it is only ever flagged via the accounts
# fan-out — exactly the case the old catalog-only universe dropped when int was absent
# from the head catalog.
_INT_BREACH = (
    "with accounts as (select * from d.s.accounts), "
    "final as (select min(accounts.first_verified_at) as reporting_month from accounts) "
    "select * from final"
)


def _accounts_manifest(compiled):
    return {
        "model.p.dim_accounts": _manifest_node("dim_accounts", compiled="select 1 as account_status"),
        "model.p.accounts": _manifest_node("accounts", compiled=compiled, depends_on=["dim_accounts"]),
        "model.p.int_breach": _manifest_node("int_breach", compiled=_INT_BREACH, depends_on=["accounts"]),
    }


def _base_registry(tmp_path):
    catalog = {
        "model.p.dim_accounts": _catalog_node("dim_accounts", ["account_status"]),
        "model.p.accounts": _catalog_node(
            "accounts", ["account_status", "account_holder_id", "first_verified_at"]
        ),
        "model.p.int_breach": _catalog_node("int_breach", ["reporting_month"]),
    }
    cat, man = _write(tmp_path, "base", catalog, _accounts_manifest(_ACCOUNTS_BASE))
    return LineageService(cat, man, adapter="snowflake")


def _head_registry(tmp_path, int_in_catalog):
    catalog = {
        "model.p.dim_accounts": _catalog_node("dim_accounts", ["account_status"]),
        "model.p.accounts": _catalog_node(
            "accounts", ["account_status", "account_holder_id", "first_verified_at"]
        ),
    }
    if int_in_catalog:
        catalog["model.p.int_breach"] = _catalog_node("int_breach", ["reporting_month"])
    cat, man = _write(tmp_path, "head", catalog, _accounts_manifest(_ACCOUNTS_HEAD))
    return LineageService(cat, man, adapter="snowflake")


# --- Issue A: built-but-uncatalogued downstream is reported, not dropped ----


def test_uncatalogued_downstream_projecting_upstream_column_is_reported(tmp_path):
    """The disputed case: int_breach is built (in head manifest) but absent from the
    head catalog. It projects an accounts-derived column, so a logic change on accounts
    must surface it as affected — NOT drop it, NOT flag it removed, NOT call it unbuilt."""
    base = _base_registry(tmp_path)
    head = _head_registry(tmp_path, int_in_catalog=False)

    # int_breach is manifest-seeded with columns recovered from its compiled SQL.
    int_model = head.registry.get_model("int_breach")
    assert list(int_model.columns) == ["reporting_month"]
    assert head.registry.is_catalog_backed("int_breach") is False
    assert head.registry.is_catalog_backed("accounts") is True

    changes = ChangesetBuilder(base.registry, head.registry).build()
    # Not misclassified as a deletion just because it left the head catalog.
    assert not any(
        c.model == "int_breach" and c.kind == ChangeKind.REMOVED for c in changes
    )
    # accounts logic changed -> all its columns flagged.
    assert {c.column for c in changes if c.model == "accounts"} == {
        "account_status",
        "account_holder_id",
        "first_verified_at",
    }

    agg = head.get_changeset_impact(changes, base_service=base)
    affected = {m["name"] for m in agg["affected_models"]}
    assert "int_breach" in affected
    assert ("int_breach", "reporting_month") in {
        (c["model"], c["column"]) for c in agg["affected_columns"]
    }
    # Nothing reachable was unanalyzable: int_breach was analyzed via its compiled SQL.
    conf = agg["confidence"]
    assert conf["level"] == "full"
    assert conf["unanalyzable_models"] == 0
    assert conf["no_column_info"] == 0
    assert conf["parse_failed"] == 0


def test_catalogued_and_uncatalogued_downstream_yield_same_impact(tmp_path):
    """Whether int_breach happens to be in the head catalog or not, the impact result
    is identical — catalog membership no longer changes analyzability."""
    base_a = _base_registry(tmp_path)
    head_catalogued = _head_registry(tmp_path, int_in_catalog=True)
    changes_a = ChangesetBuilder(base_a.registry, head_catalogued.registry).build()
    impact_catalogued = head_catalogued.get_changeset_impact(changes_a, base_service=base_a)

    base_b = _base_registry(tmp_path)
    head_uncatalogued = _head_registry(tmp_path, int_in_catalog=False)
    changes_b = ChangesetBuilder(base_b.registry, head_uncatalogued.registry).build()
    impact_uncatalogued = head_uncatalogued.get_changeset_impact(changes_b, base_service=base_b)

    def _affected(impact):
        return (
            {m["name"] for m in impact["affected_models"]},
            {(c["model"], c["column"]) for c in impact["affected_columns"]},
        )

    assert _affected(impact_catalogued) == _affected(impact_uncatalogued)


# --- registry seeding / coverage -------------------------------------------


def test_manifest_only_model_is_seeded_and_parsed(tmp_path):
    """A model present in the manifest but absent from the catalog is registered and
    analyzed from its compiled SQL; coverage still honestly reports the catalog gap."""
    catalog = {"model.p.a": _catalog_node("a", ["id"])}
    manifest = {
        "model.p.a": _manifest_node("a", compiled="select 1 as id"),
        "model.p.b": _manifest_node("b", compiled="select a.id as id from d.s.a as a", depends_on=["a"]),
    }
    cat, man = _write(tmp_path, "only", catalog, manifest)
    registry = ModelRegistry(cat, man, adapter_override="snowflake")
    registry.load()

    models = registry.get_models()
    assert "b" in models  # seeded from the manifest despite being absent from the catalog
    assert list(models["b"].columns) == ["id"]  # columns recovered from compiled SQL
    assert models["b"].columns["id"].data_type is None  # type unknown, honestly
    assert registry.is_catalog_backed("b") is False

    coverage = registry.get_coverage()
    assert coverage.models_in_manifest == 2
    assert coverage.models_in_catalog == 1  # only 'a' is catalog-backed
    assert coverage.not_in_catalog_count == 1
    assert coverage.parsed_ok == 2  # both parsed; catalog gap != analyzability gap


def test_unanalyzable_model_is_labelled_honestly_not_as_unbuilt(tmp_path):
    """A reachable downstream with neither catalog columns nor parseable SQL (e.g. a
    semantic view / python model) is reported as unanalyzable via ``no_column_info``,
    and the rendered reason never claims it 'hasn't been built'."""
    # 'sv' is a downstream of 'a' with no compiled SQL and no catalog entry.
    catalog = {"model.p.a": _catalog_node("a", ["id"])}
    manifest = {
        "model.p.a": _manifest_node("a", compiled="select 1 as id"),
        "model.p.sv": _manifest_node("sv", compiled=None, depends_on=["a"]),
    }
    cat, man = _write(tmp_path, "sv", catalog, manifest)
    service = LineageService(cat, man, adapter="snowflake")

    impact = service.get_column_impact("a", "id")
    conf = impact["confidence"]
    assert conf["level"] == "partial"
    assert conf["no_column_info"] == 1
    assert "sv" in conf["no_column_info_models"]
    assert conf["parse_failed"] == 0

    # The rendered reason must be honest: no column-level info, never "not built".
    reason = _confidence_reasons(conf)
    assert "no column-level information" in reason
    assert "semantic view" in reason
    assert "haven't been built in the warehouse yet" not in reason


def test_model_absent_from_head_manifest_is_still_removed(tmp_path):
    """Guard the other direction: a model truly gone from the head *manifest* (not
    merely from the catalog) is still classified as removed."""
    base_catalog = {
        "model.p.a": _catalog_node("a", ["id"]),
        "model.p.gone": _catalog_node("gone", ["x"]),
    }
    base_manifest = {
        "model.p.a": _manifest_node("a", compiled="select 1 as id"),
        "model.p.gone": _manifest_node("gone", compiled="select 2 as x"),
    }
    head_catalog = {"model.p.a": _catalog_node("a", ["id"])}
    head_manifest = {"model.p.a": _manifest_node("a", compiled="select 1 as id")}

    bcat, bman = _write(tmp_path, "rmbase", base_catalog, base_manifest)
    hcat, hman = _write(tmp_path, "rmhead", head_catalog, head_manifest)
    base = LineageService(bcat, bman, adapter="snowflake")
    head = LineageService(hcat, hman, adapter="snowflake")

    changes = ChangesetBuilder(base.registry, head.registry).build()
    assert any(
        c.model == "gone" and c.column == "x" and c.kind == ChangeKind.REMOVED for c in changes
    )


# --- Issue B: join/filter-only dependencies are out of scope (by design) ----


def test_join_or_filter_only_dependency_is_not_reported_by_column_lineage(tmp_path):
    """A downstream that references the changed model only in a JOIN/WHERE (projecting
    no column from it) is not surfaced — column-propagation lineage is by design. This
    test documents/locks that boundary so a future change is a conscious decision."""
    accounts_head = (
        "select coalesce(dim.status, dim.fallback) as status, dim.id as id "
        "from d.s.dim as dim"
    )
    accounts_base = "select dim.status as status, dim.id as id from d.s.dim as dim"
    join_only = (
        "with events as (select * from d.s.events), "
        "accounts as (select * from d.s.accounts), "
        "final as (select events.event_id as event_id from events "
        "join accounts on events.id = accounts.id where accounts.status = 'x') "
        "select * from final"
    )
    manifest = lambda acc: {
        "model.p.dim": _manifest_node("dim", compiled="select 1 as status"),
        "model.p.events": _manifest_node("events", compiled="select 1 as event_id, 2 as id"),
        "model.p.accounts": _manifest_node("accounts", compiled=acc, depends_on=["dim"]),
        "model.p.usage": _manifest_node("usage", compiled=join_only, depends_on=["accounts", "events"]),
    }
    catalog = {
        "model.p.dim": _catalog_node("dim", ["status"]),
        "model.p.events": _catalog_node("events", ["event_id", "id"]),
        "model.p.accounts": _catalog_node("accounts", ["status", "id"]),
        "model.p.usage": _catalog_node("usage", ["event_id"]),
    }
    bcat, bman = _write(tmp_path, "jbase", catalog, manifest(accounts_base))
    hcat, hman = _write(tmp_path, "jhead", catalog, manifest(accounts_head))
    base = LineageService(bcat, bman, adapter="snowflake")
    head = LineageService(hcat, hman, adapter="snowflake")

    changes = ChangesetBuilder(base.registry, head.registry).build()
    agg = head.get_changeset_impact(changes, base_service=base)
    # 'usage' joins/filters on accounts but projects no accounts column -> not reported.
    assert "usage" not in {m["name"] for m in agg["affected_models"]}
    assert "usage" in head.registry.get_model("accounts").downstream
