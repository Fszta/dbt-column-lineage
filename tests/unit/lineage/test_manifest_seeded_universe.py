"""Regression tests for the manifest-seeded model universe.

Before this fix the registry built its model universe *only* from ``catalog.json``,
so any model absent from the catalog was invisible to impact analysis and, in the
two-manifest diff, misreported as removed or "not built in the warehouse yet". This
is the norm under a deferred / partial CI build (``dbt docs generate --defer`` after
building just the ``state:modified+`` cone) and for non-table relations such as
semantic views.

The real analytics run exercised TWO distinct shapes that the fix must both get right,
mirrored faithfully below:

Shape 1 — filter/join-only consumer (the real ``int_risk_kpis_breach_rate``): reads
``accounts`` but uses its columns ONLY in ``WHERE`` / join / aggregate predicates and
projects NO ``accounts`` column value to its output (outputs are ``count(*)``-based
ratios plus ``reporting_month`` sourced from ``account_holders``). Under column-
propagation lineage it must remain EXCLUDED from the accounts blast radius — reporting
it would be an Issue-B over-report. This test guards that the fix did not start over-
reporting it.

Shape 2 — projecting, genuinely catalog-absent consumer (e.g. ``int_capital_deposit_otif``
or ``identifications_and_onboardings_semantic_view``): projects an ``accounts`` column
value to its output AND is absent from the head catalog (deferred / not-built, so it is
manifest-only). Before the fix it was dropped, mislabeled "not built", and reported as
``removed``; after the fix it is reported as affected, ``removed`` is false, and it does
not pollute the unanalyzable bucket.

A genuinely unanalyzable relation (no catalog columns AND no parseable compiled SQL) is
honestly labelled ("no column-level information", never "not built").
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


def _manifest_node(
    name,
    compiled=None,
    depends_on=None,
    schema="s",
    database="d",
    language="sql",
    materialized="table",
):
    node = {
        "name": name,
        "unique_id": f"model.p.{name}",
        "resource_type": "model",
        "language": language,
        "schema": schema,
        "database": database,
        "config": {"materialized": materialized},
        "depends_on": {"nodes": [f"model.p.{d}" for d in (depends_on or [])]},
    }
    if compiled is not None:
        node["compiled_code"] = compiled
    return node


def _write(tmp_path, tag, catalog_nodes, manifest_nodes):
    catalog_path = tmp_path / f"{tag}_catalog.json"
    manifest_path = tmp_path / f"{tag}_manifest.json"
    catalog_path.write_text(
        json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": catalog_nodes})
    )
    manifest_path.write_text(
        json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": manifest_nodes})
    )
    return str(catalog_path), str(manifest_path)


# --- the analytics scenario ------------------------------------------------

_ACCOUNTS_COLUMNS = [
    "account_holder_id",
    "account_status",
    "last_suspended_at",
    "account_closing_at",
    "account_closed_at",
    "account_internal_closing_reason_type",
]

# accounts: a mart whose compiled SQL changes (a pure logic edit to account_status).
_ACCOUNTS_BASE = (
    "select "
    "dim.account_holder_id as account_holder_id, "
    "dim.account_status as account_status, "
    "dim.last_suspended_at as last_suspended_at, "
    "dim.account_closing_at as account_closing_at, "
    "dim.account_closed_at as account_closed_at, "
    "dim.account_internal_closing_reason_type as account_internal_closing_reason_type "
    "from {db}.{schema}.dim_accounts as dim"
)
_ACCOUNTS_HEAD = (
    "select "
    "dim.account_holder_id as account_holder_id, "
    "coalesce(dim.account_status, dim.fallback_status) as account_status, "
    "dim.last_suspended_at as last_suspended_at, "
    "dim.account_closing_at as account_closing_at, "
    "dim.account_closed_at as account_closed_at, "
    "dim.account_internal_closing_reason_type as account_internal_closing_reason_type "
    "from {db}.{schema}.dim_accounts as dim"
)

# Shape 1: int_risk_kpis_breach_rate. accounts columns appear ONLY in the WHERE of the
# `suspended` CTE; the projected outputs are reporting_month (from account_holders) and
# two count(*)-based ratios. No accounts column VALUE reaches an output column.
_BREACH_RATE = (
    "with "
    "account_holders as (select * from {db}.{schema}.account_holders), "
    "accounts as (select * from {db}.{schema}.accounts), "
    "suspended as ("
    "  select "
    "    date_trunc('month', account_holders.first_verified_at) as reporting_month, "
    "    count(*) as breached_count "
    "  from account_holders "
    "  inner join accounts on accounts.account_holder_id = account_holders.account_holder_id "
    "  where accounts.account_status = 'Suspended' "
    "     or (accounts.account_status in ('Closing', 'Closed') "
    "         and accounts.account_internal_closing_reason_type in ('fraud')) "
    "  group by 1"
    "), "
    "cohort as ("
    "  select "
    "    date_trunc('month', account_holders.first_verified_at) as reporting_month, "
    "    count(*) as total_count "
    "  from account_holders "
    "  group by 1"
    "), "
    "final as ("
    "  select "
    "    cohort.reporting_month as reporting_month, "
    "    suspended.breached_count / nullif(cohort.total_count, 0) as m0_breach_rate, "
    "    suspended.breached_count / nullif(cohort.total_count, 0) as m3_breach_rate "
    "  from cohort "
    "  left join suspended on cohort.reporting_month = suspended.reporting_month"
    ") "
    "select * from final"
)

# Shape 2: int_capital_deposit_otif. Projects accounts.account_holder_id to its output.
_OTIF = (
    "with "
    "accounts as (select * from {db}.{schema}.accounts), "
    "final as ("
    "  select accounts.account_holder_id as account_holder_id, true as is_otif from accounts"
    ") "
    "select * from final"
)

# A real semantic_view that projects accounts columns (compiled to a SELECT here).
_SEMANTIC_VIEW = (
    "with "
    "accounts as (select * from {db}.{schema}.accounts), "
    "final as (select accounts.account_holder_id as account_holder_id from accounts) "
    "select * from final"
)


def _manifest_nodes(env, accounts_sql, *, otif_in_manifest=True):
    """Build the manifest node set for one environment ('QA' head or 'PRD' base)."""
    db = "ANALYTICS_QA" if env == "QA" else "ANALYTICS_PRD"
    prefix = "MR_758_validate" if env == "QA" else "PRD"

    def sch(layer):
        return f"{prefix}_{layer}"

    nodes = {
        "model.p.dim_accounts": _manifest_node(
            "dim_accounts", compiled="select 1 as account_status", schema=sch("star"), database=db
        ),
        "model.p.account_holders": _manifest_node(
            "account_holders",
            compiled="select 1 as account_holder_id, current_date as first_verified_at",
            schema=sch("star"),
            database=db,
        ),
        "model.p.accounts": _manifest_node(
            "accounts",
            compiled=accounts_sql.format(db=db, schema=sch("marts")),
            depends_on=["dim_accounts"],
            schema=sch("marts"),
            database=db,
        ),
        "model.p.int_risk_kpis_breach_rate": _manifest_node(
            "int_risk_kpis_breach_rate",
            # neutral relation refs -> identical text both envs, so it is never flagged
            # by its OWN logic diff; the only way it could surface is the accounts fan-out.
            compiled=_BREACH_RATE.format(db="analytics", schema="marts"),
            depends_on=["accounts", "account_holders"],
            schema=sch("metrics"),
            database=db,
        ),
        "model.p.int_capital_deposit_otif": _manifest_node(
            "int_capital_deposit_otif",
            compiled=_OTIF.format(db=db, schema=sch("marts")),
            depends_on=["accounts"],
            schema=sch("intermediate"),
            database=db,
        ),
        "model.p.identifications_and_onboardings_semantic_view": _manifest_node(
            "identifications_and_onboardings_semantic_view",
            compiled=_SEMANTIC_VIEW.format(db=db, schema=sch("marts")),
            depends_on=["accounts"],
            schema=sch("marts"),
            database=db,
            materialized="semantic_view",
        ),
    }
    if not otif_in_manifest:
        del nodes["model.p.int_capital_deposit_otif"]
    return nodes


def _catalog_nodes(env, *, include_absent):
    """Catalog for one environment. When include_absent is False, the two projecting
    consumers (otif + semantic view) are omitted, modelling a deferred/partial build
    where they were NOT written to the catalog even though they are in the manifest."""
    prefix = "MR_758_validate" if env == "QA" else "PRD"
    db = "ANALYTICS_QA" if env == "QA" else "ANALYTICS_PRD"

    def sch(layer):
        return f"{prefix}_{layer}"

    nodes = {
        "model.p.dim_accounts": _catalog_node("dim_accounts", ["account_status"], sch("star"), db),
        "model.p.account_holders": _catalog_node(
            "account_holders", ["account_holder_id", "first_verified_at"], sch("star"), db
        ),
        "model.p.accounts": _catalog_node("accounts", _ACCOUNTS_COLUMNS, sch("marts"), db),
        # breach_rate is in the built modified+ cone, so it IS catalogued in both envs.
        "model.p.int_risk_kpis_breach_rate": _catalog_node(
            "int_risk_kpis_breach_rate",
            ["reporting_month", "m0_breach_rate", "m3_breach_rate"],
            sch("metrics"),
            db,
        ),
    }
    if include_absent:
        nodes["model.p.int_capital_deposit_otif"] = _catalog_node(
            "int_capital_deposit_otif", ["account_holder_id", "is_otif"], sch("intermediate"), db
        )
        nodes["model.p.identifications_and_onboardings_semantic_view"] = _catalog_node(
            "identifications_and_onboardings_semantic_view",
            ["account_holder_id"],
            sch("marts"),
            db,
        )
    return nodes


def _base(tmp_path):
    """Base (prod) service: everything built & catalogued in ANALYTICS_PRD.PRD_*."""
    cat, man = _write(
        tmp_path, "base", _catalog_nodes("PRD", include_absent=True), _manifest_nodes("PRD", _ACCOUNTS_BASE)
    )
    return LineageService(cat, man, adapter="snowflake")


def _head(tmp_path):
    """Head (QA) service: accounts + breach_rate built & catalogued in
    ANALYTICS_QA.MR_758_validate_*; otif + semantic view are manifest-only (deferred /
    not built, so absent from the head catalog). Same unique_ids as base, divergent
    database + schema — exercising the multi-schema / multi-database divergence."""
    cat, man = _write(
        tmp_path, "head", _catalog_nodes("QA", include_absent=False), _manifest_nodes("QA", _ACCOUNTS_HEAD)
    )
    return LineageService(cat, man, adapter="snowflake")


def _changeset_impact(tmp_path):
    base = _base(tmp_path)
    head = _head(tmp_path)
    changes = ChangesetBuilder(base.registry, head.registry).build()
    agg = head.get_changeset_impact(changes, base_service=base)
    return base, head, changes, agg


# --- Shape 1: filter/join-only consumer must stay EXCLUDED ------------------


def test_shape1_filter_join_only_consumer_stays_excluded(tmp_path):
    base, head, changes, agg = _changeset_impact(tmp_path)

    # breach_rate is catalog-backed (it was in the built cone) and fully analyzable.
    assert head.registry.is_catalog_backed("int_risk_kpis_breach_rate") is True
    breach = head.registry.get_model("int_risk_kpis_breach_rate")
    # No output column of breach_rate carries any accounts.* value.
    all_sources = {
        src
        for col in breach.columns.values()
        for lineage in (col.lineage or [])
        for src in lineage.source_columns
    }
    assert not any(s.startswith("accounts.") for s in all_sources), all_sources

    # It must NOT appear as a downstream consumer of any accounts column.
    for column in ("account_status", "last_suspended_at", "account_internal_closing_reason_type"):
        impact = head.get_column_impact("accounts", column)
        assert "int_risk_kpis_breach_rate" not in {m["name"] for m in impact["affected_models"]}

    # ... nor in the aggregated blast radius, and not misclassified as removed.
    assert "int_risk_kpis_breach_rate" not in {m["name"] for m in agg["affected_models"]}
    assert not any(
        c.model == "int_risk_kpis_breach_rate" and c.kind == ChangeKind.REMOVED for c in changes
    )
    # Being analyzable, it does not inflate the unanalyzable/confidence buckets.
    conf = agg["confidence"]
    assert "int_risk_kpis_breach_rate" not in conf["no_column_info_models"]
    assert "int_risk_kpis_breach_rate" not in conf["parse_failed_models"]


# --- Shape 2: projecting, catalog-absent consumer must be INCLUDED ----------


def test_shape2_projecting_catalog_absent_consumer_is_included(tmp_path):
    base, head, changes, agg = _changeset_impact(tmp_path)

    # otif is manifest-only in head (absent from the head catalog), across a divergent
    # database + schema from base — matched by unique_id/name all the same.
    assert head.registry.is_catalog_backed("int_capital_deposit_otif") is False
    otif = head.registry.get_model("int_capital_deposit_otif")
    assert otif.database == "ANALYTICS_QA" and otif.schema_name == "MR_758_validate_intermediate"
    assert base.registry.get_model("int_capital_deposit_otif").database == "ANALYTICS_PRD"
    # Columns recovered from compiled SQL; account_holder_id traces to accounts.
    assert otif.columns["account_holder_id"].lineage[0].source_columns == {
        "accounts.account_holder_id"
    }

    affected = {m["name"] for m in agg["affected_models"]}
    assert "int_capital_deposit_otif" in affected
    assert ("int_capital_deposit_otif", "account_holder_id") in {
        (c["model"], c["column"]) for c in agg["affected_columns"]
    }
    # Not misclassified as removed just because it left the head catalog.
    assert not any(
        c.model == "int_capital_deposit_otif" and c.kind == ChangeKind.REMOVED for c in changes
    )
    # It is analyzable via parsed SQL, so it does not land in any unanalyzable bucket.
    conf = agg["confidence"]
    assert "int_capital_deposit_otif" not in conf["no_column_info_models"]
    assert "int_capital_deposit_otif" not in conf["parse_failed_models"]


def test_shape2_semantic_view_projecting_accounts_is_included(tmp_path):
    """A ``materialized: semantic_view`` relation that is absent from the catalog but
    whose compiled SQL projects an accounts column is recovered and reported — the
    coordinator's cited ``identifications_and_onboardings_semantic_view`` analogue."""
    base, head, changes, agg = _changeset_impact(tmp_path)

    sv = "identifications_and_onboardings_semantic_view"
    assert head.registry.is_catalog_backed(sv) is False
    assert sv in {m["name"] for m in agg["affected_models"]}
    assert (sv, "account_holder_id") in {
        (c["model"], c["column"]) for c in agg["affected_columns"]
    }
    assert not any(c.model == sv and c.kind == ChangeKind.REMOVED for c in changes)


# --- registry seeding / coverage -------------------------------------------


def test_manifest_only_model_is_seeded_and_parsed(tmp_path):
    """A model present in the manifest but absent from the catalog is registered and
    analyzed from its compiled SQL; coverage still honestly reports the catalog gap."""
    catalog = {"model.p.a": _catalog_node("a", ["id"])}
    manifest = {
        "model.p.a": _manifest_node("a", compiled="select 1 as id"),
        "model.p.b": _manifest_node(
            "b", compiled="select a.id as id from d.s.a as a", depends_on=["a"]
        ),
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
    semantic view whose body is not a SELECT, or a python model) is reported as
    unanalyzable via ``no_column_info`` and never claimed to be 'not built'."""
    catalog = {"model.p.a": _catalog_node("a", ["id"])}
    manifest = {
        "model.p.a": _manifest_node("a", compiled="select 1 as id"),
        # no compiled SELECT body to recover columns from.
        "model.p.opaque": _manifest_node("opaque", compiled=None, depends_on=["a"]),
    }
    cat, man = _write(tmp_path, "opaque", catalog, manifest)
    service = LineageService(cat, man, adapter="snowflake")

    impact = service.get_column_impact("a", "id")
    conf = impact["confidence"]
    assert conf["level"] == "partial"
    assert conf["no_column_info"] == 1
    assert "opaque" in conf["no_column_info_models"]
    assert conf["parse_failed"] == 0

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
