"""— end-to-end extract against the fake client, plus artifact round-trip."""

from __future__ import annotations

import pytest

from parrant.metabase.artifact import (
    MetabaseArtifactError,
    dump_metabase_lineage,
    load_metabase_lineage,
)
from parrant.metabase.client import MetabaseClient
from parrant.metabase.extract import ExtractConfig, coverage_ratio, run_extract
from tests.unit.metabase._fixtures import FakeSession, build_recorded, load_recorded

DIM_ACCOUNTS = "analytics.marts_finance.dim_accounts"
FACT_REVENUE = "analytics.marts_finance.fact_revenue"


def _run(dashboard_meta=None):
    session = FakeSession(load_recorded())
    client = MetabaseClient(
        base_url="https://metabase.example.com",
        api_key="k",
        session=session,
        sleep=lambda _s: None,
    )
    config = ExtractConfig(
        metabase_base_url="https://metabase.example.com",
        database_ids=[2],
        extractor_version="9.9.9",
        dialect="snowflake",
        dashboard_meta=dashboard_meta or {},
    )
    return run_extract(config, client)


def test_extract_produces_expected_artifact_shape():
    lineage = _run()

    # v2: cards/dashboards now carry ``updated_at`` for incremental reuse.
    assert lineage.schema_version == 2
    assert lineage.provenance.metabase_base_url == "https://metabase.example.com"
    assert lineage.provenance.metabase_version == "v0.51.6"
    assert lineage.provenance.database_ids == [2]
    assert lineage.provenance.dbt_adapter == "snowflake"
    # archived card 3500 is excluded by default.
    card_ids = {c.card_id for c in lineage.cards}
    assert card_ids == {128, 3391, 3402, 3403}


def test_extract_stamps_updated_at_on_cards_and_dashboards():
    lineage = _run()

    # schema_version 2 stamps a last-modified marker on every card and dashboard so a later
    # extract can decide reuse vs refetch.
    assert lineage.schema_version == 2
    assert all(c.updated_at == "2024-01-01T00:00:00Z" for c in lineage.cards)
    assert lineage.dashboards  # sanity: at least one dashboard survived filtering
    assert all(d.updated_at == "2024-01-01T00:00:00Z" for d in lineage.dashboards)


def test_extract_resolves_card_to_relation_column_edges():
    lineage = _run()
    cards = {c.card_id: c for c in lineage.cards}

    mbql = cards[128]
    edges = {(c.relation, c.column) for c in mbql.columns}
    assert (DIM_ACCOUNTS, "country_code") in edges
    assert (DIM_ACCOUNTS, "account_status") in edges

    native = cards[3402]
    edges = {(c.relation, c.column) for c in native.columns}
    assert (FACT_REVENUE, "amount") in edges

    # referenced relations are de-duplicated into the top-level relations map.
    assert DIM_ACCOUNTS in lineage.relations
    assert lineage.relations[DIM_ACCOUNTS].schema_name == "marts_finance"


def test_coverage_counts_are_honest():
    lineage = _run()
    cov = lineage.coverage
    assert cov.cards_total == 4
    assert cov.cards_resolved_column >= 2  # 128 (mbql) + 3402 (clean native)
    assert 3391 in cov.table_only_card_ids  # select * degrade
    assert coverage_ratio(cov) == pytest.approx(1.0)


def test_dashboard_meta_is_consumer_configurable():
    # tier is NOT hardcoded — it comes from the consumer mapping keyed by collection.
    mapping = {"by_collection": {"4": {"tier": "executive", "source": "metabase"}}}
    lineage = _run(dashboard_meta=mapping)

    dashboard = next(d for d in lineage.dashboards if d.dashboard_id == 55)
    assert dashboard.card_ids == [128, 3391]
    assert dashboard.meta == {"tier": "executive", "source": "metabase"}
    assert dashboard.url == "https://metabase.example.com/dashboard/55"


def test_dashboard_meta_absent_is_empty_dict():
    lineage = _run()
    dashboard = next(d for d in lineage.dashboards if d.dashboard_id == 55)
    assert dashboard.meta == {}


def test_artifact_round_trip(tmp_path):
    lineage = _run()
    path = tmp_path / "metabase_lineage.json"
    dump_metabase_lineage(lineage, path)

    reloaded = load_metabase_lineage(path)
    assert reloaded is not None
    assert reloaded.model_dump() == lineage.model_dump()
    # by-alias emits "schema" for the relation's schema_name field.
    assert '"schema"' in path.read_text(encoding="utf-8")


def test_load_missing_artifact_is_none():
    assert load_metabase_lineage(None) is None
    assert load_metabase_lineage("/nonexistent/metabase_lineage.json") is None


def test_load_incompatible_schema_version_raises(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text('{"schema_version": 999}', encoding="utf-8")
    with pytest.raises(MetabaseArtifactError):
        load_metabase_lineage(path)


# --- connection scoping ----------------------------------------------------

_DB_META_2 = {
    "2": {
        "id": 2,
        "name": "Analytics",
        "engine": "snowflake",
        "details": {"db": "ANALYTICS"},
        "tables": [
            {
                "id": 200,
                "name": "FACT_REVENUE",
                "schema": "MARTS_FINANCE",
                "db_id": 2,
                "fields": [
                    {"id": 2001, "name": "AMOUNT", "base_type": "type/Float"},
                    {"id": 2003, "name": "REGION", "base_type": "type/Text"},
                ],
            }
        ],
    }
}


def _mbql_card(card_id: int, database: int, updated_at: str = "2024-01-01T00:00:00Z") -> dict:
    return {
        "id": card_id,
        "name": f"card {card_id}",
        "collection_id": 1,
        "archived": False,
        "updated_at": updated_at,
        "dataset_query": {
            "type": "query",
            "database": database,
            "query": {"source-table": 200, "aggregation": [["count"]]},
        },
    }


def test_connection_filter_drops_foreign_db_card_and_orphaned_dashboard():
    # One card on the scoped database (2) and one on a foreign database (99). The extract is
    # scoped to db 2 only, so the foreign card must be filtered out — absent from cards, not
    # counted in coverage — and a dashboard that referenced only the foreign card is dropped.
    recorded = build_recorded(
        cards=[_mbql_card(1, database=2), _mbql_card(2, database=99)],
        dashboards=[
            {"id": 10, "name": "scoped dash", "collection_id": 1, "updated_at": "u10"},
            {"id": 20, "name": "foreign dash", "collection_id": 1, "updated_at": "u20"},
        ],
        dashboard_details={
            "10": {"id": 10, "dashcards": [{"card_id": 1, "card": {"id": 1}}]},
            "20": {"id": 20, "dashcards": [{"card_id": 2, "card": {"id": 2}}]},
        },
        database_metadata=_DB_META_2,
    )
    session = FakeSession(recorded)
    client = MetabaseClient(
        base_url="https://metabase.example.com",
        api_key="k",
        session=session,
        sleep=lambda _s: None,
    )
    config = ExtractConfig(
        metabase_base_url="https://metabase.example.com",
        database_ids=[2],
        extractor_version="9.9.9",
        dialect="snowflake",
    )
    lineage = run_extract(config, client)

    card_ids = {c.card_id for c in lineage.cards}
    assert card_ids == {1}  # foreign-db card 2 filtered out
    assert lineage.coverage.cards_total == 1  # not counted in coverage

    dash_ids = {d.dashboard_id for d in lineage.dashboards}
    assert dash_ids == {10}  # dashboard 20 (only the foreign card) dropped
    assert next(d for d in lineage.dashboards if d.dashboard_id == 10).card_ids == [1]
