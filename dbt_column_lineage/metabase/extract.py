""" — the extract pipeline: fetch → resolve → normalize → coverage-stamp.

All network I/O is behind :class:`~dbt_column_lineage.metabase.client.MetabaseClient`, and
the SQL dialect for the native resolver is supplied by the caller (from the dbt manifest /
``--adapter``). The pipeline itself is pure w.r.t. those inputs, so it runs end-to-end in a
unit test against a fake client with zero live Metabase.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set

from dbt_column_lineage.metabase.client import MetabaseClient
from dbt_column_lineage.metabase.resolvers import CardResolver, ResolvedCard
from dbt_column_lineage.metabase.warehouse_meta import CardCorpus, WarehouseMeta
from dbt_column_lineage.models.schema import (
    MetabaseCard,
    MetabaseCoverage,
    MetabaseDashboard,
    MetabaseLineage,
    MetabaseProvenance,
    MetabaseRelation,
)

# Capacity for the honesty id samples in coverage.
_SAMPLE_CAP = 25


@dataclass
class ExtractConfig:
    """Inputs to :func:`run_extract` that are not the network client itself."""

    metabase_base_url: str
    database_ids: List[int]
    extractor_version: str
    dialect: Optional[str] = None
    include_archived: bool = False
    # Consumer-configurable dashboard meta mapping (spec Q8 /): the tool
    # never hardcodes an org's taxonomy. Shape:
    #   {"by_collection": {<collection_id>: {...}}, "by_dashboard": {<dashboard_id>: {...}}}
    dashboard_meta: Dict[str, Dict[Any, Dict[str, Any]]] = field(default_factory=dict)


def build_dashboard_meta_resolver(
    mapping: Dict[str, Dict[Any, Dict[str, Any]]],
) -> Callable[[dict], Dict[str, Any]]:
    """Turn the consumer mapping into ``dashboard_dict -> meta`` (per-dashboard overrides
    per-collection). Absent → ``{}``; the taxonomy is entirely the consumer's."""
    by_collection = {str(k): v for k, v in (mapping.get("by_collection") or {}).items()}
    by_dashboard = {str(k): v for k, v in (mapping.get("by_dashboard") or {}).items()}

    def resolve(dashboard: dict) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        collection_id = dashboard.get("collection_id")
        if collection_id is not None and str(collection_id) in by_collection:
            meta.update(by_collection[str(collection_id)])
        dashboard_id = dashboard.get("id")
        if dashboard_id is not None and str(dashboard_id) in by_dashboard:
            meta.update(by_dashboard[str(dashboard_id)])
        return meta

    return resolve


def _dashcard_card_ids(dashboard: dict) -> List[int]:
    """Collect card ids from a dashboard's ``dashcards`` (or legacy ``ordered_cards``)."""
    entries = dashboard.get("dashcards")
    if entries is None:
        entries = dashboard.get("ordered_cards") or []
    ids: List[int] = []
    for entry in entries:
        card_id = entry.get("card_id")
        if not isinstance(card_id, int):
            card = entry.get("card") or {}
            card_id = card.get("id")
        if isinstance(card_id, int) and card_id not in ids:
            ids.append(card_id)
    return ids


def _to_card(card: dict, resolved: ResolvedCard) -> MetabaseCard:
    query = card.get("dataset_query") or {}
    kind = "native" if query.get("type") == "native" else "mbql"
    return MetabaseCard(
        card_id=card["id"],
        name=card.get("name") or "",
        query_kind=kind,  # type: ignore[arg-type]
        precision=resolved.precision,  # type: ignore[arg-type]
        collection_id=card.get("collection_id"),
        archived=bool(card.get("archived", False)),
        database_id=query.get("database"),
        columns=resolved.columns,
        table_relations=resolved.table_relations,
        upstream_card_ids=resolved.upstream_card_ids,
        snippet_ids=resolved.snippet_ids,
        unresolved_reason=resolved.unresolved_reason,
    )


def run_extract(config: ExtractConfig, client: MetabaseClient) -> MetabaseLineage:
    """Fetch from Metabase, resolve every card, and assemble the snapshot artifact."""
    # 1. Fetch corpus + warehouse metadata.
    raw_cards = client.list_cards(include_archived=config.include_archived)
    snippets = client.list_snippets()
    metadatas = [client.database_metadata(db_id) for db_id in config.database_ids]
    meta = WarehouseMeta.from_database_metadata(metadatas)
    corpus = CardCorpus(raw_cards, snippets)

    # 2. Resolve cards.
    resolver = CardResolver(meta, corpus, config.dialect)
    cards: List[MetabaseCard] = []
    used_relations: Set[str] = set()
    for raw_card in raw_cards:
        if not isinstance(raw_card.get("id"), int):
            continue
        if raw_card.get("archived") and not config.include_archived:
            continue
        resolved = resolver.resolve_card(raw_card)
        card = _to_card(raw_card, resolved)
        cards.append(card)
        for ref in card.columns:
            used_relations.add(ref.relation)
        used_relations.update(card.table_relations)

    # 3. Attach dashboards (with consumer-supplied meta).
    meta_resolver = build_dashboard_meta_resolver(config.dashboard_meta)
    dashboards: List[MetabaseDashboard] = []
    for summary in client.list_dashboards():
        dashboard_id = summary.get("id")
        if not isinstance(dashboard_id, int):
            continue
        detail = client.get_dashboard(dashboard_id)
        dashboards.append(
            MetabaseDashboard(
                dashboard_id=dashboard_id,
                name=detail.get("name") or summary.get("name") or "",
                collection_id=detail.get("collection_id"),
                url=f"{config.metabase_base_url.rstrip('/')}/dashboard/{dashboard_id}",
                card_ids=_dashcard_card_ids(detail),
                meta=meta_resolver(detail),
            )
        )

    # 4. Relations actually referenced (de-duplicated), + coverage + provenance.
    relations: Dict[str, MetabaseRelation] = {
        key: rel for key, rel in meta.relations.items() if key in used_relations
    }
    coverage = _build_coverage(cards, dashboards, len(snippets))
    provenance = MetabaseProvenance(
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        metabase_base_url=config.metabase_base_url,
        metabase_version=client.server_version(),
        database_ids=list(config.database_ids),
        extractor_version=config.extractor_version,
        dbt_adapter=config.dialect,
    )
    return MetabaseLineage(
        schema_version=1,
        provenance=provenance,
        coverage=coverage,
        relations=relations,
        cards=cards,
        dashboards=dashboards,
    )


def _build_coverage(
    cards: List[MetabaseCard], dashboards: List[MetabaseDashboard], snippets_total: int
) -> MetabaseCoverage:
    column = sum(1 for c in cards if c.precision == "column")
    table_only = sum(1 for c in cards if c.precision == "table")
    unresolved = sum(1 for c in cards if c.precision == "none")
    unresolved_ids = [c.card_id for c in cards if c.precision == "none"][:_SAMPLE_CAP]
    table_only_ids = [c.card_id for c in cards if c.precision == "table"][:_SAMPLE_CAP]
    return MetabaseCoverage(
        cards_total=len(cards),
        cards_resolved_column=column,
        cards_resolved_table_only=table_only,
        cards_unresolved=unresolved,
        dashboards_total=len(dashboards),
        snippets_total=snippets_total,
        unresolved_card_ids=unresolved_ids,
        table_only_card_ids=table_only_ids,
    )


def coverage_ratio(coverage: MetabaseCoverage) -> float:
    """(column + table_only) / total — the resolved-reach ratio for ``--fail-under``."""
    if coverage.cards_total == 0:
        return 1.0
    resolved = coverage.cards_resolved_column + coverage.cards_resolved_table_only
    return resolved / coverage.cards_total
