"""Opt-in live-Metabase end-to-end test.

Boots against a real Metabase (URL supplied by CI / a local caller via ``METABASE_URL``),
seeds a small representative corpus, runs ``parrant metabase-extract`` in-process, and
asserts a *non-zero-coverage* extract. The non-zero-coverage assertion is what fails on a
Metabase version whose API serves pMBQL / MBQL-5 without the resolver handling it — the
drift this tier exists to catch.

Double-guarded out of the per-push gate:
1. ``tests/live/`` is not one of the directory tiers ``scripts/run_tests.py`` runs.
2. ``RUN_METABASE_LIVE`` must be ``"1"`` or the whole module skips.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from parrant.metabase.artifact import load_metabase_lineage
from parrant.metabase.cli import metabase_extract
from tests.live import seed

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_METABASE_LIVE") != "1",
    reason="Live-Metabase tier is opt-in; set RUN_METABASE_LIVE=1 to enable.",
)


def test_metabase_extract_live_nonzero_coverage(
    metabase_url: str,
    metabase_auth: seed.Auth,
    seeded: seed.SeededContent,
    manifest_path: str,
    tmp_path: Path,
) -> None:
    output = tmp_path / "metabase_lineage.json"

    args = [
        "--metabase-url",
        metabase_url,
        *metabase_auth.cli_args(),
        "--database-id",
        str(seeded.database_id),
        "--manifest",
        manifest_path,
        "--output",
        str(output),
    ]

    result = CliRunner().invoke(metabase_extract, args, catch_exceptions=False)

    # 1. The CLI succeeded.
    assert result.exit_code == 0, f"CLI failed (exit {result.exit_code}):\n{result.output}"

    # 2. The written artifact exists and parses through the real loader (schema-validated).
    assert output.exists(), "metabase-extract did not write an artifact"
    raw = json.loads(output.read_text(encoding="utf-8"))
    assert raw["schema_version"] == 1
    lineage = load_metabase_lineage(output)
    assert lineage is not None

    cov = lineage.coverage

    # 3. Every card we seeded was extracted.
    assert cov.cards_total >= len(seeded.card_ids)

    # 4. NON-ZERO coverage — the pMBQL-drift canary. On a version that serves pMBQL to a
    #    resolver without the fix, every card degrades to `none` and this drops to zero.
    resolved = cov.cards_resolved_column + cov.cards_resolved_table_only
    assert resolved > 0, (
        "Zero-coverage extract: no card resolved to a warehouse relation. "
        "This is the signature of Metabase API-serialization drift "
        f"(version {lineage.provenance.metabase_version}) hitting an unpatched resolver. "
        f"coverage={cov.model_dump()}"
    )

    # 5. The native card resolved column-precise against the connected warehouse — proving
    #    native SQL parsing + warehouse-relation mapping works on this Metabase version. The
    #    relation key's db/schema depends on the connection, so match on the table suffix.
    native = next(c for c in lineage.cards if c.card_id == seeded.native_card_id)
    assert native.query_kind == "native"
    assert native.precision == "column", f"native card not column-precise: {native}"
    native_relations = {ref.relation for ref in native.columns}
    native_table_rel = next(
        (r for r in native_relations if r.split(".")[-1] == seed.NATIVE_TABLE), None
    )
    assert (
        native_table_rel is not None
    ), f"native card did not resolve a '{seed.NATIVE_TABLE}' relation; got {native_relations}"
    resolved_columns = {ref.column for ref in native.columns if ref.relation == native_table_rel}
    for expected in seed.NATIVE_COLUMNS:
        assert (
            expected in resolved_columns
        ), f"native card missing column {expected!r}; got {resolved_columns}"

    # 6. The seeded dashboard is present in the snapshot.
    dashboard_ids = {d.dashboard_id for d in lineage.dashboards}
    assert seeded.dashboard_id in dashboard_ids
