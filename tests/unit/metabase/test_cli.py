"""— unit coverage for the ``metabase-extract`` CLI dialect wiring.

Focused on the ``--manifest`` / ``--adapter`` contract: the adapter alone can supply the
dialect (so a scheduled job need not ship a dbt manifest), and omitting both is a usage error.
"""

from __future__ import annotations

from click.testing import CliRunner

from parrant.metabase.cli import _resolve_dialect, metabase_extract


def test_resolve_dialect_prefers_adapter_without_manifest():
    # Adapter supplied, no manifest — the manifest is never read.
    assert _resolve_dialect(None, "snowflake") == "snowflake"


def test_extract_requires_manifest_or_adapter():
    result = CliRunner().invoke(
        metabase_extract,
        ["--metabase-url", "https://mb.example.com", "--database-id", "100"],
    )
    assert result.exit_code != 0
    assert "Provide --manifest or --adapter" in result.output
