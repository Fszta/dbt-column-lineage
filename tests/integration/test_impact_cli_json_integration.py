"""End-to-end: the `impact` CLI command emits the verdict keys in its JSON report.

Covers the CLI glue (cli/main.py) that the unit/integration verdict tests bypass by calling
the classifier directly — a regression here (dropped key, wrong shape) would otherwise ship
silently since the classifier itself stays green.
"""

import json

from click.testing import CliRunner

from parrant.cli.main import impact


def test_impact_json_includes_verdict_keys(dbt_artifacts):
    # Diff the fixture against itself: a valid run with no changes → SAFE, no breaks.
    result = CliRunner().invoke(
        impact,
        [
            "--base-manifest",
            str(dbt_artifacts["manifest_path"]),
            "--base-catalog",
            str(dbt_artifacts["catalog_path"]),
            "--manifest",
            str(dbt_artifacts["manifest_path"]),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    report = json.loads(result.output)

    # The new keys are present and additive.
    assert report["verdict"] == "safe"
    assert report["provable_breaks"] == []
    assert report["summary"]["provable_break_count"] == 0
    # Backward-compatible keys still there.
    assert "affected_models" in report["summary"]
