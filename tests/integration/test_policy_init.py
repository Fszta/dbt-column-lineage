"""Integration / anti-footgun regression for ``policy init``.

Generates the scaffold policy for the bundled dbt project, parses it through the REAL policy
loader, and evaluates it over a no-op changeset — locking in the property the whole feature
exists to guarantee: the generated policy does NOT rage-block on day one (it yields ALLOW when
nothing changed). Uses the ``dbt_artifacts`` fixture + ``LineageService`` like
``test_backtest_policy.py``.
"""

from pathlib import Path

import pytest
import yaml

from dbt_column_lineage.lineage.policy import evaluate_policy, parse_policy
from dbt_column_lineage.lineage.policy_init import emit_policy_yaml, scan_project
from dbt_column_lineage.lineage.service import LineageService
from dbt_column_lineage.models.schema import GateDecision


@pytest.fixture
def head_service(dbt_artifacts):
    return LineageService(
        Path(dbt_artifacts["catalog_path"]),
        Path(dbt_artifacts["manifest_path"]),
    )


def test_generated_policy_parses_and_allows_noop(head_service):
    scan = scan_project(head_service.registry)
    text = emit_policy_yaml(scan)

    # 1. The generated YAML must round-trip through the real parser without raising.
    policy = parse_policy(yaml.safe_load(text))
    assert [r.id for r in policy.rules] == ["provable-break-block", "exposure-guard"]

    # 2. On a no-op changeset (nothing changed, no impact, no provable breaks) the gate ALLOWs.
    #    This is the anti-footgun regression: the scaffold never blocks with an empty changeset.
    verdict = evaluate_policy(
        changes=[],
        changeset_impact={},
        registry=head_service.registry,
        policy=policy,
        breaks=[],
    )
    assert verdict.decision is GateDecision.ALLOW
    assert verdict.blocks() is False


def test_generated_policy_defaults_are_fail_closed_never_open(head_service):
    scan = scan_project(head_service.registry)
    policy = parse_policy(yaml.safe_load(emit_policy_yaml(scan)))
    # The scaffold ships the closed posture; it must never author an open-when-unsure default.
    assert policy.defaults.on_missing_meta.value == "fail_closed"
    assert policy.defaults.on_error.value == "fail_closed"
