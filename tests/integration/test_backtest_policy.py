"""Integration: backtest a policy over the saved changeset corpus against the bundled dbt
project's HEAD registry, and lock in the fail-safe-footgun finding — a naive ``meta.pii eq true`` block
rule is reported as fail-safe-driven (every firing via a fail-safe UNKNOWN), never a real match.
"""

import os
from pathlib import Path

import pytest

from dbt_column_lineage.lineage.backtest import run_backtest
from dbt_column_lineage.lineage.policy import load_policy
from dbt_column_lineage.lineage.service import LineageService

_REPO_ROOT = Path(__file__).parent.parent.parent
_CHANGESETS = str(_REPO_ROOT / "tests" / "resources" / "changesets")
_NAIVE_PII = str(_REPO_ROOT / "tests" / "resources" / "policies" / "naive_pii.yml")
_BLOCK_ON_REMOVED = str(_REPO_ROOT / "tests" / "resources" / "policies" / "block_on_removed.yml")


@pytest.fixture
def head_service(dbt_artifacts):
    return LineageService(
        Path(dbt_artifacts["catalog_path"]),
        Path(dbt_artifacts["manifest_path"]),
    )


def _run(head_service, policy_path):
    policy = load_policy(policy_path)
    return run_backtest(
        head_service,
        policy,
        changesets_dir=_CHANGESETS,
        policy_source=policy_path,
    )


def test_corpus_replays_all_points(head_service):
    report = _run(head_service, _NAIVE_PII)
    # Three fixture changesets in the corpus.
    n_files = len([f for f in os.listdir(_CHANGESETS) if f.endswith(".json")])
    assert report.prs_replayed == n_files
    assert report.mode == "changesets"
    assert report.prs_skipped == 0
    # Changesets-mode fidelity note is honest about the un-exercised block tiers.
    assert "provable-break BLOCK tier is NOT exercised" in report.fidelity_note


def test_naive_pii_rule_is_reported_fail_safe_driven(head_service):
    """the fail-safe footgun lock-in: the naive rule blocks every changeset but every firing is a fail-safe
    UNKNOWN (the pii key is absent), so fired_on_unknown == fired_total > 0 and it blocks all PRs.
    """
    report = _run(head_service, _NAIVE_PII)
    stat = next(s for s in report.rule_stats if s.rule_id == "naive-pii-guard")
    assert stat.fired_total > 0
    assert stat.fired_on_unknown == stat.fired_total  # PURE fail-safe, never a proven match
    assert stat.matched_zero is False  # it DID fire (via fail-safe), so not "dead"
    # It would block every replayed PR.
    assert stat.would_block_prs == report.prs_replayed
    assert report.prs_would_block == report.prs_replayed


def test_block_on_removed_fires_on_real_match_not_fail_safe(head_service):
    """Contrast: a change-axis rule that matches a proven `kind == removed` fact fires with
    fired_on_unknown == 0 (a proven TRUE match), and is not dead."""
    report = _run(head_service, _BLOCK_ON_REMOVED)
    stat = next(s for s in report.rule_stats if s.rule_id == "block-on-removed")
    assert stat.fired_total > 0  # changeset_01 carries a removed column
    assert stat.fired_on_unknown == 0  # proven match, not fail-safe
    assert stat.matched_zero is False


def test_baseline_delta_and_regression_gate(head_service):
    from dbt_column_lineage.lineage.backtest import backtest_exit_code

    baseline = _run(head_service, _NAIVE_PII)
    # Same run vs itself: no rule's would-block rose -> regression gate passes.
    current = run_backtest(
        head_service,
        load_policy(_NAIVE_PII),
        changesets_dir=_CHANGESETS,
        policy_source=_NAIVE_PII,
        baseline=baseline,
    )
    assert current.baseline_delta is not None
    assert backtest_exit_code(current, "regression", baseline=baseline) == 0
    # any-block trips because the naive rule blocks every PR.
    assert backtest_exit_code(current, "any-block") == 1
