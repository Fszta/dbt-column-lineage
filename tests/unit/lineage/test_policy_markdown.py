"""Unit tests for the policy-verdict Markdown section.

The section renders ONLY when ``report['policy_verdict']`` is present, groups fired rules by
decision (block first), surfaces the selective build/test sets and notify intents, and never
re-lists the downstream blast radius (the impact section owns that).
"""

from dbt_column_lineage.lineage.display.markdown import render_changeset_markdown

_BASE_REPORT = {
    "changeset": {"total_changes": 1, "by_kind": {"logic_changed": 1}},
    "summary": {"affected_models": 1, "affected_columns": 1, "affected_exposures": 0},
    "by_change": [{"model": "stg_accounts", "column": "account_id", "kind": "logic_changed"}],
    "affected_columns": [],
    "affected_exposures": [],
}


def _report_with_policy(policy_verdict):
    report = dict(_BASE_REPORT)
    report["policy_verdict"] = policy_verdict
    return report


def test_no_policy_verdict_renders_no_policy_section():
    md = render_changeset_markdown(dict(_BASE_REPORT))
    assert "Policy verdict" not in md


def test_block_verdict_renders_section_block_first():
    verdict = {
        "decision": "block",
        "hits": [
            {
                "rule_id": "warn-rule",
                "decision": "warn",
                "change_model": "stg_x",
                "change_column": "c",
                "matched_reach": [],
                "actions": ["warn"],
            },
            {
                "rule_id": "pii-outside-allowlist",
                "decision": "block",
                "change_model": "stg_accounts",
                "change_column": "account_id",
                "matched_reach": ["mart_users", "mart_kyc"],
                "actions": ["block", "notify"],
            },
        ],
        "build_set": ["mart_users"],
        "test_set": ["mart_kyc"],
        "notifications": [
            {"channel": "slack", "target": "#data-governance", "message": "PII reaches reader"}
        ],
        "unresolved_reach_count": 0,
        "skipped_missing_meta": 0,
    }
    md = render_changeset_markdown(_report_with_policy(verdict))

    assert "Policy verdict — BLOCK" in md
    # Block band appears before the warn band.
    assert md.index("Block (1)") < md.index("Warn (1)")
    # Subject + matched reach names surface (by name only).
    assert "pii-outside-allowlist" in md
    assert "`stg_accounts.account_id`" in md
    assert "`mart_users`" in md
    # Build/test sets rendered.
    assert "Selective build set (1)" in md
    assert "Selective test set (1)" in md
    # Notification intent rendered for the consumer's CI to route.
    assert "#data-governance" in md
    assert "PII reaches reader" in md


def test_allow_verdict_with_no_hits_still_renders_section():
    verdict = {"decision": "allow", "hits": [], "build_set": [], "test_set": []}
    md = render_changeset_markdown(_report_with_policy(verdict))
    assert "Policy verdict — ALLOW" in md
    assert "No policy rule fired" in md


def test_block_verdict_states_how_to_clear():
    """Feedback F1: a block must carry its release path — framed as block-until, self-clearing
    on the next push, not a dead end."""
    verdict = {
        "decision": "block",
        "hits": [
            {
                "rule_id": "r",
                "decision": "block",
                "change_model": "m",
                "change_column": "c",
                "matched_reach": [],
                "actions": ["block"],
            }
        ],
    }
    md = render_changeset_markdown(_report_with_policy(verdict))
    assert "Blocked until" in md
    assert "clears itself" in md


def test_allow_and_warn_have_no_block_until_note():
    for decision in ("allow", "warn"):
        verdict = {"decision": decision, "hits": []}
        md = render_changeset_markdown(_report_with_policy(verdict))
        assert "Blocked until" not in md


def test_honesty_counters_surface():
    verdict = {
        "decision": "block",
        "hits": [
            {
                "rule_id": "r",
                "decision": "block",
                "change_model": "m",
                "change_column": "c",
                "matched_reach": [],
                "actions": ["block"],
            }
        ],
        "unresolved_reach_count": 2,
        "skipped_missing_meta": 1,
    }
    md = render_changeset_markdown(_report_with_policy(verdict))
    assert "unresolved reach" in md
    assert "skipped for missing meta" in md
