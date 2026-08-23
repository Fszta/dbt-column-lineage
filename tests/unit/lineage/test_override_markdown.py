"""Markdown rendering of the override section + the policy-hit suppression note."""

from dbt_column_lineage.lineage.display.markdown import (
    _policy_hit_line,
    _render_overrides_section,
    render_changeset_markdown,
)


def _base_report(**extra):
    report = {
        "changeset": {"total_changes": 1, "by_kind": {"logic_changed": 1}, "changes": []},
        "summary": {"affected_models": 0, "affected_columns": 0, "affected_exposures": 0},
        "by_change": [{"model": "orders", "column": "total"}],
        "affected_columns": [],
        "affected_exposures": [],
    }
    report.update(extra)
    return report


def test_render_overrides_section_empty_when_nothing():
    assert _render_overrides_section({}) == []


def test_render_overrides_applied_shows_delta_and_reason():
    report = {
        "overrides": [
            {
                "model": "orders",
                "column": "customer_id",
                "verb": "allow-break",
                "reason": "test yml fixed in follow-up",
                "downgraded_from": "block",
                "downgraded_to": "review",
                "source_line": 4,
                "scope": "column",
            }
        ]
    }
    text = "\n".join(_render_overrides_section(report))
    assert "Overrides applied (1)" in text
    assert "allow-break" in text
    assert "block → review" in text
    assert "test yml fixed in follow-up" in text


def test_render_override_warnings_are_first_and_unfolded():
    report = {
        "override_warnings": ["orders: line 2: 'allow-foo' pragma ... — pragma ignored"],
        "overrides": [
            {
                "model": "m",
                "column": "c",
                "verb": "allow-change",
                "reason": "r",
                "downgraded_from": "review",
                "downgraded_to": "safe",
                "source_line": 1,
                "scope": "column",
            }
        ],
    }
    lines = _render_overrides_section(report)
    text = "\n".join(lines)
    assert "IGNORED" in text
    # Warnings header appears before the applied header (loud, unfolded, first).
    assert text.index("IGNORED") < text.index("Overrides applied")
    assert "<details>" not in text.split("Overrides applied")[0]


def test_render_ineffective_override_shows_hint():
    report = {
        "ineffective_overrides": [
            {
                "model": "orders",
                "column": "amount_eur",
                "verb": "allow-break",
                "reason": "r",
                "hint": "allow-break landed on an ADDED column; ... use column=<old_name>",
                "source_line": 3,
                "scope": "column",
            }
        ]
    }
    text = "\n".join(_render_overrides_section(report))
    assert "no effect" in text
    assert "ADDED column" in text


def test_render_stale_overrides_are_folded():
    report = {
        "stale_overrides": [
            {
                "model": "orders",
                "column": "ghost",
                "verb": "allow-change",
                "reason": "dead",
                "source_line": 2,
                "scope": "column",
            }
        ]
    }
    text = "\n".join(_render_overrides_section(report))
    assert "<details>" in text
    assert "Stale overrides (1)" in text
    assert "ghost" in text


def test_model_scope_override_labelled_loud():
    report = {
        "overrides": [
            {
                "model": "orders",
                "column": "customer_id",
                "verb": "allow-break",
                "reason": "r",
                "downgraded_from": "block",
                "downgraded_to": "review",
                "source_line": 1,
                "scope": "model",
            }
        ]
    }
    text = "\n".join(_render_overrides_section(report))
    assert "(model-level)" in text


def test_policy_hit_line_shows_suppression():
    hit = {
        "rule_id": "exposure-guard",
        "change_model": "marts.x",
        "change_column": "amount",
        "overridden": True,
        "original_decision": "warn",
        "override_reason": "downstream updated",
    }
    line = _policy_hit_line(hit)
    assert "override suppressed warn" in line
    assert "downstream updated" in line


def test_full_render_includes_overrides_section():
    report = _base_report(
        overrides=[
            {
                "model": "orders",
                "column": "customer_id",
                "verb": "allow-break",
                "reason": "r",
                "downgraded_from": "block",
                "downgraded_to": "review",
                "source_line": 1,
                "scope": "column",
            }
        ]
    )
    text = render_changeset_markdown(report)
    assert "Overrides applied (1)" in text
