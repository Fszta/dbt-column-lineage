"""Human-readable Markdown rendering of a diff-driven impact report.

Same data as the ``--format json`` changeset report, shaped for a person to skim:
business-facing exposures first, then the blast-radius table.
"""

from typing import Any, Dict, List

_SEVERITY_LABEL = {"critical": "🔴 critical", "low_impact": "🟢 low"}


def _confidence_reasons(confidence: Dict[str, Any]) -> str:
    """Render the bolded root-cause clause explaining why models were unanalyzable."""
    not_in_catalog = confidence.get("not_in_catalog", 0)
    parse_failed = confidence.get("parse_failed", 0)
    if not_in_catalog and parse_failed:
        return (
            f" because **they haven't been built in the warehouse yet, or their SQL "
            f"couldn't be parsed** ({not_in_catalog} not built, {parse_failed} unparseable)"
        )
    if parse_failed:
        return " because **their SQL couldn't be parsed**"
    if not_in_catalog:
        return (
            " because **they haven't been built in the warehouse yet** (so they're "
            "absent from the catalog)"
        )
    return ""


def render_changeset_markdown(report: Dict[str, Any]) -> str:
    """Render a changeset impact report (from ``build_changeset_report``) as Markdown."""
    changeset = report.get("changeset", {})
    summary = report.get("summary", {})

    lines: List[str] = []
    lines.append("## Column-level impact of this change")
    lines.append("")

    total_changes = changeset.get("total_changes", 0)
    if not total_changes:
        lines.append("No column changes detected between base and head. ✅")
        lines.append("")
        return "\n".join(lines)

    # Headline counts.
    lines.append(
        f"**{total_changes}** changed column(s) → "
        f"**{summary.get('affected_models', 0)}** downstream model(s), "
        f"**{summary.get('affected_columns', 0)}** column(s), "
        f"**{summary.get('affected_exposures', 0)}** exposure(s) affected."
    )
    critical = summary.get("critical_count", 0)
    if critical:
        lines.append("")
        lines.append(f"> ⚠️ **{critical}** downstream column(s) recompute derived logic.")
    lines.append("")

    # Confidence: whether any DAG-reachable downstream model was impossible to analyze.
    confidence = report.get("confidence")
    if confidence:
        level = confidence.get("level")
        reachable = confidence.get("reachable_models", 0)
        if level == "full":
            lines.append(
                f"**Confidence:** full — every one of the {reachable} model(s) downstream "
                f"of this column was analyzable, so the impact above is complete, not a "
                f"lower bound."
            )
        else:
            unanalyzable = confidence.get("unanalyzable_models", 0)
            reasons = _confidence_reasons(confidence)
            lines.append(
                f"**Confidence:** partial — the impact above is a **lower bound**: "
                f"{unanalyzable} of {reachable} downstream model(s) could not be checked "
                f"at the column level{reasons}."
            )
        lines.append("")

    # Coverage: how much of the project the loaded artifacts cover.
    coverage = report.get("coverage")
    if coverage and not coverage.get("complete", False):
        lines.append(
            f"> ℹ️ Coverage is partial: analyzed {coverage.get('parsed_ok', 0)}/"
            f"{coverage.get('models_in_manifest', 0)} models "
            f"({coverage.get('not_in_catalog_count', 0)} not in catalog, "
            f"{coverage.get('parse_failed', 0)} parse-failed, "
            f"{coverage.get('skipped_no_sql', 0)} no compiled SQL)."
        )
        lines.append("")

    # Change breakdown.
    by_kind = changeset.get("by_kind", {})
    if by_kind:
        parts = [f"`{kind}`: {count}" for kind, count in sorted(by_kind.items())]
        lines.append("**Changes:** " + ", ".join(parts))
        lines.append("")

    # Exposures first — these are business-facing.
    exposures = report.get("affected_exposures", [])
    if exposures:
        lines.append("### Affected exposures")
        lines.append("")
        for exposure in exposures:
            name = exposure.get("name", "?")
            exp_type = exposure.get("type", "")
            url = exposure.get("url")
            label = f"[{name}]({url})" if url else name
            suffix = f" ({exp_type})" if exp_type else ""
            lines.append(f"- **{label}**{suffix}")
        lines.append("")

    # Blast-radius table.
    affected_columns = report.get("affected_columns", [])
    if affected_columns:
        lines.append("### Affected columns")
        lines.append("")
        lines.append("| Model | Column | Severity | Transformation | Expression |")
        lines.append("|---|---|---|---|---|")
        for column in affected_columns:
            severity = _SEVERITY_LABEL.get(column.get("severity", ""), column.get("severity", ""))
            expression = _truncate(column.get("sql_expression") or "")
            lines.append(
                f"| `{column.get('model', '')}` "
                f"| `{column.get('column', '')}` "
                f"| {severity} "
                f"| {column.get('transformation_type', '')} "
                f"| {expression} |"
            )
        lines.append("")

    # Unresolved changes — honesty about what could not be traced.
    unresolved = summary.get("unresolved_changes", 0)
    if unresolved:
        lines.append(
            f"> ℹ️ {unresolved} change(s) could not be traced downstream "
            f"(e.g. removed column with no base artifacts supplied)."
        )
        lines.append("")

    return "\n".join(lines)


def _truncate(text: str, limit: int = 60) -> str:
    text = text.replace("\n", " ").replace("|", "\\|").strip()
    if not text:
        return ""
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return f"`{text}`"
