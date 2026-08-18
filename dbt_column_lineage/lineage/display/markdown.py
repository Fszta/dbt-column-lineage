"""Human-readable Markdown rendering of a diff-driven impact report.

Same data as the ``--format json`` changeset report, shaped for a person to skim on a PR.
The layout is criticality-first: what actually changed, then the downstream columns that
*recompute logic* from it (the ones a reviewer must check), then business-facing exposures,
with low-risk pass-through references folded away so a large blast radius stays readable.
"""

from typing import Any, Dict, List

# Cap long lists so a huge blast radius doesn't produce an unscrollable comment.
_MAX_EXPOSURES_INLINE = 15


def _confidence_reasons(confidence: Dict[str, Any]) -> str:
    """Render the bolded root-cause clause explaining why models were unanalyzable."""
    no_column_info = confidence.get("no_column_info", 0)
    parse_failed = confidence.get("parse_failed", 0)
    if no_column_info and parse_failed:
        return (
            f" because **their SQL couldn't be parsed, or they expose no column-level "
            f"information** ({parse_failed} unparseable, {no_column_info} without a column "
            f"catalog)"
        )
    if parse_failed:
        return " because **their SQL couldn't be parsed**"
    if no_column_info:
        return (
            " because **they expose no column-level information** (absent from the catalog "
            "and with no parseable compiled SQL — e.g. a non-table relation such as a "
            "semantic view)"
        )
    return ""


def _group_by_model(columns: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for column in columns:
        grouped.setdefault(column.get("model", "?"), []).append(column)
    return {model: grouped[model] for model in sorted(grouped)}


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

    # --- What changed (the source) -------------------------------------------------------
    by_change = report.get("by_change", [])
    changed_nodes = sorted({(c.get("model", "?"), c.get("column", "?")) for c in by_change})
    by_kind = changeset.get("by_kind", {})
    kind_summary = ", ".join(f"`{k}`: {v}" for k, v in sorted(by_kind.items()))
    lines.append(f"**Changed:** {total_changes} column(s) — {kind_summary}")
    if changed_nodes and len(changed_nodes) <= 10:
        lines.append("")
        for model, column in changed_nodes:
            lines.append(f"- `{model}`.`{column}`")
    lines.append("")

    # --- Downstream headline -------------------------------------------------------------
    lines.append(
        f"→ **{summary.get('affected_models', 0)}** model(s), "
        f"**{summary.get('affected_columns', 0)}** column(s), "
        f"**{summary.get('affected_exposures', 0)}** exposure(s) downstream."
    )
    lines.append("")

    affected_columns = report.get("affected_columns", [])
    critical = [c for c in affected_columns if c.get("severity") == "critical"]
    filtered = [c for c in affected_columns if c.get("severity") == "filter"]
    passthrough = [
        c for c in affected_columns if c.get("severity") not in ("critical", "filter")
    ]

    # --- 🔴 Review — downstream output changes (derived value OR shifted row-set) ---------
    # Both are the same reviewer question ("this model's output changes"); they differ only
    # in HOW — a column's value is recomputed (derived), or the rows kept change because the
    # model filters/joins on the change (row-set). One section, one tag per item.
    if critical or filtered:
        review_count = len(critical) + len(filtered)
        lines.append(f"### 🔴 Review — downstream output changes ({review_count})")
        lines.append("")
        lines.append(
            "A column whose value is *derived* from the change, or a model whose *rows* shift "
            "because it filters/joins on it:"
        )
        lines.append("")

        derived_by_model = _group_by_model(critical)
        filter_by_model = {c.get("model", "?"): c for c in filtered}
        review_models = sorted(set(derived_by_model) | set(filter_by_model))

        expr_rows: List[str] = []
        for model in review_models:
            items: List[str] = []
            for column in sorted(
                derived_by_model.get(model, []), key=lambda c: c.get("column", "")
            ):
                items.append(f"`{column.get('column', '')}` · derived")
                raw = (column.get("sql_expression") or "").strip()
                if raw:
                    expr_rows += [f"**`{model}`.`{column.get('column', '')}`** · derived", ""]
                    expr_rows += ["```sql", raw, "```", ""]
            if model in filter_by_model:
                items.append("row-set · filtered/joined")
                condition = (filter_by_model[model].get("sql_expression") or "").strip()
                if condition:
                    expr_rows += [f"**`{model}`** · filtered/joined on", ""]
                    expr_rows += ["```sql", condition, "```", ""]
            lines.append(f"- **`{model}`**: " + ", ".join(items))
        lines.append("")

        if expr_rows:
            lines.append("<details><summary>Show expressions</summary>")
            lines.append("")
            lines.extend(expr_rows)
            lines.append("</details>")
            lines.append("")

    # --- 📊 Affected exposures (business-facing) -----------------------------------------
    exposures = report.get("affected_exposures", [])
    if exposures:
        lines.append(f"### 📊 Affected exposures ({len(exposures)})")
        lines.append("")
        exposure_lines = []
        for exposure in exposures:
            name = exposure.get("name", "?")
            exp_type = exposure.get("type", "")
            url = exposure.get("url")
            label = f"[{name}]({url})" if url else name
            suffix = f" ({exp_type})" if exp_type else ""
            exposure_lines.append(f"- **{label}**{suffix}")
        if len(exposure_lines) > _MAX_EXPOSURES_INLINE:
            lines.append(f"<details><summary>Show {len(exposure_lines)} exposures</summary>")
            lines.append("")
            lines.extend(exposure_lines)
            lines.append("")
            lines.append("</details>")
        else:
            lines.extend(exposure_lines)
        lines.append("")

    # --- 🟢 Pass-through references (folded: low risk) ------------------------------------
    if passthrough:
        grouped = _group_by_model(passthrough)
        lines.append(
            f"<details><summary>🟢 Pass-through references "
            f"({len(passthrough)} column(s) across {len(grouped)} model(s)) — direct "
            f"references, unchanged logic</summary>"
        )
        lines.append("")
        for model, cols in grouped.items():
            names = ", ".join(f"`{c.get('column', '')}`" for c in sorted(cols, key=lambda c: c.get("column", "")))
            lines.append(f"- **`{model}`**: {names}")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    # --- Footer: confidence + coverage (small, honest) -----------------------------------
    footer: List[str] = []
    confidence = report.get("confidence")
    if confidence:
        if confidence.get("level") == "full":
            footer.append(
                f"**Confidence:** full — all {confidence.get('reachable_models', 0)} "
                f"downstream model(s) were analyzable."
            )
        else:
            unanalyzable = confidence.get("unanalyzable_models", 0)
            reachable = confidence.get("reachable_models", 0)
            footer.append(
                f"**Confidence:** partial (lower bound) — {unanalyzable} of {reachable} "
                f"downstream model(s) could not be checked{_confidence_reasons(confidence)}."
            )
    coverage = report.get("coverage")
    if coverage and not coverage.get("complete", False):
        footer.append(
            f"Coverage: analyzed {coverage.get('parsed_ok', 0)}/"
            f"{coverage.get('models_in_manifest', 0)} models "
            f"({coverage.get('parse_failed', 0)} parse-failed)."
        )
    unresolved = summary.get("unresolved_changes", 0)
    if unresolved:
        footer.append(
            f"{unresolved} change(s) could not be traced downstream (e.g. removed column)."
        )
    if footer:
        lines.append("<sub>" + " · ".join(footer) + "</sub>")
        lines.append("")

    return "\n".join(lines)
