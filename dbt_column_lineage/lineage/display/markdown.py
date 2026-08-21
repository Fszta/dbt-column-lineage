"""Human-readable Markdown rendering of a diff-driven impact report.

Same data as the ``--format json`` changeset report, shaped for a person to triage on a PR.

Reviewer-first layout (a reviewer's real question is *"should I worry?"*):
1. a one-line **verdict banner** fusing severity + blast radius + the confidence floor,
2. what changed, then the downstream models whose **output changes** (a scannable table),
3. business-facing exposures with **operational apps surfaced above dashboards** (a stale
   dashboard is a Monday fix; an automation acting on wrong data is an incident),
with per-expression folds (oversized SQL truncated) and low-risk pass-through folded away.
"""

from typing import Any, Dict, List, Tuple

# Fold long dashboard lists so a huge blast radius stays scrollable.
_MAX_DASHBOARDS_INLINE = 8
# Truncate a single SQL expression past this many chars (a 2000-char JSON_OBJECT on one
# line forces horizontal scroll and makes the whole disclosure useless).
_MAX_SQL_CHARS = 400

# Subtle attribution appended to every rendered report so an adopting repo's PRs
# passively surface where the analysis came from. Kept to one muted line.
_CREDIT_LINE = "<sub>— lineage by [dbt-col-lineage](https://github.com/Fszta/dbt-column-lineage)</sub>"  # noqa: E501

# Machine kinds → words a reviewer reads without a glossary.
_KIND_LABELS = {
    "logic_changed": "logic changed",
    "type_changed": "type changed",
    "added": "added",
    "removed": "removed",
    "renamed": "renamed",
}


# Honesty note when a catalog.json was missing on a side: add/removed/type_changed
# detection never ran, so a clean report is not proof those changes are absent.
_STRUCTURAL_SKIP_NOTE = (
    "Structural checks (type/added/removed) skipped — no `catalog.json` on both sides; "
    "run `dbt docs generate`."
)


def _structural_checks_skipped(report: Dict[str, Any]) -> bool:
    return not report.get("structural_checks_available", True)


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" + ("" if n == 1 else "s")


def _kind_label(kind: str) -> str:
    return _KIND_LABELS.get(kind, kind.replace("_", " "))


def _truncate_sql(raw: str) -> Tuple[str, str]:
    """Return (sql_to_show, note). Oversized one-liners are head-elided with a pointer."""
    raw = raw.strip()
    if len(raw) <= _MAX_SQL_CHARS:
        return raw, ""
    head = raw[:_MAX_SQL_CHARS].rstrip()
    note = (
        f"_Expression truncated ({len(raw)} chars). "
        f"View the full form in the HTML explorer or `--format json`._"
    )
    return head + " …", note


def _format_break(b: Dict[str, Any]) -> str:
    """One compiler-style diagnostic line for a provable break.

    ``error[BREAK-TEST]`` — <removing|renaming> `model.column` breaks the **<kind>** test,
    with the schema-file path the reviewer opens to fix it. A relationships test broken
    through its referenced parent key is called out so the fix location is unambiguous.
    """
    verb = "renaming" if b.get("change_kind") == "renamed" else "removing"
    node = f"`{b.get('change_model', '?')}.{b.get('change_column', '?')}`"
    test_name = b.get("test_name", "?")
    via = " (via its referenced key)" if b.get("via_reference") else ""
    path = b.get("resource_path")
    where = f" — `{path}`" if path else ""
    return f"- `error[BREAK-TEST]` {verb} {node} breaks the **{test_name}** test{via}{where}"


def _owner_suffix(exposure: Dict[str, Any]) -> str:
    """A ' — owner: **Name**' clause routing the exposure to who must sign off.

    dbt stores an exposure ``owner`` as ``{name, email}``. Surfacing it turns blast
    radius into accountability — the reviewer knows who to ping without leaving the PR.
    Returns "" when no owner is declared.
    """
    owner = exposure.get("owner") or {}
    if not isinstance(owner, dict):
        return ""
    label = owner.get("name") or owner.get("email")
    return f" — owner: **{label}**" if label else ""


def _group_by_model(columns: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for column in columns:
        grouped.setdefault(column.get("model", "?"), []).append(column)
    return {model: grouped[model] for model in sorted(grouped)}


def _confidence_floor_clause(confidence: Dict[str, Any]) -> str:
    """A short clause for the verdict banner when impact is a lower bound."""
    if not confidence or confidence.get("level") == "full":
        return ""
    n = confidence.get("unanalyzable_models", 0)
    if not n:
        return ""
    return f" Impact is a **lower bound** — {_plural(n, 'downstream model')} couldn't be analyzed."


def _confidence_reason_words(confidence: Dict[str, Any]) -> str:
    """Plain-language reason models were unanalyzable, for the footer."""
    no_column_info = confidence.get("no_column_info", 0)
    parse_failed = confidence.get("parse_failed", 0)
    if no_column_info and parse_failed:
        return f" ({parse_failed} had unparseable SQL, {no_column_info} exposed no columns)"
    if parse_failed:
        return " (their SQL wouldn't parse)"
    if no_column_info:
        return " (they expose no column-level information)"
    return ""


def render_changeset_markdown(report: Dict[str, Any]) -> str:
    """Render a changeset impact report (from ``build_changeset_report``) as Markdown."""
    changeset = report.get("changeset", {})
    summary = report.get("summary", {})

    out: List[str] = ["## Column-level impact of this change", ""]

    total_changes = changeset.get("total_changes", 0)
    if not total_changes:
        out.append(
            "✅ **No column changes detected** between base and head — nothing downstream to check."
        )
        if _structural_checks_skipped(report):
            out += ["", "<sub>" + _STRUCTURAL_SKIP_NOTE + "</sub>"]
        out.append("")
        out.append(_CREDIT_LINE)
        out.append("")
        return "\n".join(out)

    by_change = report.get("by_change", [])
    changed_nodes = sorted({(c.get("model", "?"), c.get("column", "?")) for c in by_change})
    by_kind = changeset.get("by_kind", {})

    affected_columns = report.get("affected_columns", [])
    critical = [c for c in affected_columns if c.get("severity") == "critical"]
    filtered = [c for c in affected_columns if c.get("severity") == "filter"]
    passthrough = [c for c in affected_columns if c.get("severity") not in ("critical", "filter")]

    exposures = report.get("affected_exposures", [])
    apps = [e for e in exposures if (e.get("type") or "").lower() != "dashboard"]
    dashboards = [e for e in exposures if (e.get("type") or "").lower() == "dashboard"]

    derived_by_model = _group_by_model(critical)
    filter_by_model = {c.get("model", "?"): c for c in filtered}
    review_models = sorted(set(derived_by_model) | set(filter_by_model))
    confidence = report.get("confidence") or {}
    breaks = report.get("provable_breaks") or []

    # --- Verdict banner (the 5-second read) ----------------------------------------------
    # A provable break (a dbt test the change orphans) is the only BLOCK-worthy signal; it
    # outranks the heuristic review/safe banner below.
    if breaks:
        out.append(
            f"> ⛔ **Blocked — {_plural(len(breaks), 'provable break')}.** "
            f"This change orphans {_plural(len(breaks), 'dbt test')} that will fail on the "
            f"next `dbt build`." + _confidence_floor_clause(confidence)
        )
    elif review_models:
        if len(changed_nodes) == 1:
            subject = f"**`{changed_nodes[0][0]}.{changed_nodes[0][1]}`**"
        elif changed_nodes:
            subject = f"**{_plural(len(changed_nodes), 'column')}**"
        else:
            subject = f"**{_plural(total_changes, 'column')}**"
        reach_bits: List[str] = []
        if apps:
            reach_bits.append(f"**{_plural(len(apps), 'automation')}**")
        if dashboards:
            reach_bits.append(f"**{_plural(len(dashboards), 'dashboard')}**")
        reach = (" and reaches " + " + ".join(reach_bits)) if reach_bits else ""
        out.append(
            f"> 🔴 **Review required.** Changing {subject} recomputes logic in "
            f"**{_plural(len(review_models), 'model')}**{reach}."
            + _confidence_floor_clause(confidence)
        )
    else:
        out.append(
            "> 🟢 **Looks safe.** The change is only referenced downstream, not recomputed — "
            "no model's output logic changes." + _confidence_floor_clause(confidence)
        )
    out.append("")

    # --- What changed --------------------------------------------------------------------
    if len(by_kind) == 1 and total_changes == 1:
        kind_txt = _kind_label(next(iter(by_kind)))
    else:
        kind_txt = ", ".join(f"{_kind_label(k)}: {v}" for k, v in sorted(by_kind.items()))
    out.append(f"**Changed:** {_plural(total_changes, 'column')} — {kind_txt}")
    if changed_nodes:
        rows = [f"- `{model}.{column}`" for model, column in changed_nodes]
        if len(rows) <= 10:
            out += [""] + rows
        else:
            out += [
                "",
                f"<details><summary>Show {_plural(len(rows), 'changed column')}</summary>",
                "",
            ]
            out += rows + ["", "</details>"]
    out.append("")
    out.append(
        f"Reaches **{_plural(summary.get('affected_models', 0), 'model')}** · "
        f"**{_plural(summary.get('affected_columns', 0), 'column')}** · "
        f"**{_plural(summary.get('affected_exposures', 0), 'exposure')}** downstream."
    )
    out.append("")

    # --- ⛔ Provable breaks — the block-worthy diagnostics --------------------------------
    if breaks:
        out.append(f"### ⛔ Provable breaks ({len(breaks)})")
        out.append("")
        out.append(
            "These fail on the next `dbt build` — a dbt test now targets a column this "
            "change removed. Fix the test or restore the column."
        )
        out.append("")
        out += [_format_break(b) for b in breaks]
        out.append("")

    # --- 🔴 Output changes — the scannable table -----------------------------------------
    if review_models:
        out.append(f"### 🔴 Check these — their output changes ({len(review_models)})")
        out.append("")
        out.append("| Model | What changes | How |")
        out.append("|---|---|---|")
        folds: List[str] = []
        for model in review_models:
            what_bits: List[str] = []
            how_bits: List[str] = []
            derived_cols = sorted(
                derived_by_model.get(model, []), key=lambda c: c.get("column", "")
            )
            if derived_cols:
                what_bits.append(", ".join(f"`{c.get('column', '')}`" for c in derived_cols))
                how_bits.append("value recomputed")
                for col in derived_cols:
                    raw = (col.get("sql_expression") or "").strip()
                    if raw:
                        sql, note = _truncate_sql(raw)
                        folds += [
                            f"<details><summary>Show new logic — <code>{model}.{col.get('column', '')}</code></summary>",
                            "",
                            "```sql",
                            sql,
                            "```",
                        ]
                        if note:
                            folds += ["", note]
                        folds += ["</details>", ""]
            if model in filter_by_model:
                what_bits.append("rows kept may change")
                how_bits.append("filtered/joined on this column")
                cond = (filter_by_model[model].get("sql_expression") or "").strip()
                if cond:
                    sql, note = _truncate_sql(cond)
                    folds += [
                        f"<details><summary>Show filter — <code>{model}</code></summary>",
                        "",
                        "```sql",
                        sql,
                        "```",
                    ]
                    if note:
                        folds += ["", note]
                    folds += ["</details>", ""]
            out.append(f"| `{model}` | {'; '.join(what_bits)} | {' · '.join(how_bits)} |")
        out.append("")
        if folds:
            out += folds

    # --- ⚠️ Business-facing exposures (apps surfaced above dashboards) --------------------
    if exposures:
        rollup_bits: List[str] = []
        if dashboards:
            rollup_bits.append(_plural(len(dashboards), "dashboard"))
        if apps:
            rollup_bits.append(_plural(len(apps), "application"))
        out.append(f"### ⚠️ Business-facing exposures ({len(exposures)})")
        out.append("")

        def _fmt(exp: Dict[str, Any]) -> str:
            name = exp.get("name", "?")
            url = exp.get("url")
            head = f"- **[{name}]({url})**" if url else f"- **{name}**"
            return head + _owner_suffix(exp)

        if apps:
            out.append(
                f"**{_plural(len(apps), 'application')}** — operational, may act on this data:"
            )
            out += [_fmt(e) for e in apps]
            out.append("")
        if dashboards:
            if len(dashboards) > _MAX_DASHBOARDS_INLINE:
                out.append(f"<details><summary>{_plural(len(dashboards), 'dashboard')}</summary>")
                out += [""] + [_fmt(e) for e in dashboards] + ["", "</details>"]
            else:
                if apps:
                    out.append(f"**{_plural(len(dashboards), 'dashboard')}:**")
                out += [_fmt(e) for e in dashboards]
        out.append("")

    # --- 🟢 Pass-through references (folded: low risk) -----------------------------------
    if passthrough:
        grouped = _group_by_model(passthrough)
        out.append(
            f"<details><summary>🟢 Safe — passes through unchanged "
            f"({_plural(len(passthrough), 'column')} in {_plural(len(grouped), 'model')})</summary>"
        )
        out.append("")
        for model, cols in grouped.items():
            names = ", ".join(
                f"`{c.get('column', '')}`" for c in sorted(cols, key=lambda c: c.get("column", ""))
            )
            out.append(f"- **`{model}`**: {names}")
        out += ["", "</details>", ""]

    # --- Footer: confidence + coverage (small, plain, honest) ----------------------------
    footer: List[str] = []
    if _structural_checks_skipped(report):
        footer.append(_STRUCTURAL_SKIP_NOTE)
    # Break detection is a lower bound: tests it couldn't attribute to a column are never
    # checked, so a clean (SAFE/REVIEW) ruling is not proof no test breaks.
    unattributable = (report.get("verdict_coverage") or {}).get("unattributable_tests", 0)
    if unattributable:
        footer.append(
            f"Break detection skipped {_plural(unattributable, 'dbt test')} it couldn't tie "
            f"to a column (singular/custom tests); a clean ruling is a lower bound."
        )
    if confidence:
        if confidence.get("level") == "full":
            footer.append(
                f"**Confidence: full** — all {_plural(confidence.get('reachable_models', 0), 'downstream model')} "
                f"were analyzed."
            )
        else:
            n = confidence.get("unanalyzable_models", 0)
            reachable = confidence.get("reachable_models", 0)
            footer.append(
                f"**Confidence: partial** — {n} of {_plural(reachable, 'downstream model')} "
                f"couldn't be analyzed{_confidence_reason_words(confidence)}, so the impact above "
                f"may be incomplete."
            )
    coverage = report.get("coverage")
    if coverage and not coverage.get("complete", False):
        footer.append(
            f"Parser reached {coverage.get('parsed_ok', 0):,} of "
            f"{coverage.get('models_in_manifest', 0):,} project models "
            f"({coverage.get('parse_failed', 0)} parse-failed)."
        )
    unresolved = summary.get("unresolved_changes", 0)
    if unresolved:
        footer.append(
            f"{_plural(unresolved, 'change')} could not be traced downstream (e.g. a removed column)."
        )
    if footer:
        out += ["<sub>" + " · ".join(footer) + "</sub>", ""]

    out.append(_CREDIT_LINE)
    out.append("")

    return "\n".join(out)
