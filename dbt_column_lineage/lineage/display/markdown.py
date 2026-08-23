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
# Cap the affected-column chain shown per cross-boundary dashboard: enough to
# be actionable ("go straight to `revenue`"), not a wall of columns on a wide `select *`.
_MAX_VIA_COLUMNS_INLINE = 4
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


# Policy gate decision → a one-glyph severity marker (amber reserved for the one blocking
# band, per DESIGN.md's rationed-colour rule).
_POLICY_DECISION_MARKER = {
    "block": "⛔",
    "warn": "🟡",
    "allow": "🟢",
}
# Order decisions block-first so the reviewer reads the reason it blocked before anything else.
_POLICY_DECISION_ORDER = ["block", "warn", "allow"]


def _policy_hit_line(hit: Dict[str, Any]) -> str:
    """One line for a fired rule: the rule id, the subject change, and the matched reach.

    Deliberately references node NAMES only — the blast radius already has its own section
    above, and re-listing it here would duplicate (and risk contradicting) that report.
    """
    rule_id = hit.get("rule_id", "?")
    model = hit.get("change_model")
    column = hit.get("change_column")
    if model and column:
        subject = f"`{model}.{column}`"
    elif model:
        subject = f"`{model}`"
    else:
        subject = "_changeset_"  # aggregate-scope rule: no single subject
    reach = hit.get("matched_reach") or []
    reach_txt = ""
    if reach:
        names = ", ".join(f"`{name}`" for name in reach)
        reach_txt = f" → reaches {names}"
    return f"- **{rule_id}** — {subject}{reach_txt}"


def _render_policy_section(verdict: Dict[str, Any]) -> List[str]:
    """Render the policy-engine verdict: fired rules grouped by decision (block first),
    the selective build/test sets, and the notify intents for the consumer's CI to route.

    Rendered ONLY when a policy is present (the caller guards on ``report['policy_verdict']``).
    Does NOT re-list the downstream blast radius — the impact section owns that.
    """
    decision = str(verdict.get("decision", "allow"))
    marker = _POLICY_DECISION_MARKER.get(decision, "🟢")
    hits = verdict.get("hits") or []
    out: List[str] = [f"### {marker} Policy verdict — {decision.upper()}", ""]

    if decision == "block":
        # A block must state its EXIT, not just the obstacle: reframe it as
        # "block-until". The gate re-runs on every push and clears itself once the change stops
        # tripping the rules below — so a reviewer sees the release path, not a dead end. This is
        # pure messaging over the existing verdict; no override input is consulted.
        out += [
            "> **Blocked until the change stops tripping the rules below.** This gate re-runs on "
            "every push and clears itself — no manual override needed. Clear it by any of: "
            "reverting or proving-equivalent the breaking change; evolving the downstream model / "
            "schema to absorb it; or stopping it from reaching the flagged object.",
            "",
        ]

    if not hits:
        out.append("No policy rule fired against this change.")
    else:
        by_decision: Dict[str, List[Dict[str, Any]]] = {}
        for hit in hits:
            by_decision.setdefault(str(hit.get("decision", "allow")), []).append(hit)
        for band in _POLICY_DECISION_ORDER:
            band_hits = by_decision.get(band)
            if not band_hits:
                continue
            band_marker = _POLICY_DECISION_MARKER.get(band, "🟢")
            out.append(f"**{band_marker} {band.capitalize()} ({len(band_hits)})**")
            out.append("")
            out += [_policy_hit_line(hit) for hit in band_hits]
            out.append("")

    build_set = verdict.get("build_set") or []
    test_set = verdict.get("test_set") or []
    if build_set or test_set:
        if build_set:
            out.append(
                f"**Selective build set ({len(build_set)}):** "
                + ", ".join(f"`{name}`" for name in build_set)
            )
        if test_set:
            out.append(
                f"**Selective test set ({len(test_set)}):** "
                + ", ".join(f"`{name}`" for name in test_set)
            )
        out.append("")

    notifications = verdict.get("notifications") or []
    if notifications:
        out.append(f"**Notifications ({len(notifications)})** — routed by your CI:")
        out.append("")
        for note in notifications:
            channel = note.get("channel") or "?"
            target = note.get("target") or "?"
            message = note.get("message") or ""
            out.append(f"- `{channel}` → **{target}**: {message}")
        out.append("")

    # Honesty counters — a fail-closed block driven by unknowns should read as such.
    unresolved = int(verdict.get("unresolved_reach_count", 0) or 0)
    skipped = int(verdict.get("skipped_missing_meta", 0) or 0)
    honesty: List[str] = []
    if unresolved:
        honesty.append(f"{_plural(unresolved, 'change')} had unresolved reach (treated fail-safe)")
    if skipped:
        honesty.append(f"{_plural(skipped, 'rule evaluation')} skipped for missing meta")
    if honesty:
        out += ["<sub>" + " · ".join(honesty) + "</sub>", ""]

    return out


def _truncate_expr(raw: str, limit: int = 120) -> str:
    """One-line, length-capped rendering of a defining expression for the explain view."""
    collapsed = " ".join(raw.split())
    if len(collapsed) > limit:
        return collapsed[: limit - 1].rstrip() + "…"
    return collapsed


def render_changeset_markdown(report: Dict[str, Any], explain: bool = False) -> str:
    """Render a changeset impact report (from ``build_changeset_report``) as Markdown.

    When ``explain`` is True, each changed column is annotated with the semantic reason it
    was flagged and (when available) a compact ``base → head`` expression line. The default
    (``explain=False``) leaves the output byte-for-byte as before.
    """
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
    # (model, column) -> the explain block a logic change carried, so `--explain` can annotate
    # each changed column with WHY it was flagged. Structural changes carry no explain block.
    explain_by_node: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for change in by_change:
        block = change.get("explain")
        if isinstance(block, dict):
            explain_by_node[(change.get("model", "?"), change.get("column", "?"))] = block
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
        rows: List[str] = []
        for model, column in changed_nodes:
            rows.append(f"- `{model}.{column}`")
            if explain:
                block = explain_by_node.get((model, column))
                if block is not None:
                    reason = block.get("reason")
                    if reason:
                        rows.append(f"  - _why:_ {reason}")
                    base = (block.get("base") or "").strip()
                    head = (block.get("head") or "").strip()
                    if base or head:
                        rows.append(f"  - `{_truncate_expr(base)}` → `{_truncate_expr(head)}`")
        if len(changed_nodes) <= 10:
            out += [""] + rows
        else:
            out += [
                "",
                f"<details><summary>Show {_plural(len(changed_nodes), 'changed column')}</summary>",
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
            # Cross-boundary provenance: a dashboard reached PAST dbt's edge via the
            # Metabase artifact is tagged so a reviewer knows it's not a dbt-native exposure,
            # and whether the reach is column-precise or coarse (table-grain).
            tag = ""
            if exp.get("source") == "metabase":
                precision = exp.get("precision")
                grain = " (table-grain)" if precision == "table" else ""
                tag = f" — via **Metabase**{grain}"
                # F4: name WHICH column(s) of the dashboard the change hits, so the reviewer
                # goes straight to the field instead of hunting the whole board. Distinct
                # `model.column`, deterministically ordered, capped. Empty on a table-grain
                # reach (no proven column) — the "(table-grain)" tag already says why.
                via = exp.get("via_columns") or []
                cols = sorted({f"{v['model']}.{v['column']}" for v in via if isinstance(v, dict)})
                if cols:
                    shown = ", ".join(f"`{c}`" for c in cols[:_MAX_VIA_COLUMNS_INLINE])
                    extra = len(cols) - _MAX_VIA_COLUMNS_INLINE
                    if extra > 0:
                        shown += f" +{extra} more"
                    tag += f" · affects {shown}"
            return head + tag + _owner_suffix(exp)

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

    # --- 🛡️ Policy verdict (only when a policy ran) --------------------------------------
    policy_verdict = report.get("policy_verdict")
    if isinstance(policy_verdict, dict):
        out += _render_policy_section(policy_verdict)

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
    # Cross-boundary (Metabase) reach honesty: a stale or coarse snapshot must read as such so a
    # fail-closed policy block driven by it is explainable, never a fabricated certainty.
    metabase = report.get("metabase")
    if isinstance(metabase, dict) and metabase.get("level") != "absent":
        reached = metabase.get("dashboards_reached", 0)
        table_only = metabase.get("cards_table_only", 0)
        bits = f"Metabase reach: {_plural(reached, 'dashboard')}"
        if table_only:
            bits += f" ({table_only} table-grain, not column-precise)"
        if metabase.get("stale"):
            age = metabase.get("snapshot_age_hours")
            age_txt = f" ({age:.0f}h old)" if isinstance(age, (int, float)) else ""
            bits += f" — ⚠️ snapshot is STALE{age_txt}; treat this reach as degraded"
        footer.append(bits + ".")
    if footer:
        out += ["<sub>" + " · ".join(footer) + "</sub>", ""]

    out.append(_CREDIT_LINE)
    out.append("")

    return "\n".join(out)
