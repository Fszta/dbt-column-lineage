"""Unit tests for diff-driven impact.

These exercise the pure pieces — changeset derivation, severity-aware
aggregation, report assembly and Markdown rendering — with lightweight stubs, so
they run without dbt artifacts on disk.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import pytest

from dbt_column_lineage.lineage.changeset import (
    ChangeKind,
    ChangesetBuilder,
    ColumnChange,
    build_changeset_report,
    scope_changes_to_models,
)
from dbt_column_lineage.lineage.display.markdown import render_changeset_markdown
from dbt_column_lineage.lineage.service import LineageService


# --- stubs -----------------------------------------------------------------


@dataclass
class _Col:
    data_type: Optional[str]


@dataclass
class _Model:
    columns: Dict[str, _Col]


@dataclass
class _Lin:
    """Stand-in for ColumnLineage (the per-column derivation signature source)."""

    source_columns: Set[str]
    transformation_type: str
    sql_expression: str


@dataclass
class _LinCol:
    """A column that also carries parsed per-column lineage, enabling a precise diff."""

    data_type: Optional[str] = None
    lineage: List[_Lin] = field(default_factory=list)


class _FakeRegistry:
    """Minimal stand-in for ModelRegistry used by ChangesetBuilder."""

    def __init__(
        self,
        models: Dict[str, _Model],
        compiled: Optional[Dict[str, str]] = None,
        catalog_backed: Optional[set] = None,
    ):
        self._models = models
        self._compiled = compiled or {}
        # Default: every model is catalog-backed (structural column diffs are trusted).
        # Pass an explicit set to simulate catalog-missing (manifest-only) models.
        self._catalog_backed = catalog_backed if catalog_backed is not None else set(models)

    def get_models(self) -> Dict[str, _Model]:
        return self._models

    def is_catalog_backed(self, model_name: str) -> bool:
        return model_name in self._catalog_backed

    def get_compiled_sql(self, model_name: str) -> str:
        if model_name not in self._compiled:
            raise ValueError(f"no compiled sql for {model_name}")
        return self._compiled[model_name]


class _FakeService:
    """Stub exposing get_column_impact, used to drive get_changeset_impact."""

    def __init__(self, impacts: Dict[tuple, dict]):
        self._impacts = impacts

    def get_column_impact(self, model: str, column: str) -> dict:
        if (model, column) not in self._impacts:
            raise ValueError(f"no impact for {model}.{column}")
        return self._impacts[(model, column)]


def _impact(models, columns, exposures):
    return {
        "summary": {
            "affected_models": len(models),
            "affected_columns": len(columns),
            "affected_exposures": len(exposures),
            "critical_count": sum(1 for c in columns if c["severity"] == "critical"),
            "low_impact_count": sum(1 for c in columns if c["severity"] != "critical"),
        },
        "affected_models": models,
        "affected_columns": columns,
        "affected_exposures": exposures,
    }


# --- ChangeKind ------------------------------------------------------------


def test_change_kind_priority_ordering():
    order = [
        ChangeKind.ADDED,
        ChangeKind.RENAMED,
        ChangeKind.LOGIC_CHANGED,
        ChangeKind.TYPE_CHANGED,
        ChangeKind.REMOVED,
    ]
    priorities = [k.priority for k in order]
    assert priorities == sorted(priorities), "priority must increase with severity"


# --- ChangesetBuilder ------------------------------------------------------


def test_builder_detects_added_removed_and_type_change():
    base = _FakeRegistry(
        {"m": _Model({"a": _Col("int"), "b": _Col("text")})},
    )
    head = _FakeRegistry(
        {"m": _Model({"a": _Col("bigint"), "c": _Col("text")})},
    )
    changes = ChangesetBuilder(base, head).build()
    by_col = {(c.column): c.kind for c in changes}

    assert by_col["a"] == ChangeKind.TYPE_CHANGED
    assert by_col["b"] == ChangeKind.REMOVED
    assert by_col["c"] == ChangeKind.ADDED
    # type_changed carries a base -> head detail string
    a_change = next(c for c in changes if c.column == "a")
    assert a_change.detail == "int -> bigint"


def test_builder_new_and_removed_models():
    base = _FakeRegistry({"gone": _Model({"x": _Col("int")})})
    head = _FakeRegistry({"fresh": _Model({"y": _Col("int")})})
    changes = ChangesetBuilder(base, head).build()

    kinds = {(c.model, c.column): c.kind for c in changes}
    assert kinds[("gone", "x")] == ChangeKind.REMOVED
    assert kinds[("fresh", "y")] == ChangeKind.ADDED


def test_builder_logic_change_flags_all_columns_and_dedups_with_type_change():
    base = _FakeRegistry(
        {"m": _Model({"a": _Col("int"), "b": _Col("text")})},
        compiled={"m": "select 1 as a, 'x' as b"},
    )
    head = _FakeRegistry(
        {"m": _Model({"a": _Col("bigint"), "b": _Col("text")})},
        compiled={"m": "select 2 as a, 'y' as b"},
    )
    changes = ChangesetBuilder(base, head).build()
    kinds = {c.column: c.kind for c in changes}

    # 'b' only differs by logic; 'a' differs by both type and logic -> keep the
    # higher-severity type_changed.
    assert kinds["b"] == ChangeKind.LOGIC_CHANGED
    assert kinds["a"] == ChangeKind.TYPE_CHANGED


def test_builder_ignores_cosmetic_sql_changes():
    base = _FakeRegistry(
        {"m": _Model({"a": _Col("int")})},
        compiled={"m": "select 1 as a  -- old comment"},
    )
    head = _FakeRegistry(
        {"m": _Model({"a": _Col("int")})},
        compiled={"m": "select    1 as a\n-- new comment"},
    )
    assert ChangesetBuilder(base, head).build() == []


def test_builder_logic_change_is_per_column_when_lineage_is_available():
    # base: a, b, c are all plain pass-throughs of an upstream column.
    base = _FakeRegistry(
        {
            "m": _Model(
                {
                    "a": _LinCol("text", [_Lin({"up.a"}, "direct", "up.a")]),
                    "b": _LinCol("text", [_Lin({"up.b"}, "direct", "up.b")]),
                    "c": _LinCol("text", [_Lin({"up.c"}, "direct", "up.c")]),
                }
            )
        },
        compiled={"m": "select up.a as a, up.b as b, up.c as c from up"},
    )
    # head: only `a`'s derivation changed (now a coalesce); b and c are untouched.
    head = _FakeRegistry(
        {
            "m": _Model(
                {
                    "a": _LinCol(
                        "text", [_Lin({"up.a", "up.z"}, "derived", "coalesce(up.a, up.z)")]
                    ),
                    "b": _LinCol("text", [_Lin({"up.b"}, "direct", "up.b")]),
                    "c": _LinCol("text", [_Lin({"up.c"}, "direct", "up.c")]),
                }
            )
        },
        compiled={"m": "select coalesce(up.a, up.z) as a, up.b as b, up.c as c from up"},
    )
    changes = ChangesetBuilder(base, head).build()
    logic = {c.column for c in changes if c.kind == ChangeKind.LOGIC_CHANGED}
    # Precisely `a` — NOT the whole model. This is what stops an edit to one column from
    # flooding downstream with every unrelated pass-through.
    assert logic == {"a"}, logic


def test_builder_logic_change_flags_all_columns_when_lineage_missing():
    # No per-column lineage on the stub columns -> can't diff precisely, so fall back to
    # the conservative model-level behaviour (flag every output column).
    base = _FakeRegistry(
        {"m": _Model({"a": _Col("text"), "b": _Col("text")})},
        compiled={"m": "select 1 as a, 2 as b"},
    )
    head = _FakeRegistry(
        {"m": _Model({"a": _Col("text"), "b": _Col("text")})},
        compiled={"m": "select 9 as a, 8 as b"},
    )
    changes = ChangesetBuilder(base, head).build()
    logic = {c.column for c in changes if c.kind == ChangeKind.LOGIC_CHANGED}
    assert logic == {"a", "b"}, logic


# --- get_changeset_impact (severity-aware aggregation) ---------------------


def test_get_changeset_impact_dedups_and_keeps_highest_severity():
    # Two changed columns both hit downstream node ('dm', 'dc'): once low, once
    # critical. The aggregated node must keep 'critical' and count it once.
    low = {
        "model": "dm",
        "column": "dc",
        "severity": "low_impact",
        "transformation_type": "direct",
        "sql_expression": "dm.dc",
    }
    crit = {
        "model": "dm",
        "column": "dc",
        "severity": "critical",
        "transformation_type": "derived",
        "sql_expression": "sum(dm.dc)",
    }
    other = {
        "model": "dm2",
        "column": "z",
        "severity": "low_impact",
        "transformation_type": "renamed",
        "sql_expression": "dm2.z",
    }

    exposure = {
        "name": "dash",
        "type": "dashboard",
        "url": None,
        "description": None,
        "depends_on_models": ["dm"],
    }

    impacts = {
        ("src", "col1"): _impact([{"name": "dm"}], [low, other], [exposure]),
        ("src", "col2"): _impact([{"name": "dm"}], [crit], [exposure]),
    }
    changes = [
        ColumnChange("src", "col1", ChangeKind.LOGIC_CHANGED),
        ColumnChange("src", "col2", ChangeKind.TYPE_CHANGED),
    ]

    result = LineageService.get_changeset_impact(_FakeService(impacts), changes)

    assert result["summary"]["affected_columns"] == 2  # (dm,dc) deduped + (dm2,z)
    assert result["summary"]["critical_count"] == 1
    assert result["summary"]["affected_exposures"] == 1  # deduped by name
    assert result["summary"]["unresolved_changes"] == 0
    dedup = {(c["model"], c["column"]): c["severity"] for c in result["affected_columns"]}
    assert dedup[("dm", "dc")] == "critical"


def test_get_changeset_impact_reports_unresolved():
    changes = [ColumnChange("ghost", "col", ChangeKind.REMOVED)]
    result = LineageService.get_changeset_impact(_FakeService({}), changes)

    assert result["summary"]["unresolved_changes"] == 1
    assert result["by_change"][0]["resolved"] is False


def test_removed_column_resolved_against_base_service():
    base_impact = _impact(
        [{"name": "dm"}],
        [
            {
                "model": "dm",
                "column": "dc",
                "severity": "low_impact",
                "transformation_type": "direct",
                "sql_expression": "dm.dc",
            }
        ],
        [],
    )
    head = _FakeService({})  # column is gone from head -> would be unresolved
    base = _FakeService({("src", "removed_col"): base_impact})

    changes = [ColumnChange("src", "removed_col", ChangeKind.REMOVED)]
    result = LineageService.get_changeset_impact(head, changes, base_service=base)

    assert result["summary"]["unresolved_changes"] == 0
    assert result["summary"]["affected_columns"] == 1


# --- report + markdown -----------------------------------------------------


def test_build_changeset_report_shape():
    changes = [
        ColumnChange("m", "a", ChangeKind.TYPE_CHANGED),
        ColumnChange("m", "b", ChangeKind.ADDED),
    ]
    aggregated = _impact([], [], [])
    aggregated["by_change"] = []
    report = build_changeset_report("two-manifest", changes, aggregated)

    assert report["changeset"]["source"] == "two-manifest"
    assert report["changeset"]["total_changes"] == 2
    assert report["changeset"]["by_kind"] == {"type_changed": 1, "added": 1}
    # aggregated impact keys are merged in at top level
    assert "summary" in report and "affected_columns" in report


def test_markdown_empty_changeset():
    report = build_changeset_report("two-manifest", [], {**_impact([], [], []), "by_change": []})
    md = render_changeset_markdown(report)
    assert "No column changes" in md


def test_markdown_lists_exposures_first_and_blast_table():
    columns = [
        {
            "model": "dm",
            "column": "dc",
            "severity": "critical",
            "transformation_type": "derived",
            "sql_expression": "sum(x)",
        }
    ]
    exposures = [
        {
            "name": "Finance Dashboard",
            "type": "dashboard",
            "url": "https://x",
            "description": None,
            "depends_on_models": ["dm"],
        }
    ]
    aggregated = _impact([{"name": "dm"}], columns, exposures)
    aggregated["by_change"] = []
    report = build_changeset_report(
        "two-manifest", [ColumnChange("s", "c", ChangeKind.LOGIC_CHANGED)], aggregated
    )
    md = render_changeset_markdown(report)

    # verdict banner leads, fusing severity + blast radius
    assert "Review required" in md
    assert md.index("Review required") < md.index("Column-level impact") + 200
    # business-facing exposures section present
    assert "Business-facing exposures" in md
    assert "Finance Dashboard" in md
    # criticality-first: the output-changes section comes before exposures.
    assert "Check these — their output changes" in md
    assert md.index("Check these — their output changes") < md.index("Business-facing exposures")
    # the review section is a scannable table with plain-language tags
    assert "| Model | What changes | How |" in md
    assert "`dm`" in md and "`dc`" in md and "value recomputed" in md
    # the derived expression is reachable behind a per-model fold, not cluttering the table
    assert "Show new logic" in md and "sum(x)" in md


def test_markdown_renders_partial_confidence_and_coverage_warning():
    aggregated = _impact([{"name": "dm"}], [], [])
    aggregated["by_change"] = []
    # Confidence + coverage are attached by the service/CLI, not build_changeset_report.
    aggregated["confidence"] = {
        "reachable_models": 187,
        "resolved_models": 1,
        "unanalyzable_models": 186,
        "no_column_info": 186,
        "parse_failed": 0,
        "level": "partial",
    }
    report = build_changeset_report(
        "two-manifest", [ColumnChange("s", "c", ChangeKind.TYPE_CHANGED)], aggregated
    )
    report["coverage"] = {
        "models_in_manifest": 1217,
        "parsed_ok": 176,
        "not_in_catalog_count": 1031,
        "parse_failed": 0,
        "skipped_no_sql": 0,
        "complete": False,
    }
    md = render_changeset_markdown(report)

    assert "Confidence:" in md and "partial" in md
    assert "186 of 187" in md
    # The diagnosis must be honest: no column-level info, NOT a claim that the models
    # "haven't been built" (a built model can still be absent from the catalog).
    assert "no column-level information" in md
    assert "haven't been built in the warehouse yet" not in md
    # the "this is a floor, not a count" caveat is surfaced in the verdict banner
    assert "lower bound" in md
    # coverage stated in plain words with the real counts
    assert "Parser reached" in md and "176" in md and "1,217" in md


def test_markdown_full_confidence_is_quiet_without_coverage_warning():
    aggregated = _impact([{"name": "dm"}], [], [])
    aggregated["by_change"] = []
    aggregated["confidence"] = {
        "reachable_models": 5,
        "resolved_models": 5,
        "level": "full",
    }
    report = build_changeset_report(
        "two-manifest", [ColumnChange("s", "c", ChangeKind.TYPE_CHANGED)], aggregated
    )
    report["coverage"] = {"complete": True}
    md = render_changeset_markdown(report)

    assert "Confidence:" in md and "full" in md
    # A complete project must not emit the scary partial-coverage note.
    assert "Coverage is partial" not in md


def test_markdown_folds_passthrough_columns_and_omits_empty_critical_section():
    columns = [
        {
            "model": "dm",
            "column": "c1",
            "severity": "low_impact",
            "transformation_type": "direct",
            "sql_expression": "dm.c1",
        }
    ]
    aggregated = _impact([{"name": "dm"}], columns, [])
    aggregated["by_change"] = []
    report = build_changeset_report(
        "two-manifest", [ColumnChange("s", "c", ChangeKind.LOGIC_CHANGED)], aggregated
    )
    md = render_changeset_markdown(report)

    # Pass-throughs are low-risk: folded into a <details>, not shown as a wall of rows.
    assert "passes through unchanged" in md
    assert "<details>" in md and "`c1`" in md
    # With no derived or row-set impact, the review section is omitted entirely.
    assert "Check these — their output changes" not in md


def test_markdown_renders_filter_section_for_row_set_impact():
    columns = [
        {
            "model": "orders_flag_rate",
            "column": "(row-set)",
            "severity": "filter",
            "transformation_type": "filter",
            "sql_expression": "order_status = 'flagged'",
        }
    ]
    aggregated = _impact([{"name": "orders_flag_rate"}], columns, [])
    aggregated["by_change"] = []
    report = build_changeset_report(
        "two-manifest",
        [ColumnChange("orders", "order_status", ChangeKind.LOGIC_CHANGED)],
        aggregated,
    )
    md = render_changeset_markdown(report)

    # Filter/join-only consumers appear in the output-changes table with plain tags,
    # with the predicate condition available in the per-model fold.
    assert "Check these — their output changes" in md
    assert "`orders_flag_rate`" in md
    assert "rows kept may change" in md and "filtered/joined on this column" in md
    assert "order_status = 'flagged'" in md
    # A row-set impact is not a pass-through.
    assert "passes through unchanged" not in md


# --- git scope filter ------------------------------------------------------


@dataclass
class _PathModel:
    columns: Dict[str, _Col]
    resource_path: Optional[str]


class _PathRegistry:
    def __init__(self, models: Dict[str, _PathModel]):
        self._models = models

    def get_models(self) -> Dict[str, _PathModel]:
        return self._models


def _registry_with_paths():
    return _PathRegistry(
        {
            "orders": _PathModel({"id": _Col("int")}, "models/orders.sql"),
            "customers": _PathModel({"id": _Col("int")}, "models/customers.sql"),
        }
    )


def test_git_changed_models_maps_files_to_models(monkeypatch):
    from dbt_column_lineage.lineage import changeset

    monkeypatch.setattr(
        changeset,
        "_git_changed_sql_files",
        lambda ref, repo_dir=None: ["models/orders.sql", "macros/helper.sql"],
    )
    changed = changeset.git_changed_models(_registry_with_paths(), "origin/main")
    # orders maps to a model; the macro file has no model and is ignored.
    assert changed == {"orders"}


def test_scope_changes_to_models_filters():
    changes = [
        ColumnChange("orders", "id", ChangeKind.TYPE_CHANGED),
        ColumnChange("customers", "id", ChangeKind.LOGIC_CHANGED),
    ]
    scoped = scope_changes_to_models(changes, {"orders"})
    assert [(c.model, c.column) for c in scoped] == [("orders", "id")]


def test_scope_changes_empty_when_no_overlap():
    changes = [ColumnChange("customers", "id", ChangeKind.LOGIC_CHANGED)]
    assert scope_changes_to_models(changes, {"orders"}) == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
