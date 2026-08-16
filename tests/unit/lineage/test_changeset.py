"""Unit tests for diff-driven impact.

These exercise the pure pieces — changeset derivation, severity-aware
aggregation, report assembly and Markdown rendering — with lightweight stubs, so
they run without dbt artifacts on disk.
"""

from dataclasses import dataclass
from typing import Dict, Optional

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


class _FakeRegistry:
    """Minimal stand-in for ModelRegistry used by ChangesetBuilder."""

    def __init__(self, models: Dict[str, _Model], compiled: Optional[Dict[str, str]] = None):
        self._models = models
        self._compiled = compiled or {}

    def get_models(self) -> Dict[str, _Model]:
        return self._models

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
    assert "No column changes detected" in md


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

    assert "Affected exposures" in md
    assert "Finance Dashboard" in md
    # exposures section appears before the columns table
    assert md.index("Affected exposures") < md.index("Affected columns")
    assert "`dm`" in md and "critical" in md


def test_markdown_renders_partial_confidence_and_coverage_warning():
    aggregated = _impact([{"name": "dm"}], [], [])
    aggregated["by_change"] = []
    # Confidence + coverage are attached by the service/CLI, not build_changeset_report.
    aggregated["confidence"] = {
        "reachable_models": 187,
        "resolved_models": 1,
        "unanalyzable_models": 186,
        "not_in_catalog": 186,
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
    assert "haven't been built in the warehouse yet" in md
    assert "lower bound" in md
    assert "Coverage is partial" in md


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
