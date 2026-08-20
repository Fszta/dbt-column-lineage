"""Diff-driven impact: turn a set of changes into a consolidated blast radius.

The single-column impact engine answers "what does this one column touch?".
The real unit of work, however, is a *pull request*: a set of changed columns.
This module derives that changeset — either from two dbt artifact sets (base vs.
head, the reliable dbt-native signal) or from a git diff of ``.sql`` files
(fallback when only one manifest is available) — and feeds every changed column
through the existing traversal core, deduplicating downstream nodes.
"""

from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.parser.sql_parser_utils import strip_sql_comments

logger = logging.getLogger(__name__)


class ChangeKind(str, Enum):
    """How a column changed between base and head.

    Ordering matters: when the same column is touched in more than one way (e.g.
    a model whose ``compiled_code`` changed *and* whose column was retyped), the
    highest-priority kind wins. Roughly ordered by blast-radius risk.
    """

    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"
    LOGIC_CHANGED = "logic_changed"
    RENAMED = "renamed"
    ADDED = "added"

    @property
    def priority(self) -> int:
        return _KIND_PRIORITY[self]


_KIND_PRIORITY: Dict[ChangeKind, int] = {
    ChangeKind.REMOVED: 5,
    ChangeKind.TYPE_CHANGED: 4,
    ChangeKind.LOGIC_CHANGED: 3,
    ChangeKind.RENAMED: 2,
    ChangeKind.ADDED: 1,
}


@dataclass(frozen=True)
class ColumnChange:
    """A single changed column, with the kind of change and optional detail."""

    model: str
    column: str
    kind: ChangeKind
    detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "model": self.model,
            "column": self.column,
            "kind": self.kind.value,
            "detail": self.detail,
        }


def _normalize_sql(sql: Optional[str]) -> Optional[str]:
    """Normalize compiled SQL so cosmetic reformatting isn't read as a logic change.

    ``strip_sql_comments`` already removes comments and collapses whitespace runs,
    which is exactly the noise we want to ignore when deciding whether the logic
    that produces a model actually changed.
    """
    if sql is None:
        return None
    return strip_sql_comments(sql)


class ChangesetBuilder:
    """Derive a :class:`ColumnChange` list from two loaded ``ModelRegistry`` instances.

    Compares base (target branch) against head (current) and emits, per column:
    ``added`` / ``removed`` / ``type_changed``, plus ``logic_changed`` for every
    column of a model whose compiled SQL changed. When a column is touched in more
    than one way, the highest-priority kind is kept (see :class:`ChangeKind`).
    """

    def __init__(self, base: ModelRegistry, head: ModelRegistry):
        self.base = base
        self.head = head

    def build(self) -> List[ColumnChange]:
        # (model, column) -> ColumnChange, keeping the highest-priority kind.
        chosen: Dict[Tuple[str, str], ColumnChange] = {}

        def record(change: ColumnChange) -> None:
            key = (change.model, change.column)
            existing = chosen.get(key)
            if existing is None or change.kind.priority > existing.kind.priority:
                chosen[key] = change

        base_models = self.base.get_models()
        head_models = self.head.get_models()

        for model_name in sorted(set(base_models) | set(head_models)):
            base_model = base_models.get(model_name)
            head_model = head_models.get(model_name)

            if base_model is not None and head_model is None:
                # Whole model removed: every column it exposed is now gone. Because the
                # universe is manifest-seeded, ``head_model is None`` means the model is
                # absent from the *head manifest* — a genuine deletion — not merely absent
                # from the head catalog (a built-but-uncatalogued model is still present).
                for column in sorted(base_model.columns):
                    record(ColumnChange(model_name, column, ChangeKind.REMOVED))
                continue

            if head_model is not None and base_model is None:
                # Brand-new model: all columns are additive.
                for column in sorted(head_model.columns):
                    record(ColumnChange(model_name, column, ChangeKind.ADDED))
                continue

            assert base_model is not None and head_model is not None

            # Structural column diffs (added/removed/type_changed) are only trustworthy
            # when BOTH sides are backed by a real catalog. For a catalog-missing model
            # the column set is recovered from parsing compiled SQL (best-effort, no data
            # types), so diffing it would emit phantom add/remove/type churn. In that case
            # we rely solely on the compiled-SQL diff below (logic_changed).
            if self._both_catalog_backed(model_name):
                base_cols = base_model.columns
                head_cols = head_model.columns

                for column in sorted(set(base_cols) | set(head_cols)):
                    in_base = column in base_cols
                    in_head = column in head_cols
                    if in_head and not in_base:
                        record(ColumnChange(model_name, column, ChangeKind.ADDED))
                    elif in_base and not in_head:
                        record(ColumnChange(model_name, column, ChangeKind.REMOVED))
                    else:
                        base_type = (base_cols[column].data_type or "").lower()
                        head_type = (head_cols[column].data_type or "").lower()
                        # Only a real type change counts; an unknown type on either side
                        # (empty string) is not evidence of a change.
                        if base_type and head_type and base_type != head_type:
                            record(
                                ColumnChange(
                                    model_name,
                                    column,
                                    ChangeKind.TYPE_CHANGED,
                                    detail=f"{base_cols[column].data_type} -> {head_cols[column].data_type}",
                                )
                            )

            # Logic change: the model's compiled SQL differs. Rather than flag EVERY output
            # column — which floods the downstream blast radius with unrelated pass-throughs
            # (editing one column must not implicate every other column of the model) — diff each output
            # column's derivation between base and head and flag ONLY the columns that actually
            # changed. Falls back to flagging all columns when neither side exposes per-column
            # lineage (nothing to diff precisely).
            if self._logic_changed(model_name):
                for column in sorted(self._logic_changed_columns(base_model, head_model)):
                    record(ColumnChange(model_name, column, ChangeKind.LOGIC_CHANGED))

        return sorted(chosen.values(), key=lambda c: (c.model, c.column, c.kind.value))

    def _both_catalog_backed(self, model_name: str) -> bool:
        """True when the model has a real catalog entry in BOTH base and head.

        Structural column diffs are only authoritative when column names and data types
        come from the catalog on both sides; otherwise we fall back to the compiled-SQL
        (logic) diff to avoid phantom add/remove/type changes from parser-derived columns.
        """
        return self.base.is_catalog_backed(model_name) and self.head.is_catalog_backed(
            model_name
        )

    def structural_diff_available(self) -> bool:
        """Whether add/removed/type_changed detection could run at all.

        Those structural checks require a real catalog on BOTH sides (see
        :meth:`_both_catalog_backed`); without one we fall back to the compiled-SQL
        (logic) diff and cannot see columns added, removed, or retyped. Returns
        ``True`` only when each side contributes at least one catalog-backed model —
        so the report can be honest that structural checks were skipped when a
        ``catalog.json`` is absent on either side.
        """
        return self._side_has_catalog(self.base) and self._side_has_catalog(self.head)

    @staticmethod
    def _side_has_catalog(registry: ModelRegistry) -> bool:
        return any(registry.is_catalog_backed(name) for name in registry.get_models())

    def _logic_changed(self, model_name: str) -> bool:
        base_sql = _normalize_sql(self._safe_compiled_sql(self.base, model_name))
        head_sql = _normalize_sql(self._safe_compiled_sql(self.head, model_name))
        if not base_sql or not head_sql:
            return False
        return base_sql != head_sql

    def _logic_changed_columns(self, base_model, head_model) -> Set[str]:
        """Which output columns actually changed derivation between base and head.

        The model's compiled SQL differs, but usually only a few columns are responsible.
        We compare each column's per-column lineage signature (transformation type +
        normalized expression + source columns) so downstream tracing follows only the
        columns whose *value* changed — not every pass-through the model happens to expose.

        Conservative fallbacks preserve correctness where a precise diff isn't possible:
        - if NEITHER side has any parsed per-column lineage, flag all head columns;
        - a column parsed on exactly one side (its signature appeared or disappeared) is
          treated as changed.
        """
        base_sigs = self._column_signatures(base_model)
        head_sigs = self._column_signatures(head_model)
        if not base_sigs and not head_sigs:
            return set(head_model.columns)

        changed: Set[str] = set()
        for column in head_model.columns:
            base_sig = base_sigs.get(column)
            head_sig = head_sigs.get(column)
            if base_sig is None and head_sig is None:
                # Neither side parsed this column (e.g. a literal constant with no lineage);
                # there's nothing to diff, so don't treat it as changed.
                continue
            if base_sig != head_sig:
                changed.add(column)
        return changed

    @staticmethod
    def _column_signatures(model) -> Dict[str, Tuple]:
        """Per-column derivation signature: {column -> sorted lineage fingerprint}.

        Columns with no parsed lineage are omitted (no signature), so the caller can tell
        "parsed, unchanged" apart from "not parsed".
        """
        signatures: Dict[str, Tuple] = {}
        for column_name, column in model.columns.items():
            lineage = getattr(column, "lineage", None) or []
            if not lineage:
                continue
            parts = []
            for entry in lineage:
                expression = _normalize_sql(getattr(entry, "sql_expression", None)) or ""
                sources = ",".join(sorted(entry.source_columns or []))
                parts.append((entry.transformation_type or "", expression, sources))
            signatures[column_name] = tuple(sorted(parts))
        return signatures

    @staticmethod
    def _safe_compiled_sql(registry: ModelRegistry, model_name: str) -> Optional[str]:
        try:
            return registry.get_compiled_sql(model_name)
        except Exception:
            # Sources, seeds and models without compiled SQL simply have no logic
            # to diff; treat as "no logic change".
            return None


def _path_to_model_map(head: ModelRegistry) -> Dict[str, str]:
    """Map each model's ``resource_path`` (dbt ``original_file_path``) to its name."""
    mapping: Dict[str, str] = {}
    for model_name, model in head.get_models().items():
        if model.resource_path:
            mapping[_norm_path(model.resource_path)] = model_name
    return mapping


def git_changed_models(
    head: ModelRegistry,
    git_base: str,
    repo_dir: Optional[str] = None,
) -> Set[str]:
    """Return the set of models whose ``.sql`` file changed against ``git_base``.

    Files with no matching model (macros, tests, deleted files) are ignored, so
    this reflects only *model* edits — the unit the scope filter cares about.
    """
    path_to_model = _path_to_model_map(head)
    changed: Set[str] = set()
    for changed_file in _git_changed_sql_files(git_base, repo_dir):
        matched = path_to_model.get(_norm_path(changed_file))
        if matched:
            changed.add(matched)
    return changed


def build_git_changeset(
    head: ModelRegistry,
    git_base: str,
    repo_dir: Optional[str] = None,
) -> List[ColumnChange]:
    """Fallback changeset: diff ``.sql`` model files against ``git_base``.

    When only one manifest is available we cannot diff columns, so every column
    of each touched model is reported as ``logic_changed`` — a coarse but honest
    signal. Files are mapped to models via each model's ``resource_path``.
    """
    changed_models = git_changed_models(head, git_base, repo_dir)
    if not changed_models:
        return []

    head_models = head.get_models()
    chosen: Dict[Tuple[str, str], ColumnChange] = {}
    for model_name in changed_models:
        model = head_models[model_name]
        for column in sorted(model.columns):
            chosen[(model_name, column)] = ColumnChange(
                model_name, column, ChangeKind.LOGIC_CHANGED, detail=model.resource_path
            )

    return sorted(chosen.values(), key=lambda c: (c.model, c.column))


def scope_changes_to_models(changes: List[ColumnChange], models: Set[str]) -> List[ColumnChange]:
    """Keep only changes whose model is in ``models``.

    Used to intersect a precise two-manifest changeset with the set of models
    the current branch actually touched (``git diff base...HEAD``), so a stale
    base artifact can't leak already-merged changes into the report.
    """
    return [change for change in changes if change.model in models]


def _norm_path(path: str) -> str:
    return re.sub(r"^\./", "", path.strip()).lstrip("/")


def _git_changed_sql_files(git_base: str, repo_dir: Optional[str]) -> List[str]:
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", f"{git_base}...HEAD", "--", "*.sql"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        raise RuntimeError(f"Failed to compute git diff against '{git_base}': {exc}") from exc

    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def build_changeset_report(
    source: str,
    changes: List[ColumnChange],
    aggregated: Dict[str, object],
) -> Dict[str, object]:
    """Assemble the final report: a ``changeset`` block plus the aggregated impact.

    The impact keys (``summary``, ``affected_models``, ``affected_columns``,
    ``affected_exposures``) are a superset of the single-column ``impact`` block,
    so existing consumers keep working; ``changeset`` and ``by_change`` are added.
    """
    by_kind: Dict[str, int] = {}
    for change in changes:
        by_kind[change.kind.value] = by_kind.get(change.kind.value, 0) + 1

    report: Dict[str, object] = {
        "changeset": {
            "source": source,
            "total_changes": len(changes),
            "by_kind": by_kind,
            "changes": [change.to_dict() for change in changes],
        }
    }
    report.update(aggregated)
    return report
