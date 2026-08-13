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
from typing import Dict, List, Optional, Tuple

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
                # Whole model removed: every column it exposed is now gone.
                for column in sorted(base_model.columns):
                    record(ColumnChange(model_name, column, ChangeKind.REMOVED))
                continue

            if head_model is not None and base_model is None:
                # Brand-new model: all columns are additive.
                for column in sorted(head_model.columns):
                    record(ColumnChange(model_name, column, ChangeKind.ADDED))
                continue

            assert base_model is not None and head_model is not None
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
                    if base_type != head_type:
                        record(
                            ColumnChange(
                                model_name,
                                column,
                                ChangeKind.TYPE_CHANGED,
                                detail=f"{base_cols[column].data_type} -> {head_cols[column].data_type}",
                            )
                        )

            # Logic change: compiled SQL differs. Any output column of the model
            # may now be produced differently, so flag them all (dedup keeps the
            # higher-severity kind where a column was also added/removed/retyped).
            if self._logic_changed(model_name):
                for column in sorted(head_cols):
                    record(ColumnChange(model_name, column, ChangeKind.LOGIC_CHANGED))

        return sorted(chosen.values(), key=lambda c: (c.model, c.column, c.kind.value))

    def _logic_changed(self, model_name: str) -> bool:
        base_sql = _normalize_sql(self._safe_compiled_sql(self.base, model_name))
        head_sql = _normalize_sql(self._safe_compiled_sql(self.head, model_name))
        if not base_sql or not head_sql:
            return False
        return base_sql != head_sql

    @staticmethod
    def _safe_compiled_sql(registry: ModelRegistry, model_name: str) -> Optional[str]:
        try:
            return registry.get_compiled_sql(model_name)
        except Exception:
            # Sources, seeds and models without compiled SQL simply have no logic
            # to diff; treat as "no logic change".
            return None


def build_git_changeset(
    head: ModelRegistry,
    git_base: str,
    repo_dir: Optional[str] = None,
) -> List[ColumnChange]:
    """Fallback changeset: diff ``.sql`` model files against ``git_base``.

    When only one manifest is available we cannot diff columns, so every column
    of each touched model is reported as ``logic_changed`` — a coarse but honest
    signal. Files are mapped to models via each model's ``resource_path``
    (dbt's ``original_file_path``).
    """
    changed_files = _git_changed_sql_files(git_base, repo_dir)
    if not changed_files:
        return []

    # Map original_file_path -> model name for quick lookup.
    path_to_model: Dict[str, str] = {}
    head_models = head.get_models()
    for model_name, model in head_models.items():
        if model.resource_path:
            path_to_model[_norm_path(model.resource_path)] = model_name

    chosen: Dict[Tuple[str, str], ColumnChange] = {}
    for changed in changed_files:
        matched = path_to_model.get(_norm_path(changed))
        if not matched:
            # A changed file with no matching model (macro, test, deleted file, …).
            continue
        model_name = matched
        model = head_models[model_name]
        for column in sorted(model.columns):
            chosen[(model_name, column)] = ColumnChange(
                model_name, column, ChangeKind.LOGIC_CHANGED, detail=changed
            )

    return sorted(chosen.values(), key=lambda c: (c.model, c.column))


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
