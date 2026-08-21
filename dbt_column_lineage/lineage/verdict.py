"""Provable-break classifier: turn a changeset into a blockable verdict.

The blast radius answers "what does this change *reach*?". A reviewer's real question is
"will it *break* anything, and may I merge?". This module answers the blockable half of
that offline, from dbt artifacts alone: a column a change removes/renames that a dbt test
still targets will fail on the next ``dbt build`` — a deterministic, warehouse-free FAIL.

Only *provable* breaks drive a BLOCK verdict. Heuristic reach (derived recompute, exposures)
is REVIEW, and everything else is SAFE — so a team can flip ``--fail-on tests`` from warn to
block without fearing false positives. "Will the *value* be wrong" needs the warehouse and is
deliberately out of scope.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.lineage.changeset import ChangeKind, ColumnChange
from dbt_column_lineage.models.schema import BreakFinding, TestNode

# Change kinds that can orphan a test by making the column disappear. A rename is emitted by
# the changeset builder as REMOVED(old) + ADDED(new), so REMOVED already covers it; RENAMED
# is handled too for any caller that produces it directly.
_COLUMN_GONE_KINDS = (ChangeKind.REMOVED, ChangeKind.RENAMED)


def _column_missing_in_head(head: ModelRegistry, model: str, column: str) -> bool:
    """True when ``model.column`` no longer exists in the head registry.

    This is the guard that keeps a break *provable*: we only flag a test as broken when the
    column it targets is genuinely absent from head — either the whole model was removed, or
    the column was dropped/renamed away. If head still exposes the column (e.g. a mis-derived
    change, or a removal the PR reverted), we do NOT flag it.
    """
    head_model = head.get_models().get(model.lower())
    if head_model is None:
        return True  # whole model gone → its columns are certainly gone
    return column.lower() not in {c.lower() for c in head_model.columns}


def classify_provable_breaks(
    changes: List[ColumnChange],
    head_registry: ModelRegistry,
    base_registry: Optional[ModelRegistry] = None,
) -> List[BreakFinding]:
    """Return the dbt tests that a changeset provably breaks (BREAK-TEST).

    For each removed/renamed column we look up the tests that targeted it in the *base*
    registry (where the column and its tests both existed), then confirm the column is truly
    gone in *head*. Both sides of a ``relationships`` test are checked: dropping either the
    child column or the referenced parent key breaks it. Deduplicated by test per change.

    ``base_registry`` is the reliable source of pre-change tests. Without it (git-diff
    fallback, which only yields ``logic_changed``) there is nothing to prove, so no breaks
    are returned — an honest empty result rather than a guess.

    A base test is only flagged when it *still exists in head* (same ``unique_id``). dbt
    encodes a generic test's target column into its unique_id, so a rename that updates the
    test's yml produces a new id — the base id is gone and is correctly NOT flagged
    (removing a false block on the dominant "rename + update the test" refactor). A rename
    that leaves the yml stale keeps the old id in head, targeting a now-missing column, and
    IS flagged. Deleting a column together with its test also drops the id → not flagged.
    """
    source = base_registry or head_registry
    head_test_ids = head_registry.get_test_unique_ids()
    findings: List[BreakFinding] = []
    # Dedup across the whole changeset: a relationships test whose child column AND
    # referenced parent key are both removed in one PR must count once, not twice.
    seen: Set[str] = set()

    def _emit(model: str, column: str, kind: str, test: TestNode, via_reference: bool) -> None:
        # Only a test that survives into head (still declared) can actually fail on build.
        if test.unique_id not in head_test_ids or test.unique_id in seen:
            return
        seen.add(test.unique_id)
        findings.append(
            BreakFinding(
                break_kind="break_test",
                change_model=model,
                change_column=column,
                change_kind=kind,
                test_name=test.test_name,
                test_unique_id=test.unique_id,
                resource_path=test.resource_path,
                via_reference=via_reference,
            )
        )

    # A model absent from head entirely takes ALL its tests down with it — including tests on
    # columns we couldn't recover from compiled SQL (incomplete column recovery would
    # otherwise make a dropped model read as SAFE). Handle these once per model.
    wholly_removed = {
        c.model
        for c in changes
        if c.kind in _COLUMN_GONE_KINDS and head_registry.get_models().get(c.model.lower()) is None
    }
    for model in wholly_removed:
        for test in source.get_model_tests(model):
            attached_here = (test.target_model or "").lower() == model.lower()
            # The column that's gone: the tested column for an attached test, or the parent
            # key for a relationships test that referenced this (now-removed) model.
            column = (test.target_column if attached_here else test.referenced_column) or model
            _emit(model, column, ChangeKind.REMOVED.value, test, via_reference=not attached_here)

    for change in changes:
        if change.kind not in _COLUMN_GONE_KINDS or change.model in wholly_removed:
            continue
        if not _column_missing_in_head(head_registry, change.model, change.column):
            # The column still exists in head — no orphaned test, nothing provable.
            continue

        for test in source.get_column_tests(change.model, change.column):
            _emit(change.model, change.column, change.kind.value, test, via_reference=False)
        # relationships tests whose *referenced* parent key is this removed column.
        for test in source.get_tests_referencing(change.model, change.column):
            _emit(change.model, change.column, change.kind.value, test, via_reference=True)

    return findings


def decide_verdict(breaks: List[BreakFinding], summary: Dict[str, Any]) -> str:
    """Collapse breaks + blast-radius summary into a single ruling.

    - ``block``: at least one provable break — objective enough to fail a gate.
    - ``review``: no provable break, but the change recomputes downstream logic, shifts a
      row-set, or reaches a business-facing exposure — a human should look.
    - ``safe``: nothing downstream is recomputed and no exposure is touched.
    """
    if breaks:
        return "block"
    reaches_review = (
        summary.get("critical_count", 0)
        or summary.get("filter_count", 0)
        or summary.get("affected_exposures", 0)
    )
    return "review" if reaches_review else "safe"
