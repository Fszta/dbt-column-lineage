"""Tests for UPSTREAM meta propagation via the ``effective.*`` predicate namespace.

Covers ``MetaIndex.effective_meta`` (folding a key's value over the column DAG) and the
``effective.<key>`` predicate leaf on the subject, exercising the taint semantics the feature
was designed around:

  * multi-hop inheritance (a classification declared once is inherited downstream);
  * an unresolvable break -> UNKNOWN -> fail-closed (cannot prove NOT-PII => block);
  * declassification (own ``pii: false`` halts the taint) and reclassification (own wins up);
  * the two registered fold strategies (pii = most-restrictive ``true>unknown>false``;
    secret = boolean OR) including the ``unknown outranks false`` rung;
  * diamond de-duplication + the cycle guard (memoized, no infinite loop, no double-count);
  * a regression guard proving ``effective.*`` and ``meta.*`` are genuinely different axes
    (own-node ``meta.pii is_true`` on an absent key is FALSE, never UNKNOWN).

Mirrors the harness in ``test_policy_engine.py`` (hand-written FakeRegistry + by_change impact
dicts), extended with a ``get_column_lineage`` accessor so the upstream fold has a DAG to walk.
"""

import collections

from parrant.lineage.changeset import ChangeKind, ColumnChange
from parrant.lineage.policy import (
    MetaIndex,
    PolicyEngine,
    build_impact_view,
)
from parrant.models.schema import (
    ColumnLineage,
    GateDecision,
    SemanticChangeKind,
)


# --- fakes ------------------------------------------------------------------


class FakeRegistry:
    """Registry stand-in with the meta accessors *and* a column-lineage accessor.

    ``column_lineage`` maps a lowercased ``(model, column)`` to the set of ``model.col``
    upstream refs feeding it. A missing entry -> no edge (a root/literal); an entry with an
    EMPTY set -> an edge with no resolvable sources (e.g. ``select *`` / a derived column).
    Call counters let a test prove the DAG fold visits each node once (memoization).
    """

    def __init__(self, column_meta=None, model_meta=None, column_lineage=None):
        self._column_meta = column_meta or {}
        self._model_meta = model_meta or {}
        self._column_lineage = column_lineage or {}
        self.lineage_calls = collections.Counter()
        self.column_meta_calls = collections.Counter()

    def get_model_dbt_meta(self, model):
        return dict(self._model_meta.get(model, {}))

    def get_column_dbt_meta(self, model, column):
        self.column_meta_calls[(model, column)] += 1
        return dict(self._column_meta.get((model, column), {}))

    def get_column_lineage(self, model, column):
        self.lineage_calls[(model, column)] += 1
        refs = self._column_lineage.get((model, column))
        if refs is None:
            return []
        return [ColumnLineage(source_columns=set(refs), transformation_type="renamed")]


def _impact(*entries):
    return {"by_change": list(entries)}


def _resolved(model, column, kind="logic_changed"):
    return {
        "model": model,
        "column": column,
        "kind": kind,
        "resolved": True,
        "reached_models": [],
        "reached_columns": [],
        "reached_exposures": [],
    }


def _engine(policy, registry, impact=None):
    return PolicyEngine(policy, MetaIndex(registry), build_impact_view(impact or _impact()), [])


def _change(model="mart", column="ssn", kind=ChangeKind.LOGIC_CHANGED):
    return ColumnChange(model, column, kind, semantic=SemanticChangeKind.MEANING_CHANGED)


def _effective_policy(key="pii", op="is_true", action="block", defaults=None):
    policy = {
        "version": 1,
        "rules": [
            {
                "id": "eff",
                "predicate": {"effective": {"key": key, "op": op}},
                "action": [{"type": action}],
            }
        ],
    }
    if defaults is not None:
        policy["defaults"] = defaults
    from parrant.lineage.policy import parse_policy

    return parse_policy(policy)


def _meta_policy(key="pii", op="is_true", action="block", defaults=None):
    policy = {
        "version": 1,
        "rules": [
            {
                "id": "own",
                "predicate": {"meta": {"key": key, "op": op}},
                "action": [{"type": action}],
            }
        ],
    }
    if defaults is not None:
        policy["defaults"] = defaults
    from parrant.lineage.policy import parse_policy

    return parse_policy(policy)


# --- case 1: 3-hop inheritance ----------------------------------------------


def test_three_hop_inheritance_propagates_pii_and_matches():
    """source column ``pii: true`` -> int (plain rename, no meta) -> mart (no meta): the mart
    column's EFFECTIVE pii is true, and a subject ``effective.pii is_true`` rule matches for real
    (a proven TRUE, not a fail-safe fire)."""
    registry = FakeRegistry(
        column_meta={("src", "ssn"): {"pii": True}},
        column_lineage={
            ("int_accounts", "ssn"): {"src.ssn"},
            ("mart", "ssn"): {"int_accounts.ssn"},
        },
    )
    lookup = MetaIndex(registry).effective_meta("mart", "ssn", "pii")
    assert lookup.present is True
    assert lookup.value is True

    verdict = _engine(_effective_policy(), registry).evaluate([_change("mart", "ssn")])
    assert verdict.blocks()
    hit = next(h for h in verdict.hits if h.rule_id == "eff")
    assert hit.fired_on_unknown is False  # a real match, not a fail-safe fire


# --- case 2: unresolved break -> fail-closed --------------------------------


def test_unresolved_upstream_is_unknown_and_fails_closed():
    """A downstream column whose upstream cannot be resolved (an edge with empty source_columns,
    e.g. ``select *`` / a derived expression) -> effective UNKNOWN (present=False). A BLOCKING
    ``effective.pii is_true`` rule then fires via the fail-safe path under ``fail_closed`` (marked
    ``fired_on_unknown``), and does NOT fire under ``skip`` / ``fail_open``."""
    registry = FakeRegistry(column_lineage={("mart", "ssn"): set()})

    lookup = MetaIndex(registry).effective_meta("mart", "ssn", "pii")
    assert lookup.present is False  # cannot prove it is (or is not) PII

    # fail_closed (default): a blocking rule fires on the undecidable predicate.
    verdict = _engine(_effective_policy(), registry).evaluate([_change("mart", "ssn")])
    assert verdict.blocks()
    hit = next(h for h in verdict.hits if h.rule_id == "eff")
    assert hit.fired_on_unknown is True
    assert hit.unknown_cause == "missing"

    # skip: dropped for the subject, counted; no fire.
    skip_policy = _effective_policy(defaults={"on_missing_meta": "skip"})
    skip_verdict = _engine(skip_policy, registry).evaluate([_change("mart", "ssn")])
    assert skip_verdict.decision is GateDecision.ALLOW
    assert skip_verdict.skipped_missing_meta == 1
    assert skip_verdict.hits == []

    # fail_open: never fires on UNKNOWN.
    open_policy = _effective_policy(defaults={"on_missing_meta": "fail_open"})
    open_verdict = _engine(open_policy, registry).evaluate([_change("mart", "ssn")])
    assert open_verdict.decision is GateDecision.ALLOW
    assert open_verdict.hits == []


# --- case 3: declassification override --------------------------------------


def test_declassification_own_false_halts_upstream_taint():
    """Upstream ``pii: true`` but the downstream column declares its OWN ``pii: false``: the own
    value wins verbatim and the taint stops there (effective == false)."""
    registry = FakeRegistry(
        column_meta={("src", "ssn"): {"pii": True}, ("mart", "ssn_masked"): {"pii": False}},
        column_lineage={("mart", "ssn_masked"): {"src.ssn"}},
    )
    lookup = MetaIndex(registry).effective_meta("mart", "ssn_masked", "pii")
    assert lookup.present is True
    assert lookup.value is False

    # An ``effective.pii is_true`` rule must NOT fire on a declassified column.
    verdict = _engine(_effective_policy(), registry).evaluate([_change("mart", "ssn_masked")])
    assert verdict.decision is GateDecision.ALLOW


# --- case 4: reclassification override --------------------------------------


def test_reclassification_own_true_overrides_absent_upstream():
    """Downstream own ``pii: true`` over an upstream that is false/absent -> effective true (own
    wins, upstream is never consulted)."""
    registry = FakeRegistry(
        column_meta={("src", "col"): {"pii": False}, ("mart", "flagged"): {"pii": True}},
        column_lineage={("mart", "flagged"): {"src.col"}},
    )
    lookup = MetaIndex(registry).effective_meta("mart", "flagged", "pii")
    assert lookup.present is True
    assert lookup.value is True

    verdict = _engine(_effective_policy(), registry).evaluate([_change("mart", "flagged")])
    assert verdict.blocks()


# --- case 5: secret OR-propagation ------------------------------------------


def test_secret_or_propagates_but_own_false_halts():
    """``secret`` folds by boolean OR: any upstream truthy => downstream effective true; but a
    downstream OWN ``secret: false`` still wins and halts propagation."""
    # (a) any upstream secret:true -> downstream effective true.
    prop = FakeRegistry(
        column_meta={("src", "token"): {"secret": True}},
        column_lineage={("mart", "token"): {"src.token"}},
    )
    prop_lookup = MetaIndex(prop).effective_meta("mart", "token", "secret")
    assert prop_lookup.present is True
    assert prop_lookup.value is True
    assert _engine(_effective_policy(key="secret"), prop).evaluate([_change("mart", "token")]).blocks()

    # (b) downstream own secret:false wins over an upstream secret:true.
    halt = FakeRegistry(
        column_meta={("src", "token"): {"secret": True}, ("mart", "token"): {"secret": False}},
        column_lineage={("mart", "token"): {"src.token"}},
    )
    halt_lookup = MetaIndex(halt).effective_meta("mart", "token", "secret")
    assert halt_lookup.present is True
    assert halt_lookup.value is False
    halt_verdict = _engine(_effective_policy(key="secret"), halt).evaluate([_change("mart", "token")])
    assert halt_verdict.decision is GateDecision.ALLOW


# --- case 6: most-restrictive fold ------------------------------------------


def test_most_restrictive_fold_true_dominates():
    """Multiple upstreams mixing true/false/unknown -> true dominates (the ``max`` over the
    lattice)."""
    registry = FakeRegistry(
        column_meta={("up_true", "c"): {"pii": True}, ("up_false", "c"): {"pii": False}},
        column_lineage={
            # up_unknown is a root with no meta -> UNKNOWN.
            ("mart", "c"): {"up_true.c", "up_false.c", "up_unknown.c"},
        },
    )
    lookup = MetaIndex(registry).effective_meta("mart", "c", "pii")
    assert lookup.present is True
    assert lookup.value is True


def test_most_restrictive_fold_unknown_outranks_false():
    """{false, unknown} -> UNKNOWN: a proven-false does NOT win over an unresolved upstream
    (``UNKNOWN`` ranks above ``LOW``), so the result is present=False (fail-safe)."""
    registry = FakeRegistry(
        column_meta={("up_false", "c"): {"pii": False}},
        column_lineage={
            # up_unknown: root with no meta -> UNKNOWN.
            ("mart", "c"): {"up_false.c", "up_unknown.c"},
        },
    )
    lookup = MetaIndex(registry).effective_meta("mart", "c", "pii")
    assert lookup.present is False


# --- case 7: diamond + cycle guard ------------------------------------------


def test_diamond_resolves_once_no_double_count():
    """A diamond (two paths converging on the same tagged root) resolves to true, and the shared
    intermediate node is folded exactly ONCE (memoized) — proving no double-count / re-walk."""
    registry = FakeRegistry(
        column_meta={("src", "ssn"): {"pii": True}},
        column_lineage={
            ("mid", "ssn"): {"src.ssn"},
            ("left", "ssn"): {"mid.ssn"},
            ("right", "ssn"): {"mid.ssn"},
            ("mart", "ssn"): {"left.ssn", "right.ssn"},
        },
    )
    index = MetaIndex(registry)
    lookup = index.effective_meta("mart", "ssn", "pii")
    assert lookup.present is True
    assert lookup.value is True
    # Both diamond arms reach ``mid`` but the memo means its lineage is walked only once.
    assert registry.lineage_calls[("mid", "ssn")] == 1


def test_pure_cycle_terminates_as_unknown():
    """A cyclic ref (a <-> b, and a self-ref) does not infinite-loop and resolves deterministically
    to UNKNOWN (nothing tagged anywhere along the cycle)."""
    registry = FakeRegistry(
        column_lineage={
            ("a", "c"): {"b.c"},
            ("b", "c"): {"a.c"},
            ("self", "c"): {"self.c"},
        }
    )
    index = MetaIndex(registry)
    assert index.effective_meta("a", "c", "pii").present is False
    assert index.effective_meta("self", "c", "pii").present is False


def test_cycle_with_tagged_escape_still_resolves_true():
    """The cycle guard breaks the loop WITHOUT losing a genuine taint reachable off the cycle: a
    node in an a<->b cycle that also draws from a ``pii: true`` root resolves to true."""
    registry = FakeRegistry(
        column_meta={("root", "c"): {"pii": True}},
        column_lineage={
            ("a", "c"): {"b.c", "root.c"},
            ("b", "c"): {"a.c"},
        },
    )
    lookup = MetaIndex(registry).effective_meta("a", "c", "pii")
    assert lookup.present is True
    assert lookup.value is True


# --- case 8: regression -- meta.* vs effective.* are different axes ----------


def test_meta_axis_is_own_node_only_and_absent_is_false_not_unknown():
    """The regression guard: on a column with NO own pii but a ``pii: true`` UPSTREAM,

      * ``meta.pii is_true`` reads only the OWN node -> absent -> FALSE (a *total* operator, NOT
        UNKNOWN), so under the default ``fail_closed`` the blocking rule still does NOT fire;
      * ``effective.pii is_true`` folds the lineage -> TRUE -> the blocking rule fires.

    Proving the two axes are genuinely distinct, exactly as designed."""
    registry = FakeRegistry(
        column_meta={("src", "ssn"): {"pii": True}},
        column_lineage={("mart", "ssn"): {"src.ssn"}},
    )
    index = MetaIndex(registry)

    # API level: own meta absent, but effective folds to true.
    assert index.subject_meta("mart", "ssn", "pii").present is False
    effective = index.effective_meta("mart", "ssn", "pii")
    assert effective.present is True
    assert effective.value is True

    # meta.pii is_true (own-node) under default fail_closed: absent => FALSE => no fire.
    meta_verdict = _engine(_meta_policy(), registry).evaluate([_change("mart", "ssn")])
    assert meta_verdict.decision is GateDecision.ALLOW
    assert meta_verdict.hits == []

    # effective.pii is_true: folded => TRUE => blocks.
    eff_verdict = _engine(_effective_policy(), registry).evaluate([_change("mart", "ssn")])
    assert eff_verdict.blocks()
