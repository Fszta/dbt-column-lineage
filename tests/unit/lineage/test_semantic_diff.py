"""Unit tests for the pure AST semantic-diff engine (Package A).

Each row of the matrix calls ``compare_expressions(base, head, dialect="snowflake")`` and
asserts the ``.equal`` / ``.kind`` verdict. Verified against the pinned sqlglot 26.33.0.
Fixture style = inline SQL fragment string pairs (matches the pure-function unit tests).
"""

import pytest

from parrant.lineage.semantic_diff import (
    _UNPARSEABLE_PREFIX,
    canonical_key,
    compare_expressions,
)
from parrant.models.schema import SemanticChangeKind

_DIALECT = "snowflake"


# (id, base, head, expected_equal, expected_kind)
_MATRIX: list[tuple[str, str | None, str | None, bool, SemanticChangeKind]] = [
    ("whitespace", "a + b", "a  +   b", True, SemanticChangeKind.EQUIVALENT),
    ("comment", "a /* c */", "a", True, SemanticChangeKind.EQUIVALENT),
    ("identifier_case", "A + B", "a + b", True, SemanticChangeKind.EQUIVALENT),
    ("function_case", "SUM(x)", "sum( x )", True, SemanticChangeKind.EQUIVALENT),
    # Conservative-but-sound: canonicalization no longer runs ``simplify`` (unsound under SQL
    # three-valued logic), so redundant parens and commutative reordering — folds only
    # ``simplify`` provided — now read as MEANING_CHANGED (over-block, the safe direction).
    # A NULL-safe rewrite set that restores these soundly is deferred follow-up work.
    ("redundant_parens", "(a + b)", "a + b", False, SemanticChangeKind.MEANING_CHANGED),
    ("boolean_reorder", "a and b", "b and a", False, SemanticChangeKind.MEANING_CHANGED),
    ("arithmetic_reorder", "a + b", "b + a", False, SemanticChangeKind.MEANING_CHANGED),
    ("changed_operator", "a + b", "a - b", False, SemanticChangeKind.MEANING_CHANGED),
    ("changed_cast", "a::int", "a::text", False, SemanticChangeKind.MEANING_CHANGED),
    (
        "changed_case_predicate",
        "case when a>0 then 1 else 0 end",
        "case when a>=0 then 1 else 0 end",
        False,
        SemanticChangeKind.MEANING_CHANGED,
    ),
    (
        "changed_source_column",
        "coalesce(a, z)",
        "coalesce(a, y)",
        False,
        SemanticChangeKind.MEANING_CHANGED,
    ),
    ("quoted_vs_unquoted", '"a"', "a", False, SemanticChangeKind.MEANING_CHANGED),
    ("unparseable_base", "a +", "a + b", False, SemanticChangeKind.INDETERMINATE),
    ("unparseable_head", "a + b", "a +", False, SemanticChangeKind.INDETERMINATE),
    ("identical", "a + b", "a + b", True, SemanticChangeKind.EQUIVALENT),
    ("both_none", None, None, True, SemanticChangeKind.EQUIVALENT),
]


@pytest.mark.parametrize(
    "base, head, expected_equal, expected_kind",
    [(row[1], row[2], row[3], row[4]) for row in _MATRIX],
    ids=[row[0] for row in _MATRIX],
)
def test_compare_expressions_matrix(
    base: str | None,
    head: str | None,
    expected_equal: bool,
    expected_kind: SemanticChangeKind,
) -> None:
    result = compare_expressions(base, head, dialect=_DIALECT)
    assert result.equal is expected_equal
    assert result.kind is expected_kind
    # Fail-safe invariant: only a proven EQUIVALENT is non-breaking.
    assert result.kind.is_breaking is (not expected_equal)


def test_indeterminate_never_equal() -> None:
    """A parse failure must never be reported as equal (fail-safe)."""
    result = compare_expressions("a +", "a + b", dialect=_DIALECT)
    assert result.equal is False
    assert result.kind is SemanticChangeKind.INDETERMINATE


def test_canonical_key_equal_for_cosmetic_rows() -> None:
    """Provably meaning-preserving cosmetic edits must produce identical canonical keys.

    (Redundant parens are intentionally excluded now — see the matrix note: without an unsound
    ``simplify`` pass they no longer fold, and over-blocking is the correct failure direction.)
    """
    cosmetic_pairs = [
        ("a + b", "a  +   b"),
        ("a /* c */", "a"),
        ("A + B", "a + b"),
        ("SUM(x)", "sum( x )"),
    ]
    for base, head in cosmetic_pairs:
        assert canonical_key(base, _DIALECT) == canonical_key(head, _DIALECT)


def test_boolean_simplification_is_never_equivalent_under_null_logic() -> None:
    """Regression: boolean rewrites that are valid in two-valued logic but WRONG under SQL's
    three-valued (NULL) logic must never be reported EQUIVALENT.

    Each pair below differs whenever an operand is NULL, so calling them equal would silently
    pass a breaking change — the one failure this engine must never make. These asserted
    EQUIVALENT before the ``simplify`` pass was removed.
    """
    unsound_pairs = [
        ("(x AND y) OR (x AND NOT y)", "x"),  # x=T, y=NULL -> base NULL, head TRUE
        ("a OR NOT a", "TRUE"),  # a=NULL -> base NULL, head TRUE
        ("(x > 5) AND NOT (x > 5)", "FALSE"),  # x=NULL -> base NULL, head FALSE
    ]
    for base, head in unsound_pairs:
        result = compare_expressions(base, head, dialect=_DIALECT)
        assert result.equal is False, f"{base!r} vs {head!r} must not be equal (NULL-unsound)"
        assert result.kind is SemanticChangeKind.MEANING_CHANGED
        assert result.kind.is_breaking is True


def test_canonical_key_differs_for_meaning_change() -> None:
    assert canonical_key("a + b", _DIALECT) != canonical_key("b + a", _DIALECT)
    assert canonical_key('"a"', _DIALECT) != canonical_key("a", _DIALECT)


def test_canonical_key_unparseable_namespaced() -> None:
    """An unparseable fragment yields a key in the disjoint sentinel namespace."""
    key = canonical_key("a +", _DIALECT)
    assert key.startswith(_UNPARSEABLE_PREFIX)
    # Byte-identical unparseable fragments compare equal (genuinely unchanged)...
    assert canonical_key("a +", _DIALECT) == canonical_key("a +", _DIALECT)
    # ...but can never collide with a real canonical form.
    assert canonical_key("a +", _DIALECT) != canonical_key("a + b", _DIALECT)


def test_canonical_key_none_namespaced() -> None:
    key = canonical_key(None, _DIALECT)
    assert key.startswith(_UNPARSEABLE_PREFIX)


# --------------------------------------------------------------------------------------
# Comment / whitespace changes must ALWAYS be EQUIVALENT, even when parse_one fails.
# --------------------------------------------------------------------------------------

# A realistic-but-synthetic multi-CTE model with a join and a window function. It also uses
# Snowflake semi-structured access (``payload:total::number``), which parse_one CANNOT build
# an AST for under the generic dialect — exercising the parse-failure fallback path that,
# before the fix, misclassified a comment-only edit as INDETERMINATE (the CI repro).
_MODEL_SQL = """with
orders as (
    select
        order_id,
        customer_id,
        payload:total::number as order_total,
        created_at
    from raw_events.orders
),

customers as (
    select customer_id, region from raw_events.customers
),

joined as (
    select
        orders.order_id,
        orders.order_total,
        customers.region
    from orders
    left join customers
        on orders.customer_id = customers.customer_id
),

final as (
    select
        order_id,
        order_total,
        region,
        sum(order_total) over (partition by region) as region_total
    from joined
)

select * from final"""


# (id, head) — each head differs from _MODEL_SQL ONLY by comments and/or whitespace.
_COMMENT_WHITESPACE_HEADS: list[tuple[str, str]] = [
    ("trailing_line_comment", _MODEL_SQL + " -- expose region_total for the weekly rollup"),
    ("leading_comment", "-- weekly regional totals\n" + _MODEL_SQL),
    (
        "inline_mid_expression_comment",
        _MODEL_SQL.replace(
            "sum(order_total) over",
            "sum(order_total) /* running per region */ over",
        ),
    ),
    (
        "block_comment",
        _MODEL_SQL.replace("select * from final", "select * from final /* terminal passthrough */"),
    ),
    (
        "whitespace_and_indentation",
        _MODEL_SQL.replace("    ", "        ").replace("\n\n", "\n\n\n"),
    ),
    ("trailing_newline", _MODEL_SQL + "\n"),
]


@pytest.mark.parametrize(
    "head",
    [head for _, head in _COMMENT_WHITESPACE_HEADS],
    ids=[name for name, _ in _COMMENT_WHITESPACE_HEADS],
)
@pytest.mark.parametrize("dialect", [None, "snowflake"], ids=["generic_dialect", "snowflake"])
def test_comment_and_whitespace_only_changes_are_equivalent(head: str, dialect: str | None) -> None:
    """A change that only adds/removes/edits comments or whitespace has no semantic value.

    Parametrized over the generic dialect (where parse_one fails on the semi-structured
    access and the OLD code fell to INDETERMINATE) and Snowflake (where the AST path already
    proved EQUIVALENT). Both must now be EQUIVALENT — comments and formatting carry no meaning.
    """
    result = compare_expressions(_MODEL_SQL, head, dialect=dialect)
    assert (
        result.kind is SemanticChangeKind.EQUIVALENT
    ), f"comment/whitespace-only change classified {result.kind.value}, expected equivalent"
    assert result.equal is True


def test_comment_only_change_survives_parse_failure() -> None:
    """Regression (CI repro): the model uses SQL parse_one cannot AST under the generic dialect,
    so the ONLY route to a correct verdict is the lexical comment-stripped comparison.

    Guards the parse-failure precondition explicitly so this stays a real regression test.
    """
    assert canonical_key(_MODEL_SQL, None).startswith(
        _UNPARSEABLE_PREFIX
    ), "precondition: _MODEL_SQL must be unparseable under the generic dialect for this repro"
    head = _MODEL_SQL + " -- trailing note added by a downstream consumer"
    result = compare_expressions(_MODEL_SQL, head, dialect=None)
    assert result.equal is True
    assert result.kind is SemanticChangeKind.EQUIVALENT


def test_string_literal_dashes_are_not_treated_as_comments() -> None:
    """Soundness: a ``--`` (or ``/* */``) INSIDE a string literal is data, not a comment.

    Changing such a literal is a real meaning change and must NOT be swallowed by comment
    stripping. The tokenizer keeps string contents intact, so this stays MEANING_CHANGED.
    """
    base = "select label from t where note = '-- not a comment'"
    head = "select label from t where note = '-- different literal'"
    result = compare_expressions(base, head, dialect=_DIALECT)
    assert result.equal is False
    assert result.kind is SemanticChangeKind.MEANING_CHANGED


def test_comment_only_change_collapses_unparseable_canonical_keys() -> None:
    """Two byte-different-but-comment-only-different UNPARSEABLE fragments share a canonical key,
    so a per-column signature over them suppresses the cosmetic edit instead of flagging it."""
    base = "a +"  # unparseable fragment
    head = "a + -- dangling note"
    assert canonical_key(base, _DIALECT).startswith(_UNPARSEABLE_PREFIX)
    assert canonical_key(base, _DIALECT) == canonical_key(head, _DIALECT)
    # ...but a real token change still differs.
    assert canonical_key(base, _DIALECT) != canonical_key("a + b +", _DIALECT)
