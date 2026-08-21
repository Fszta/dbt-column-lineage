"""Unit tests for the pure AST semantic-diff engine (Package A).

Each row of the matrix calls ``compare_expressions(base, head, dialect="snowflake")`` and
asserts the ``.equal`` / ``.kind`` verdict. Verified against the pinned sqlglot 26.33.0.
Fixture style = inline SQL fragment string pairs (matches the pure-function unit tests).
"""

import pytest

from dbt_column_lineage.lineage.semantic_diff import (
    _UNPARSEABLE_PREFIX,
    canonical_key,
    compare_expressions,
)
from dbt_column_lineage.models.schema import SemanticChangeKind

_DIALECT = "snowflake"


# (id, base, head, expected_equal, expected_kind)
_MATRIX: list[tuple[str, str | None, str | None, bool, SemanticChangeKind]] = [
    ("whitespace", "a + b", "a  +   b", True, SemanticChangeKind.EQUIVALENT),
    ("comment", "a /* c */", "a", True, SemanticChangeKind.EQUIVALENT),
    ("identifier_case", "A + B", "a + b", True, SemanticChangeKind.EQUIVALENT),
    ("function_case", "SUM(x)", "sum( x )", True, SemanticChangeKind.EQUIVALENT),
    ("redundant_parens", "(a + b)", "a + b", True, SemanticChangeKind.EQUIVALENT),
    ("boolean_reorder", "a and b", "b and a", True, SemanticChangeKind.EQUIVALENT),
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
    """Cosmetic-only rows 1–5 must produce identical canonical keys."""
    cosmetic_pairs = [
        ("a + b", "a  +   b"),
        ("a /* c */", "a"),
        ("A + B", "a + b"),
        ("SUM(x)", "sum( x )"),
        ("(a + b)", "a + b"),
    ]
    for base, head in cosmetic_pairs:
        assert canonical_key(base, _DIALECT) == canonical_key(head, _DIALECT)


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
