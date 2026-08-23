"""Pure AST semantic-diff engine for per-column defining expressions.

No I/O, no dbt artifacts, no registry — SQL expression strings in, a verdict out. This is
the first factor of the roadmap formula (AST semantic-diff × column-level lineage). Its only
consumer is ``changeset.py``.

Canonicalization pipeline:
``parse_one(sql, dialect)`` → ``normalize_identifiers(dialect)`` →
render with ``.sql(dialect=..., comments=False, normalize_functions="upper")``.

Fail-safe is load-bearing: anything that is not a *proven* ``EQUIVALENT`` is classified as
breaking (``MEANING_CHANGED`` or ``INDETERMINATE``). A parse failure never yields "equal".

Soundness note — why there is no ``simplify`` pass: sqlglot's ``simplify`` applies boolean
absorption / complement / collapse rules (``a OR NOT a`` → ``TRUE``, ``x AND NOT x`` → ``FALSE``,
``(x AND y) OR (x AND NOT y)`` → ``x``) that are valid in two-valued logic but **wrong under
SQL's three-valued (NULL) logic** — the two sides differ whenever an operand is NULL. Running it
made the engine report such genuinely-breaking rewrites as ``EQUIVALENT`` (a false negative that
silently passes a breaking change — the one thing this engine must never do). Canonicalization is
therefore limited to parse + identifier/whitespace/comment/function-name normalization, which is
provably meaning-preserving. This is conservative: sound cosmetic folds that only ``simplify``
provided (redundant parens, commutative reordering) now read as ``MEANING_CHANGED`` — over-blocking,
the correct failure direction for a gate. Restoring them soundly (a curated, NULL-safe rewrite set)
is deferred follow-up work.
"""

from __future__ import annotations

from functools import lru_cache

from sqlglot import exp, parse_one
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from dbt_column_lineage.models.schema import SemanticChangeKind, SemanticDiff

# Sentinel prefix for a canonical key we could not parse. Namespaced so a parse-failure key
# can never accidentally equal a real canonicalized expression string.
_UNPARSEABLE_PREFIX: str = "\x00unparseable:"


@lru_cache(maxsize=4096)
def canonicalize_expression(expr_sql: str, dialect: str | None = None) -> exp.Expression | None:
    """Parse a SQL expression fragment and return its canonical AST, or ``None``.

    Pipeline: ``parse_one(expr_sql, dialect)`` → ``normalize_identifiers(dialect)``.
    Canonicalization folds only *provably* meaning-preserving cosmetic differences
    (whitespace, comments, identifier case, function-name case) into one form so two
    cosmetically-different-but-equivalent expressions produce equal ASTs (``==``). It deliberately
    does **not** run sqlglot's ``simplify`` — see the module docstring's soundness note: those
    boolean rewrites are unsound under SQL's three-valued (NULL) logic and would let a breaking
    change be reported ``EQUIVALENT``.

    Returns ``None`` when ``parse_one`` raises (``ParseError`` / unsupported) — the caller must
    treat ``None`` as *indeterminate → breaking* (fail-safe), never as "equal".
    """
    try:
        expression = parse_one(expr_sql, dialect=dialect)
    except Exception:  # noqa: BLE001 - fail-safe: any parse failure -> None (indeterminate)
        return None

    return normalize_identifiers(expression, dialect=dialect)


@lru_cache(maxsize=4096)
def canonical_key(expr_sql: str | None, dialect: str | None = None) -> str:
    """Return a stable, hashable canonical key for an expression, for signature comparison.

    - Parseable  → the canonical SQL string
      (``.sql(dialect=dialect, comments=False, normalize_functions="upper")``).
    - ``None`` / unparseable → ``_UNPARSEABLE_PREFIX + <raw stripped string>`` — a value in a
      disjoint namespace, so two byte-identical unparseable fragments still compare equal
      (genuinely unchanged) while an unparseable fragment can never equal a canonical form.

    This is the drop-in replacement for ``changeset._normalize_sql`` inside the per-column
    signature: equal keys ⇒ semantically equal ⇒ not a logic change.
    """
    raw = expr_sql or ""
    if expr_sql is None:
        return _UNPARSEABLE_PREFIX + raw.strip()

    canonical = canonicalize_expression(expr_sql, dialect)
    if canonical is None:
        return _UNPARSEABLE_PREFIX + raw.strip()

    return canonical.sql(dialect=dialect, comments=False, normalize_functions="upper")


def compare_expressions(
    base_sql: str | None, head_sql: str | None, dialect: str | None = None
) -> SemanticDiff:
    """Classify the change between two column-defining expressions.

    Decision order (fail-safe):
      1. both ``None``/empty, or byte-identical raw string → ``EQUIVALENT`` (equal=True).
      2. either side fails to parse (``canonicalize_expression`` → ``None``) → ``INDETERMINATE``.
      3. canonical ASTs equal (``==``) → ``EQUIVALENT`` (equal=True).
      4. otherwise → ``MEANING_CHANGED`` (equal=False).

    ``reason`` is a short display string (e.g. ``"expressions are semantically equivalent"``,
    ``"could not parse base expression"``, ``"expression meaning changed"``). It is advisory;
    ``kind`` / ``equal`` are the load-bearing outputs.
    """
    # 1. Fast paths: both empty, or byte-identical raw strings.
    if not base_sql and not head_sql:
        return SemanticDiff(
            equal=True,
            kind=SemanticChangeKind.EQUIVALENT,
            reason="both expressions are empty",
        )
    if base_sql == head_sql:
        return SemanticDiff(
            equal=True,
            kind=SemanticChangeKind.EQUIVALENT,
            reason="expressions are identical",
        )

    # 2. Fail-safe: an empty side against a non-empty side is not provably equivalent.
    if not base_sql or not head_sql:
        side = "base" if not base_sql else "head"
        return SemanticDiff(
            equal=False,
            kind=SemanticChangeKind.INDETERMINATE,
            reason=f"missing {side} expression",
        )

    base_canonical = canonicalize_expression(base_sql, dialect)
    if base_canonical is None:
        return SemanticDiff(
            equal=False,
            kind=SemanticChangeKind.INDETERMINATE,
            reason="could not parse base expression",
        )
    head_canonical = canonicalize_expression(head_sql, dialect)
    if head_canonical is None:
        return SemanticDiff(
            equal=False,
            kind=SemanticChangeKind.INDETERMINATE,
            reason="could not parse head expression",
        )

    # 3. Canonical ASTs equal ⇒ cosmetic-only change.
    if base_canonical == head_canonical:
        return SemanticDiff(
            equal=True,
            kind=SemanticChangeKind.EQUIVALENT,
            reason="expressions are semantically equivalent",
        )

    # 4. Both parse, canonical forms differ ⇒ a real meaning change.
    return SemanticDiff(
        equal=False,
        kind=SemanticChangeKind.MEANING_CHANGED,
        reason="expression meaning changed",
    )
