"""Unit tests for override-pragma parsing (``parser/sql_parser.parse_override_directives``).

Pure raw-text scanning — no sqlglot, no artifacts. Covers both verbs, explicit column arg,
line-adjacency, model scope, and the audit invariants (reasonless / unknown-verb / malformed
pragmas are DROPPED as warnings, never constructed).
"""

from dbt_column_lineage.models.schema import OverrideVerb
from dbt_column_lineage.parser.sql_parser import (
    _extract_select_alias,
    parse_override_directives,
)


def test_explicit_column_arg_sets_column_scope():
    sql = 'select\n  -- lineage:allow-change column=amount_eur reason="renamed"\n  amount as amount_eur\n'
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert len(directives) == 1
    d = directives[0]
    assert d.verb is OverrideVerb.ALLOW_CHANGE
    assert d.column == "amount_eur"
    assert d.scope == "column"
    assert d.reason == "renamed"


def test_line_adjacency_resolves_next_projected_column():
    sql = (
        "select\n"
        '  -- lineage:allow-change reason="intended"\n'
        "  amount as amount_eur,\n"
        "  id\n"
    )
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert len(directives) == 1
    assert directives[0].column == "amount_eur"
    assert directives[0].scope == "column"


def test_model_scope_when_pragma_precedes_first_select():
    sql = '-- lineage:allow-change reason="whole model intended"\nselect id, amount from t\n'
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert len(directives) == 1
    assert directives[0].scope == "model"
    assert directives[0].column is None


def test_allow_break_verb_parsed():
    sql = 'select\n  -- lineage:allow-break column=old_id reason="test yml updated in follow-up"\n  new_id\n'
    directives, _ = parse_override_directives(sql)
    assert directives[0].verb is OverrideVerb.ALLOW_BREAK
    assert directives[0].verb.is_hard is True
    assert directives[0].column == "old_id"


def test_missing_reason_is_dropped_with_warning():
    sql = "select\n  -- lineage:allow-change column=amount\n  amount\n"
    directives, warnings = parse_override_directives(sql)
    assert directives == []
    assert len(warnings) == 1
    assert "no non-empty reason" in warnings[0]


def test_empty_reason_is_dropped_with_warning():
    sql = 'select\n  -- lineage:allow-change reason="" column=amount\n  amount\n'
    directives, warnings = parse_override_directives(sql)
    assert directives == []
    assert len(warnings) == 1


def test_unknown_verb_is_dropped_with_warning():
    sql = 'select\n  -- lineage:allow-foo reason="x"\n  amount\n'
    directives, warnings = parse_override_directives(sql)
    assert directives == []
    assert len(warnings) == 1
    assert "unknown override verb" in warnings[0]


def test_reason_containing_column_token_is_not_misparsed():
    # A `column=` INSIDE the quoted reason must not be scanned out as the target column.
    sql = (
        "select\n"
        '  -- lineage:allow-change reason="renamed; see the column=x note in the ticket"\n'
        "  amount as amount_eur\n"
    )
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert len(directives) == 1
    # No explicit column= arg outside the reason -> resolves by adjacency to amount_eur.
    assert directives[0].column == "amount_eur"
    assert "column=x note" in directives[0].reason


def test_single_quoted_reason_accepted():
    sql = "select\n  -- lineage:allow-change column=amount reason='intended change'\n  amount\n"
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert directives[0].reason == "intended change"


def test_source_line_is_one_indexed():
    sql = 'select\n\n  -- lineage:allow-change column=x reason="r"\n  x\n'
    directives, _ = parse_override_directives(sql)
    assert directives[0].source_line == 3


def test_adjacency_unresolved_leaves_column_none():
    # Pragma is the last meaningful line -> no adjacent code line -> None (caller marks stale).
    sql = 'select id from t\n-- lineage:allow-change reason="dangling"\n'
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert len(directives) == 1
    assert directives[0].column is None
    assert directives[0].scope == "column"


def test_extract_select_alias_variants():
    assert _extract_select_alias("  amount as amount_eur,") == "amount_eur"
    assert _extract_select_alias("  sum(x) AS total") == "total"
    assert _extract_select_alias("  amount_eur,") == "amount_eur"
    assert _extract_select_alias("  t.amount_eur") == "amount_eur"
    assert _extract_select_alias('  "Amount",') == "amount"
    assert _extract_select_alias("  ") is None


def test_multiple_pragmas_and_case_insensitive_verb():
    sql = (
        "select\n"
        '  -- LINEAGE:ALLOW-CHANGE column=a reason="one"\n'
        "  a,\n"
        '  -- lineage:allow-break column=b reason="two"\n'
        "  b\n"
    )
    directives, warnings = parse_override_directives(sql)
    assert warnings == []
    assert {d.column for d in directives} == {"a", "b"}
    assert {d.verb for d in directives} == {OverrideVerb.ALLOW_CHANGE, OverrideVerb.ALLOW_BREAK}
