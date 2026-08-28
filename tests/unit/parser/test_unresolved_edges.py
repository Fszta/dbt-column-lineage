"""Parser detection — unresolved column edges.

The parser must DECLARE (via ``SQLParseResult.unresolved_edges``) the constructs where it cannot
genuinely resolve a column's upstream source, instead of fabricating a confident-but-wrong source
column. See ``UNRESOLVED_EDGES_PLAN.md`` §4a for the marker contract.

Two constructs are detectable from the SQL alone (registry-independent) and live here:
  * ``phantom_alias``  — a ``lateral flatten`` / table-function pseudo-alias (``p.value``);
  * ``pivot_output``   — a quoted pivot literal projected as a column (``"'name'"``);
  * ``star_rename``    — a ``select * rename(old as new)`` output the star expander drops today.

The third construct, ``unexpandable_star`` (``select *`` off a relation that is not a declared
upstream), needs the registry's declared-dependency set and is asserted in the registry tests.
"""

from parrant.parser import SQLColumnParser


def _reasons(result):
    return {edge.reason for edge in result.unresolved_edges}


def _by_column(result):
    out = {}
    for edge in result.unresolved_edges:
        out.setdefault(edge.column, set()).add(edge.reason)
    return out


def test_flatten_alias_emits_phantom_marker_not_source():
    """A column sourced from a ``lateral flatten`` alias must be declared, not fabricated."""
    sql = """
    with
    exploded as (
        select
            page_id,
            f.value:country::varchar as country
        from raw_pages,
            lateral flatten(properties) f
    )
    select page_id, country from exploded
    """
    result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)

    # The phantom flatten token `f.value` is gone from every resolved source set.
    all_sources = {
        token
        for lineages in result.column_lineage.values()
        for lineage in lineages
        for token in lineage.source_columns
    }
    assert not any(token.startswith("f.") for token in all_sources), all_sources

    # ...and `country` carries a phantom_alias marker instead.
    assert _by_column(result).get("country") == {"phantom_alias"}
    detail = next(e.detail for e in result.unresolved_edges if e.column == "country")
    assert detail.startswith("f.")
    # Parser leaves the model name empty (the registry stamps it).
    assert all(edge.model == "" for edge in result.unresolved_edges)


def test_pivot_literal_output_emits_pivot_marker():
    """Quoted pivot-literal source columns (``"'name'"``) become markers, not fabricated sources."""
    sql = """
    with
    to_pivot as (
        select squad_id, prop_name, prop_value from raw_props
    ),
    pivoted as (
        select
            squad_id,
            "'name'" as name,
            "'linear team id'" as linear_team_id
        from to_pivot
        pivot ( max(prop_value) for prop_name in ('name', 'linear team id') ) p
    )
    select squad_id, name, linear_team_id from pivoted
    """
    result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)

    by_col = _by_column(result)
    assert by_col.get("name") == {"pivot_output"}
    assert by_col.get("linear_team_id") == {"pivot_output"}

    # No quoted-literal token survives as a "resolved" source.
    all_sources = {
        token
        for lineages in result.column_lineage.values()
        for lineage in lineages
        for token in lineage.source_columns
    }
    assert not any("'" in token for token in all_sources), all_sources

    # squad_id resolves cleanly through the pivot and is NOT marked.
    assert "squad_id" not in by_col


def test_star_rename_output_is_declared():
    """``select * rename (old as new)`` outputs are unresolved today — declare them."""
    sql = "select * rename (old_col as new_col) from some_ref"
    result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)

    assert "star_rename" in _reasons(result)
    star_rename = [e for e in result.unresolved_edges if e.reason == "star_rename"]
    assert any(e.column == "new_col" for e in star_rename)


def test_plain_star_off_ref_has_no_false_positive_marker():
    """Regression: a normal ``select *`` off a ref/CTE must resolve cleanly with NO marker."""
    sql = """
    with
    renamed as (
        select id as customer_id, name as customer_name from stg_customers
    )
    select * from renamed
    """
    result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)

    assert result.unresolved_edges == []
    # And the genuine edges survive untouched.
    assert result.column_lineage["customer_id"][0].source_columns == {"stg_customers.id"}
    assert result.column_lineage["customer_name"][0].source_columns == {"stg_customers.name"}


def test_direct_column_references_have_no_marker():
    """Regression: plain qualified column references never produce a marker (fail-safe)."""
    sql = """
    select
        customers.id as customer_id,
        orders.amount as order_amount
    from customers
    join orders on orders.customer_id = customers.id
    """
    result = SQLColumnParser(dialect="snowflake").parse_column_lineage(sql)

    assert result.unresolved_edges == []
    assert result.column_lineage["customer_id"][0].source_columns == {"customers.id"}
    assert result.column_lineage["order_amount"][0].source_columns == {"orders.amount"}
