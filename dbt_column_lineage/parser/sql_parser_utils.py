import re
from sqlglot import exp
from typing import Dict, List, Optional, Any


def strip_sql_comments(text: str) -> str:
    """Remove SQL comments from a string.

    Removes both /* ... */ and -- style comments.
    Normalizes whitespace (multiple spaces become single space).
    """
    if not text:
        return text

    # Remove /* ... */ style comments
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

    # Remove -- style comments (everything after -- until end of line)
    text = re.sub(r"--.*?$", "", text, flags=re.MULTILINE)

    # Normalize whitespace (multiple spaces/tabs/newlines become single space)
    text = re.sub(r"\s+", " ", text)

    # Clean up any extra whitespace that might be left
    return text.strip()


def get_table_aliases(parsed: Any) -> Dict[str, str]:
    aliases = {}
    for table in parsed.find_all((exp.Table, exp.From, exp.Join)):
        if table.alias:
            aliases[table.alias] = table.name
    return aliases


def get_table_context(select: Any) -> str:
    from_clause = select.find(exp.From)
    if from_clause:
        table = from_clause.find(exp.Table)
        if table:
            return str(table.name)

        subquery = from_clause.find(exp.Subquery)
        if subquery:
            subquery_select = subquery.find(exp.Select)
            if subquery_select:
                return get_table_context(subquery_select)
    return ""


def get_all_tables_from_select(select: Any) -> List[str]:
    tables = []
    from_clause = select.find(exp.From)
    if from_clause:
        table = from_clause.find(exp.Table)
        if table:
            tables.append(str(table.name))

    for join in select.find_all(exp.Join):
        if hasattr(join, "this"):
            join_table = join.this
            if isinstance(join_table, exp.Table):
                tables.append(str(join_table.name))
            elif hasattr(join_table, "name"):
                tables.append(str(join_table.name))

    return tables


def get_final_select(parsed: Any) -> Optional[Any]:
    query = parsed
    while hasattr(query, "this") and query.this:
        query = query.this

    if isinstance(query, exp.Select):
        return query

    if isinstance(query, exp.Query):
        return query.this if isinstance(query.this, exp.Select) else None

    return None


def split_qualified_name(qualified_name: str) -> tuple[str, str]:
    """Split a qualified name into table and column parts, stripping SQL comments."""
    if "." not in qualified_name:
        return ("", strip_sql_comments(qualified_name))
    qualified_name = strip_sql_comments(qualified_name)
    parts = qualified_name.split(".")
    table_part = ".".join(parts[:-1])
    column_part = strip_sql_comments(parts[-1])
    return (table_part, column_part)
