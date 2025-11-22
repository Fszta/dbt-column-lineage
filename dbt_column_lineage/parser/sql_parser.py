import re
import logging
from dataclasses import dataclass, field
from sqlglot import parse_one, exp
from typing import Dict, List, Set, Optional, Any, Callable
from dbt_column_lineage.models.schema import ColumnLineage, SQLParseResult
from dbt_column_lineage.parser.sql_parser_utils import (
    get_table_aliases,
    get_table_context,
    get_all_tables_from_select,
    get_final_select,
    split_qualified_name,
)

logging.getLogger("sqlglot").setLevel(logging.ERROR)


@dataclass
class ParserContext:
    """Context object containing parser state and dependencies."""

    aliases: Dict[str, str]
    table_context: str
    cte_sources: Dict[str, Dict[str, str]]
    cte_to_model: Optional[Dict[str, str]]
    cte_transformation_types: Dict[str, Dict[str, str]] = field(default_factory=dict)
    cte_sql_expressions: Dict[str, Dict[str, Optional[str]]] = field(
        default_factory=dict
    )
    cte_base_tables: Dict[str, Set[str]] = field(default_factory=dict)
    column_definitions: Optional[Dict[str, Any]] = None


class CTEHandler:
    def extract_cte_model_mappings_from_parsed(self, parsed: Any) -> Dict[str, str]:
        mappings = {}
        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            select = cte.this.find(exp.Select)
            if select:
                base_table = get_table_context(select)
                if base_table:
                    mappings[cte_name] = base_table
        return mappings

    def trace_base_tables(
        self,
        table: str,
        cte_to_model: Optional[Dict[str, str]],
        cte_sources: Dict[str, Dict[str, str]],
        star_sources: Set[str],
    ) -> None:
        if cte_to_model is None:
            if table not in cte_sources:
                star_sources.add(table)
            return

        trace_table = table
        trace_visited = set()

        while trace_table in cte_to_model and trace_table not in trace_visited:
            trace_visited.add(trace_table)
            next_table = cte_to_model[trace_table]
            if next_table == trace_table or next_table == trace_table.split(".")[-1]:
                star_sources.add(trace_table)
                break
            trace_table = next_table
        else:
            if trace_table not in cte_sources:
                star_sources.add(trace_table)


class StarExpressionHandler:
    def __init__(self):
        self._cte_handler: Optional[CTEHandler] = None

    def is_star_expression(self, expr: Any) -> bool:
        return isinstance(expr, exp.Star) or (
            isinstance(expr, exp.Column) and getattr(expr, "is_star", False)
        )

    def get_star_source_table(
        self, expr: Any, aliases: Dict[str, str], table_context: str
    ) -> str:
        if isinstance(expr, exp.Column) and expr.table:
            star_table_alias = str(expr.table)
            return aliases.get(star_table_alias, star_table_alias)
        else:
            return table_context

    def get_excluded_columns(self, star_expr: exp.Star) -> List[str]:
        excluded = []
        if hasattr(star_expr, "args") and "except" in star_expr.args:
            except_clause = star_expr.args["except"]
            if except_clause:
                for col_expr in except_clause:
                    if isinstance(col_expr, exp.Column):
                        col_name = (
                            str(col_expr.this)
                            if hasattr(col_expr, "this")
                            else str(col_expr)
                        )
                        excluded.append(col_name)
        return excluded

    def get_cte_transformation_info(
        self, context: ParserContext, cte_name: str, col_name: str
    ) -> tuple[str, Optional[str]]:
        trans_type = context.cte_transformation_types.get(cte_name, {}).get(
            col_name, "direct"
        )
        sql_expr = context.cte_sql_expressions.get(cte_name, {}).get(col_name)
        return trans_type, sql_expr

    def expand_from_join_tables(
        self,
        select: Any,
        all_tables: List[str],
        excluded_col_names: Set[str],
        context: ParserContext,
        columns: Dict[str, List[ColumnLineage]],
        star_sources: Set[str],
    ) -> None:
        for join_table in all_tables:
            if join_table in context.cte_sources:
                if len(context.cte_sources[join_table]) > 0:
                    for col_name, col_source in context.cte_sources[join_table].items():
                        if col_name.lower() not in excluded_col_names:
                            trans_type, sql_expr = self.get_cte_transformation_info(
                                context, join_table, col_name
                            )
                            columns[col_name.lower()] = [
                                ColumnLineage(
                                    source_columns={col_source},
                                    transformation_type=trans_type,
                                    sql_expression=sql_expr,
                                )
                            ]
                if join_table in context.cte_base_tables:
                    star_sources.update(context.cte_base_tables[join_table])
                self._cte_handler.trace_base_tables(
                    join_table, context.cte_to_model, context.cte_sources, star_sources
                )
            elif context.cte_to_model and join_table in context.cte_to_model:
                star_sources.add(context.cte_to_model[join_table])

    def expand_from_cte(
        self,
        source_table: str,
        excluded_col_names: Set[str],
        context: ParserContext,
        columns: Dict[str, List[ColumnLineage]],
        star_sources: Set[str],
    ) -> bool:
        if source_table in context.cte_sources:
            if len(context.cte_sources[source_table]) > 0:
                for col_name, col_source in context.cte_sources[source_table].items():
                    if col_name.lower() not in excluded_col_names:
                        trans_type, sql_expr = self.get_cte_transformation_info(
                            context, source_table, col_name
                        )
                        columns[col_name.lower()] = [
                            ColumnLineage(
                                source_columns={col_source},
                                transformation_type=trans_type,
                                sql_expression=sql_expr,
                            )
                        ]

            if source_table in context.cte_base_tables:
                star_sources.update(context.cte_base_tables[source_table])

            self._cte_handler.trace_base_tables(
                source_table, context.cte_to_model, context.cte_sources, star_sources
            )
            return True
        return False


class ExpressionAnalyzer:
    def __init__(self, parser: "SQLColumnParser"):
        self.parser = parser
        self._handlers: Dict[type, Callable] = {}
        self._register_default_handlers()

    def _register_default_handlers(self):
        self.register_handler(exp.Alias, self._handle_alias)
        self.register_handler(exp.Column, self._handle_column)

    def register_handler(self, expr_type: type, handler: Callable):
        self._handlers[expr_type] = handler

    def analyze(
        self, expr: Any, context: ParserContext, is_aliased: bool = False
    ) -> List[ColumnLineage]:
        expr_type = type(expr)
        if expr_type in self._handlers:
            return self._handlers[expr_type](expr, context, is_aliased)
        return self._default_handler(expr, context)

    def _handle_alias(
        self, expr: exp.Alias, context: ParserContext, is_aliased: bool
    ) -> List[ColumnLineage]:
        return self.analyze(expr.this, context, is_aliased=True)

    def _handle_column(
        self, expr: exp.Column, context: ParserContext, is_aliased: bool
    ) -> List[ColumnLineage]:
        col_name = (
            str(expr.this).lower()
            if hasattr(expr, "this") and expr.this
            else str(expr).lower()
        )

        forward_result = self.parser._handle_forward_reference(expr, col_name, context)
        if forward_result is not None:
            return forward_result

        return self.parser._analyze_column_reference(
            expr, col_name, context, is_aliased
        )

    def _default_handler(
        self, expr: Any, context: ParserContext
    ) -> List[ColumnLineage]:
        source_cols = self.parser._extract_source_columns(expr, context)
        normalized_source_cols = self.parser._normalize_source_columns(source_cols)
        return [
            ColumnLineage(
                source_columns=normalized_source_cols,
                transformation_type="derived",
                sql_expression=str(expr),
            )
        ]


class SQLColumnParser:
    def __init__(self, dialect: Optional[str] = None):
        self.dialect = dialect
        self._cte_handler = CTEHandler()
        self._star_handler = StarExpressionHandler()
        self._star_handler._cte_handler = self._cte_handler
        self._expression_analyzer = ExpressionAnalyzer(self)

    def parse_column_lineage(self, sql: str) -> SQLParseResult:
        parsed = parse_one(sql, dialect=self.dialect)
        cte_to_model = self._cte_handler.extract_cte_model_mappings_from_parsed(parsed)

        cte_transformation_types: Dict[str, Dict[str, str]] = {}
        cte_sql_expressions: Dict[str, Dict[str, Optional[str]]] = {}
        cte_base_tables: Dict[str, Set[str]] = {}

        aliases = get_table_aliases(parsed)
        for cte in parsed.find_all(exp.CTE):
            cte_base_tables[cte.alias] = set()

        cte_sources = self._build_cte_sources(
            parsed,
            cte_to_model,
            cte_transformation_types,
            cte_sql_expressions,
            cte_base_tables,
        )

        columns = {}
        star_sources = set()

        final_select = get_final_select(parsed)
        if not final_select:
            selects_to_process = parsed.find_all(exp.Select)
        else:
            selects_to_process = [final_select]

        for select in selects_to_process:
            table_context = get_table_context(select)

            column_definitions = {}
            for expr in select.expressions:
                col_name = expr.alias_or_name.lower()
                column_definitions[col_name] = expr

            context = ParserContext(
                aliases=aliases,
                table_context=table_context,
                cte_sources=cte_sources,
                cte_to_model=cte_to_model,
                cte_transformation_types=cte_transformation_types,
                cte_sql_expressions=cte_sql_expressions,
                cte_base_tables=cte_base_tables,
                column_definitions=column_definitions,
            )

            for expr in select.expressions:
                if self._star_handler.is_star_expression(expr):
                    excluded_columns = (
                        self._star_handler.get_excluded_columns(expr)
                        if isinstance(expr, exp.Star)
                        else []
                    )
                    excluded_col_names = {col.lower() for col in excluded_columns}
                    source_table = self._star_handler.get_star_source_table(
                        expr, context.aliases, context.table_context
                    )

                    all_tables = get_all_tables_from_select(select)
                    if len(all_tables) > 1 and not isinstance(expr, exp.Column):
                        self._star_handler.expand_from_join_tables(
                            select,
                            all_tables,
                            excluded_col_names,
                            context,
                            columns,
                            star_sources,
                        )
                        continue

                    if self._star_handler.expand_from_cte(
                        source_table,
                        excluded_col_names,
                        context,
                        columns,
                        star_sources,
                    ):
                        continue

                    if context.cte_to_model and source_table in context.cte_to_model:
                        base_table = context.cte_to_model[source_table]
                        star_sources.add(base_table)
                        continue

                    self._cte_handler.trace_base_tables(
                        source_table,
                        context.cte_to_model,
                        context.cte_sources,
                        star_sources,
                    )
                    continue

                target_col = expr.alias_or_name.lower()
                lineage = self._expression_analyzer.analyze(expr, context)
                columns[target_col] = lineage

        return SQLParseResult(column_lineage=columns, star_sources=star_sources)

    def _extract_cte_model_mappings(self, sql: str) -> Dict[str, str]:
        """Extract mappings from CTE names to model names (legacy method using regex)."""
        mappings = {}
        # Pattern to handle:
        # - SQLite: from main."stg_transactions"
        # - DuckDB: from "test"."main"."stg_transactions"
        # - Snowflake: from test.main.stg_transactions
        pattern = r'(\w+)\s+as\s*\(\s*select\b.*?\bfrom\s+(["\w\.]+(?:\."[^"]+"|[^"\s]+))\s*\)'
        matches = re.findall(pattern, sql, re.IGNORECASE | re.DOTALL)

        for cte_name, full_table_ref in matches:
            parts = re.findall(r'"([^"]+)"|([^"\s\.]+)', full_table_ref)
            model_name = next(name for pair in reversed(parts) for name in pair if name)
            mappings[cte_name] = model_name

        return mappings

    def _normalize_table_ref(
        self, column: str, aliases: Dict[str, str], table_context: str
    ) -> str:
        table_part, col = split_qualified_name(column)
        if not table_part:
            return f"{table_context}.{col}" if table_context else col
        table = aliases.get(table_part, table_part)
        return f"{table}.{col}"

    def _build_cte_sources(
        self,
        parsed: Any,
        cte_to_model: Optional[Dict[str, str]],
        cte_transformation_types: Dict[str, Dict[str, str]],
        cte_sql_expressions: Dict[str, Dict[str, Optional[str]]],
        cte_base_tables: Dict[str, Set[str]],
    ) -> Dict[str, Dict[str, str]]:
        cte_sources: Dict[str, Dict[str, str]] = {}

        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            cte_sources[cte_name] = {}
            cte_transformation_types[cte_name] = {}
            cte_sql_expressions[cte_name] = {}

            select = cte.this.find(exp.Select)
            if select:
                table_context = get_table_context(select)
                aliases = get_table_aliases(select)

                column_definitions = {}
                for expr in select.expressions:
                    col_name = expr.alias_or_name
                    column_definitions[col_name.lower()] = expr

                context = ParserContext(
                    aliases=aliases,
                    table_context=table_context,
                    cte_sources=cte_sources,
                    cte_to_model=cte_to_model,
                    cte_transformation_types=cte_transformation_types,
                    cte_sql_expressions=cte_sql_expressions,
                    cte_base_tables=cte_base_tables,
                    column_definitions=column_definitions,
                )

                for expr in select.expressions:
                    col_name = expr.alias_or_name

                    if self._star_handler.is_star_expression(expr):
                        from_table = self._resolve_star_from_table_in_cte(
                            expr, select, context.aliases, context.table_context
                        )
                        excluded_columns = (
                            self._star_handler.get_excluded_columns(expr)
                            if isinstance(expr, exp.Star)
                            else []
                        )
                        excluded_col_names = {col.lower() for col in excluded_columns}

                        if from_table in cte_sources:
                            self._copy_cte_columns_with_exclusions(
                                from_table,
                                cte_name,
                                excluded_col_names,
                                context,
                            )
                        elif cte_to_model and from_table in cte_to_model:
                            base_table = cte_to_model[from_table]
                            cte_base_tables[cte_name].add(base_table)
                        else:
                            if from_table not in cte_sources:
                                cte_base_tables[cte_name].add(from_table)
                    else:
                        lineage_list = self._expression_analyzer.analyze(expr, context)
                        if lineage_list:
                            lineage = lineage_list[0]
                            self._store_column_lineage_in_cte(
                                cte_name,
                                col_name,
                                lineage,
                                context,
                            )

        return cte_sources

    def _resolve_star_from_table_in_cte(
        self,
        expr: Any,
        select: Any,
        aliases: Dict[str, str],
        table_context: str,
    ) -> str:
        if isinstance(expr, exp.Column) and expr.table:
            star_table_alias = str(expr.table)
            from_table = aliases.get(star_table_alias, star_table_alias)
            if from_table == star_table_alias:
                for join in select.find_all(exp.Join):
                    if join.alias and join.alias == star_table_alias:
                        from_table = (
                            str(join.this) if hasattr(join, "this") else from_table
                        )
                        break
                from_clause = select.find(exp.From)
                if from_clause:
                    table_expr = from_clause.find(exp.Table)
                    if table_expr and table_expr.alias == star_table_alias:
                        from_table = (
                            str(table_expr.name)
                            if hasattr(table_expr, "name")
                            else from_table
                        )
            return from_table
        else:
            return table_context

    def _copy_cte_columns_with_exclusions(
        self,
        from_table: str,
        cte_name: str,
        excluded_col_names: Set[str],
        context: ParserContext,
    ) -> None:
        if from_table in context.cte_sources:
            for src_col_name, src_col_source in context.cte_sources[from_table].items():
                if src_col_name.lower() not in excluded_col_names:
                    context.cte_sources[cte_name][src_col_name] = src_col_source
                    if from_table in context.cte_transformation_types:
                        context.cte_transformation_types[cte_name][src_col_name] = (
                            context.cte_transformation_types[from_table].get(
                                src_col_name, "direct"
                            )
                        )
                    else:
                        context.cte_transformation_types[cte_name][
                            src_col_name
                        ] = "direct"
                    if from_table in context.cte_sql_expressions:
                        context.cte_sql_expressions[cte_name][src_col_name] = (
                            context.cte_sql_expressions[from_table].get(src_col_name)
                        )
                    else:
                        context.cte_sql_expressions[cte_name][src_col_name] = None
            if from_table in context.cte_base_tables:
                context.cte_base_tables[cte_name].update(
                    context.cte_base_tables[from_table]
                )

    def _store_column_lineage_in_cte(
        self,
        cte_name: str,
        col_name: str,
        lineage: ColumnLineage,
        context: ParserContext,
    ) -> None:
        if lineage.source_columns:
            context.cte_sources[cte_name][col_name] = next(iter(lineage.source_columns))
        else:
            if context.table_context:
                context.cte_sources[cte_name][col_name] = f"{context.table_context}.*"
            else:
                context.cte_sources[cte_name][col_name] = col_name
        context.cte_transformation_types[cte_name][
            col_name
        ] = lineage.transformation_type
        context.cte_sql_expressions[cte_name][col_name] = lineage.sql_expression

    def _resolve_column_source(
        self,
        column: str,
        table: str,
        cte_sources: Dict[str, Dict[str, str]],
        cte_to_model: Optional[Dict[str, str]] = None,
    ) -> str:
        table_part, col_name = split_qualified_name(column)
        if table_part:
            table = table_part

        if table in cte_sources and col_name in cte_sources[table]:
            return cte_sources[table][col_name]
        elif table and cte_to_model and table in cte_to_model:
            return f"{cte_to_model[table]}.{col_name}"
        elif table:
            return f"{table}.{col_name}"
        return column

    def _handle_forward_reference(
        self,
        expr: exp.Column,
        col_name: str,
        context: ParserContext,
    ) -> Optional[List[ColumnLineage]]:
        is_qualified = bool(expr.table)
        if (
            not is_qualified
            and context.column_definitions
            and col_name in context.column_definitions
        ):
            forward_expr = context.column_definitions[col_name]
            if forward_expr != expr:
                forward_sources = self._extract_source_columns(
                    forward_expr,
                    context,
                    visited_forward_refs={col_name},
                )
                return [
                    ColumnLineage(
                        source_columns=forward_sources,
                        transformation_type="derived",
                        sql_expression=str(expr),
                    )
                ]
        return None

    def _analyze_column_reference(
        self,
        expr: exp.Column,
        col_name: str,
        context: ParserContext,
        is_aliased: bool,
    ) -> List[ColumnLineage]:
        source_col = self._normalize_table_ref(
            str(expr), context.aliases, context.table_context
        )
        table_part, col = split_qualified_name(source_col)
        table = table_part if table_part else context.table_context
        resolved_source = self._resolve_column_source(
            source_col, table, context.cte_sources, context.cte_to_model
        )

        trans_type = "direct"
        sql_expr = None
        if table in context.cte_sources and col_name in context.cte_sources[table]:
            trans_type = context.cte_transformation_types.get(table, {}).get(
                col_name, "direct"
            )
            sql_expr = context.cte_sql_expressions.get(table, {}).get(col_name)
        elif is_aliased:
            trans_type = "renamed"

        resolved_table, resolved_col = split_qualified_name(resolved_source)
        if resolved_table:
            resolved_source = f"{resolved_table}.{resolved_col.lower()}"
        elif resolved_col:
            resolved_source = resolved_col.lower()

        return [
            ColumnLineage(
                source_columns={resolved_source},
                transformation_type=trans_type,
                sql_expression=sql_expr,
            )
        ]

    def _normalize_source_columns(self, source_cols: Set[str]) -> Set[str]:
        normalized = set()
        for s in source_cols:
            table_part, col_part = split_qualified_name(s)
            if table_part:
                normalized.add(f"{table_part}.{col_part.lower()}")
            else:
                normalized.add(s)
        return normalized

    def _handle_forward_reference_in_extraction(
        self,
        col: exp.Column,
        col_name: str,
        context: ParserContext,
        visited_forward_refs: Set[str],
    ) -> Optional[Set[str]]:
        is_qualified = bool(col.table)
        if (
            not is_qualified
            and context.column_definitions
            and col_name in context.column_definitions
            and col_name not in visited_forward_refs
        ):
            forward_expr = context.column_definitions[col_name]
            if forward_expr != col:
                visited_forward_refs.add(col_name)
                forward_cols = self._extract_source_columns(
                    forward_expr,
                    context,
                    visited_forward_refs,
                )
                visited_forward_refs.remove(col_name)
                return forward_cols
        return None

    def _extract_source_columns(
        self,
        expr: Any,
        context: ParserContext,
        visited_forward_refs: Optional[Set[str]] = None,
    ) -> Set[str]:
        if visited_forward_refs is None:
            visited_forward_refs = set()

        columns = set()
        for col in expr.find_all(exp.Column):
            col_name = (
                str(col.this).lower()
                if hasattr(col, "this") and col.this
                else str(col).lower()
            )

            forward_cols = self._handle_forward_reference_in_extraction(
                col,
                col_name,
                context,
                visited_forward_refs,
            )
            if forward_cols is not None:
                columns.update(forward_cols)
                continue

            source_col = self._normalize_table_ref(
                str(col), context.aliases, context.table_context
            )
            table_part, _ = split_qualified_name(source_col)
            table = table_part if table_part else context.table_context
            resolved = self._resolve_column_source(
                source_col, table, context.cte_sources, context.cte_to_model
            )
            columns.add(resolved)
        return columns
