import re
import logging
from sqlglot import parse_one, exp
from typing import Dict, List, Set, Optional, Any
from dbt_column_lineage.models.schema import ColumnLineage, SQLParseResult

logging.getLogger('sqlglot').setLevel(logging.ERROR)


class SQLColumnParser:
    def __init__(self, dialect: Optional[str] = None):
        """
        Initialize the parser with an optional SQL dialect.

        Args:
            dialect: The SQL dialect to use (e.g., 'snowflake', 'bigquery', 'postgres'). 
        """
        self.dialect = dialect

    def parse_column_lineage(self, sql: str) -> SQLParseResult:
        """Parse SQL to extract column-level lineage using sqlglot."""
        cte_to_model = self._extract_cte_model_mappings(sql)
        parsed = parse_one(sql, dialect=self.dialect)
        
        aliases = self._get_table_aliases(parsed)
        cte_sources = self._build_cte_sources(parsed)
        
        columns = {}
        star_sources = set()
        
        for select in parsed.find_all(exp.Select):
            table_context = self._get_table_context(select)
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    source_table = table_context
                    visited_ctes = set()
                    
                    # Resolve through CTEs to get to the base table
                    while source_table in cte_to_model and source_table not in visited_ctes:
                        visited_ctes.add(source_table)
                        next_table = cte_to_model[source_table]
                        # If the CTE maps to itself or maps to its own name, it's a model reference
                        if next_table == source_table or next_table == source_table.split('.')[-1]:
                            star_sources.add(source_table)
                            break
                        source_table = next_table
                    else:
                        if source_table not in cte_sources:
                            star_sources.add(source_table)
                    continue
                
                target_col = expr.alias_or_name.lower()
                lineage = self._analyze_expression(expr, aliases, table_context, cte_sources, cte_to_model)
                columns[target_col] = lineage
        
        return SQLParseResult(
            column_lineage=columns,
            star_sources=star_sources
        )
    
    def _extract_cte_model_mappings(self, sql: str) -> Dict[str, str]:
        """Extract mappings from CTE names to model names."""
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
    
    def _get_table_aliases(self, parsed: Any) -> Dict[str, str]:
        """Build mapping of alias -> real table name."""
        aliases = {}
        for table in parsed.find_all((exp.Table, exp.From, exp.Join)):
            if table.alias:
                aliases[table.alias] = table.name
        return aliases
    
    def _get_table_context(self, select: Any) -> str:
        """Get the main table being selected from."""
        from_clause = select.find(exp.From)
        if from_clause:
            table = from_clause.find(exp.Table)
            if table:
                return str(table.name)
        return ""
    
    def _normalize_table_ref(self, column: str, aliases: Dict[str, str], table_context: str) -> str:
        """Convert aliased table references to actual table names."""
        if '.' not in column:
            return f"{table_context}.{column}" if table_context else column
        table, col = column.split('.')
        return f"{aliases.get(table, table)}.{col}"
    
    def _build_cte_sources(self, parsed: Any) -> Dict[str, Dict[str, str]]:
        """Build mapping of CTE columns to their original sources."""
        cte_sources: Dict[str, Dict[str, str]] = {}
        
        # Process CTEs in order to build up dependencies
        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            cte_sources[cte_name] = {}
            
            select = cte.this.find(exp.Select)
            if select:
                table_context = self._get_table_context(select)
                
                for expr in select.expressions:
                    col_name = expr.alias_or_name
                    source_cols = self._extract_source_columns(expr, {}, table_context, cte_sources, None)
                    if source_cols:
                        cte_sources[cte_name][col_name] = next(iter(source_cols))
                    elif isinstance(expr, exp.Star):
                        from_table = self._get_table_context(select)
                        if from_table in cte_sources:
                            cte_sources[cte_name].update(cte_sources[from_table])
                        
        return cte_sources
    
    def _resolve_column_source(self, column: str, table: str, cte_sources: Dict[str, Dict[str, str]], 
                              cte_to_model: Optional[Dict[str, str]] = None) -> str:
        """Resolve a column reference to its original source through CTEs."""
        if '.' not in column:
            col_name = column
        else:
            table, col_name = column.split('.')
        if table in cte_sources and col_name in cte_sources[table]:
            return cte_sources[table][col_name]
        elif table and cte_to_model and table in cte_to_model:
            return f"{cte_to_model[table]}.{col_name}"
        elif table:
            return f"{table}.{col_name}"
        return column
    
    def _analyze_expression(self, expr: Any, aliases: Dict[str, str], table_context: str, 
                          cte_sources: Dict[str, Dict[str, str]], cte_to_model: Optional[Dict[str, str]] = None, 
                          is_aliased: bool = False) -> List[ColumnLineage]:
        """Analyze expression to determine column lineage."""
        if isinstance(expr, exp.Alias):
            return self._analyze_expression(expr.this, aliases, table_context, cte_sources, cte_to_model, is_aliased=True)
            
        if isinstance(expr, exp.Column):
            source_col = self._normalize_table_ref(str(expr), aliases, table_context)
            table, col = source_col.split('.') if '.' in source_col else (table_context, source_col)
            resolved_source = self._resolve_column_source(source_col, table, cte_sources, cte_to_model)
            
            # Normalize column names to lowercase
            if '.' in resolved_source:
                table_part, col_part = resolved_source.split('.')
                resolved_source = f"{table_part}.{col_part.lower()}"
            
            return [ColumnLineage(
                source_columns={resolved_source},
                transformation_type="renamed" if is_aliased else "direct"
            )]
            
        else:
            source_cols = self._extract_source_columns(expr, aliases, table_context, cte_sources, cte_to_model)
            normalized_source_cols = {
                s if '.' not in s else f"{s.split('.')[0]}.{s.split('.')[1].lower()}"
                for s in source_cols
            }
            return [ColumnLineage(
                source_columns=normalized_source_cols,
                transformation_type="derived",
                sql_expression=str(expr)
            )]
    
    def _extract_source_columns(self, expr: Any, aliases: Dict[str, str], table_context: str, 
                              cte_sources: Dict[str, Dict[str, str]], cte_to_model: Optional[Dict[str, str]] = None) -> Set[str]:
        """Extract all source column references from an expression."""
        columns = set()
        for col in expr.find_all(exp.Column):
            source_col = self._normalize_table_ref(str(col), aliases, table_context)
            table, _ = source_col.split('.') if '.' in source_col else (table_context, source_col)
            resolved = self._resolve_column_source(source_col, table, cte_sources, cte_to_model)
            columns.add(resolved)
        return columns 