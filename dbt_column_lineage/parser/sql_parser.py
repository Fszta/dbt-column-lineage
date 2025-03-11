import re
from sqlglot import parse_one, exp
from typing import Dict, List, Set, Optional
from dbt_column_lineage.models.schema import ColumnLineage

class SQLColumnParser:
    def parse_column_lineage(self, sql: str) -> Dict[str, List[ColumnLineage]]:
        """Parse SQL to extract column-level lineage using sqlglot."""
        cte_to_model = self._extract_cte_model_mappings(sql)
        parsed = parse_one(sql)
        
        aliases = self._get_table_aliases(parsed)
        cte_sources = self._build_cte_sources(parsed)
        
        columns = {}
        for select in parsed.find_all(exp.Select):
            table_context = self._get_table_context(select)
            
            for expr in select.expressions:
                target_col = expr.alias_or_name
                lineage = self._analyze_expression(expr, aliases, table_context, cte_sources, cte_to_model)
                columns[target_col] = lineage
                
        return columns
    
    def _extract_cte_model_mappings(self, sql: str) -> Dict[str, str]:
        """Extract mappings from CTE names to model names."""
        mappings = {}
        pattern = r'(\w+)\s+as\s*\(\s*select\b.*?\bfrom\s*\w+\."([^"]+)"\s*\)'
        matches = re.findall(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        for cte_name, model_name in matches:
            mappings[cte_name] = model_name
            
        return mappings
    
    def _get_table_aliases(self, parsed) -> Dict[str, str]:
        """Build mapping of alias -> real table name."""
        aliases = {}
        for table in parsed.find_all((exp.Table, exp.From, exp.Join)):
            if table.alias:
                aliases[table.alias] = table.name
        return aliases
    
    def _get_table_context(self, select) -> str:
        """Get the main table being selected from."""
        from_clause = select.find(exp.From)
        if from_clause:
            table = from_clause.find(exp.Table)
            if table:
                return table.name
        return ""
    
    def _normalize_table_ref(self, column: str, aliases: Dict[str, str], table_context: str) -> str:
        """Convert aliased table references to actual table names."""
        if '.' not in column:
            return f"{table_context}.{column}" if table_context else column
        table, col = column.split('.')
        return f"{aliases.get(table, table)}.{col}"
    
    def _build_cte_sources(self, parsed) -> Dict[str, Dict[str, str]]:
        """Build mapping of CTE columns to their original sources."""
        cte_sources = {}
        
        # Process CTEs in order to build up dependencies
        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            cte_sources[cte_name] = {}
            
            # Get table context for this CTE
            select = cte.this.find(exp.Select)
            if select:
                table_context = self._get_table_context(select)
                
                # Process each column in the CTE
                for expr in select.expressions:
                    col_name = expr.alias_or_name
                    source_cols = self._extract_source_columns(expr, {}, table_context, cte_sources, None)
                    if source_cols:
                        cte_sources[cte_name][col_name] = next(iter(source_cols))
                    elif isinstance(expr, exp.Star):
                        # Handle SELECT * by copying sources from referenced table/CTE
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
            
        # Check if this is a CTE reference
        if table in cte_sources and col_name in cte_sources[table]:
            return cte_sources[table][col_name]
        elif table and cte_to_model and table in cte_to_model:
            # Map CTE to model name
            return f"{cte_to_model[table]}.{col_name}"
        elif table:
            return f"{table}.{col_name}"
        return column
    
    def _analyze_expression(self, expr, aliases: Dict[str, str], table_context: str, 
                          cte_sources: Dict[str, Dict[str, str]], cte_to_model: Optional[Dict[str, str]] = None, 
                          is_aliased: bool = False) -> List[ColumnLineage]:
        """Analyze expression to determine column lineage."""
        if isinstance(expr, exp.Alias):
            return self._analyze_expression(expr.this, aliases, table_context, cte_sources, cte_to_model, is_aliased=True)
            
        if isinstance(expr, exp.Column):
            source_col = self._normalize_table_ref(str(expr), aliases, table_context)
            table, col = source_col.split('.') if '.' in source_col else (table_context, source_col)
            resolved_source = self._resolve_column_source(source_col, table, cte_sources, cte_to_model)
            
            return [ColumnLineage(
                source_columns={resolved_source},
                transformation_type="renamed" if is_aliased else "direct"
            )]
            
        else:
            # Any other expression is a derivation
            source_cols = self._extract_source_columns(expr, aliases, table_context, cte_sources, cte_to_model)
            return [ColumnLineage(
                source_columns=source_cols,
                transformation_type="derived",
                sql_expression=str(expr)
            )]
    
    def _extract_source_columns(self, expr, aliases: Dict[str, str], table_context: str, 
                              cte_sources: Dict[str, Dict[str, str]], cte_to_model: Optional[Dict[str, str]] = None) -> Set[str]:
        """Extract all source column references from an expression."""
        columns = set()
        for col in expr.find_all(exp.Column):
            source_col = self._normalize_table_ref(str(col), aliases, table_context)
            table, _ = source_col.split('.') if '.' in source_col else (table_context, source_col)
            resolved = self._resolve_column_source(source_col, table, cte_sources, cte_to_model)
            columns.add(resolved)
        return columns 