import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict
from dataclasses import dataclass

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.models.schema import ColumnLineage, Column


@dataclass
class LineageSelector:
    """Represents a parsed lineage selector."""
    model: str
    column: Optional[str]
    upstream: bool
    downstream: bool
    
    @classmethod
    def from_string(cls, selector: str) -> "LineageSelector":
        """
        Parse a selector string to determine model, column, and lineage direction.
        
        Format: [+]model_name.column_name[+]
        - + prefix: upstream lineage
        - + suffix: downstream lineage
        - no +: both directions
        
        Returns:
            LineageSelector object with parsed values
        """
        if not selector:
            raise ValueError("Selector cannot be empty")
        
        upstream = selector.startswith('+')
        downstream = selector.endswith('+')
        
        if not upstream and not downstream:
            upstream = True
            downstream = True
        
        clean_selector = selector.strip('+')
        
        if '.' in clean_selector:
            model_name, column_name = clean_selector.split('.', 1)
        else:
            model_name = clean_selector
            column_name = None
        
        return cls(
            model=model_name,
            column=column_name,
            upstream=upstream,
            downstream=downstream
        )
    
    def __str__(self) -> str:
        """String representation of the selector."""
        direction = ""
        if self.upstream and not self.downstream:
            direction = "upstream"
        elif self.downstream and not self.upstream:
            direction = "downstream"
        else:
            direction = "both directions"
            
        column_str = f".{self.column}" if self.column else ""
        return f"{self.model}{column_str} ({direction})"


def get_upstream_lineage(registry: ModelRegistry, model_name: str, column_name: str, visited=None) -> dict:
    """Recursively get all upstream column references."""
    if visited is None:
        visited = set()
        
    if f"{model_name}.{column_name}" in visited:
        return {}
        
    visited.add(f"{model_name}.{column_name}")
    upstream_refs = {}
    
    # Skip if this is a raw/source table (starts with 'raw_')
    if model_name.startswith('raw_'):
        return {}
    
    try:
        model = registry.get_model(model_name)
    except ModelNotFoundError:
        # If model not found (likely a source/raw table), stop recursion
        return {}
        
    column = model.columns[column_name]
    
    if column.lineage:
        for lineage in column.lineage:
            for source in lineage.source_columns:
                if '.' in source:  # Only process model references, not raw sources
                    src_model, src_column = source.split('.')
                    if src_model not in upstream_refs:
                        upstream_refs[src_model] = {}
                    upstream_refs[src_model][src_column] = lineage
                    
                    # Only recurse if not a raw/source table
                    if not src_model.startswith('raw_'):
                        nested_upstream = get_upstream_lineage(registry, src_model, src_column, visited)
                        for nested_model, nested_cols in nested_upstream.items():
                            if nested_model not in upstream_refs:
                                upstream_refs[nested_model] = {}
                            upstream_refs[nested_model].update(nested_cols)
    
    return upstream_refs

def get_downstream_columns(registry: ModelRegistry, model_name: str, column_name: str, visited=None) -> dict:
    """Recursively get all downstream column references."""
    if visited is None:
        visited = set()
    
    current_ref = f"{model_name}.{column_name}"
    if current_ref in visited:
        return {}
    
    visited.add(current_ref)
    downstream_refs = {}
    model = registry.get_model(model_name)
    
    for other_model_name, other_model in registry.get_models().items():
        for col_name, col in other_model.columns.items():
            if col.lineage:
                for lineage in col.lineage:
                    if current_ref in lineage.source_columns:
                        if other_model_name not in downstream_refs:
                            downstream_refs[other_model_name] = {}
                        downstream_refs[other_model_name][col_name] = lineage
                        
                        nested_downstream = get_downstream_columns(
                            registry, 
                            other_model_name, 
                            col_name, 
                            visited
                        )
                        for nested_model, nested_cols in nested_downstream.items():
                            if nested_model not in downstream_refs:
                                downstream_refs[nested_model] = {}
                            downstream_refs[nested_model].update(nested_cols)
    
    return downstream_refs

def display_lineage_refs(refs: Dict[str, Dict[str, ColumnLineage]], column: Optional[Column] = None) -> None:
    """Display lineage references in a structured format."""
    if refs:
        for model_name, columns in refs.items():
            print(f"\n  Model: {model_name}")
            for col_name, lineage in columns.items():
                print(f"    Column: {col_name}")
                print(f"      Transformation: {lineage.transformation_type}")
                if lineage.sql_expression:
                    print(f"      SQL: {lineage.sql_expression}")
    else:
        if column and column.lineage:
            print("  Direct source columns:")
            for lineage in column.lineage:
                raw_sources = {src for src in lineage.source_columns if src.startswith('raw_')}
                if raw_sources:
                    print(f"    Raw sources: {', '.join(raw_sources)}")
                other_sources = {src for src in lineage.source_columns if not src.startswith('raw_')}
                if other_sources:
                    print(f"    Model sources: {', '.join(other_sources)}")
                if lineage.sql_expression:
                    print(f"    SQL: {lineage.sql_expression}")
        else:
            print("  No lineage information available")

def display_column_lineage(registry: ModelRegistry, model_name: str, column_name: str, upstream: bool = True, downstream: bool = True) -> None:
    """Display column lineage in both directions."""
    model = registry.get_model(model_name)
    column = model.columns[column_name]
    
    print(f"\nColumn: {column.name}")
    print(f"Data type: {column.data_type}")
    print(f"Description: {column.description}")
    
    if upstream:
        print("\nUpstream Lineage:")
        upstream_refs = get_upstream_lineage(registry, model_name, column_name)
        display_lineage_refs(upstream_refs, column)
    
    if downstream:
        print("\nDownstream Columns:")
        downstream_refs = get_downstream_columns(registry, model_name, column_name)
        display_lineage_refs(downstream_refs)

def main():
    parser = argparse.ArgumentParser(description="Generate column lineage for dbt models")
    parser.add_argument(
        "--select",
        type=str,
        help="Select models column to generate lineage for, e.g. 'stg_accounts.account_id+'"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        help="Path to the dbt catalog file",
        default="target/catalog.json"
    )
    parser.add_argument(
        "--manifest",
        type=str,
        help="Path to the dbt manifest file",
        default="target/manifest.json"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )

    args = parser.parse_args()
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")
    
    if not args.select:
        print("Error: --select argument is required")
        sys.exit(1)
        
    try:
        selector = LineageSelector.from_string(args.select)
        print(f"Parsed selector: {selector}")
    except ValueError as e:
        print(f"Error parsing selector: {e}")
        sys.exit(1)

    if args.catalog is None or args.manifest is None:
        print("--catalog and --manifest are not provided, using default values : target/catalog.json and target/manifest.json")

    catalog_path = Path(args.catalog)
    manifest_path = Path(args.manifest)

    if not catalog_path.exists():
        print(f"Catalog file does not exist: {catalog_path}")
        sys.exit(1)

    if not manifest_path.exists():
        print(f"Manifest file does not exist: {manifest_path}")
        sys.exit(1)
    
    try:
        print(f"Loading registry from {catalog_path} and {manifest_path}")
        registry = ModelRegistry(str(catalog_path), str(manifest_path))
        registry.load()
        print(f"Registry loaded with {len(registry.get_models())} models")
    except Exception as e:
        print(f"Error loading registry: {e}")
        sys.exit(1)
    
    try:
        model = registry.get_model(selector.model)
        print(f"Found model: {model.name}")
        print(model.columns)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    if selector.column:
        if selector.column in model.columns:
            column = model.columns[selector.column]
            print(f"\nColumn: {column.name}")
            print(f"Data type: {column.data_type}")
            print(f"Description: {column.description}")
            
            display_column_lineage(
                registry,
                selector.model,
                selector.column,
                upstream=selector.upstream,
                downstream=selector.downstream
            )
        else:
            print(f"Error: Column '{selector.column}' not found in model '{selector.model}'")
            sys.exit(1)
    else:
        print(f"\nModel: {model.name}")
        print(f"Schema: {model.schema_name}")
        print(f"Database: {model.database}")
        print(f"Columns: {', '.join(model.columns.keys())}")
        
        if selector.upstream:
            print(f"\nUpstream dependencies:")
            for upstream in model.upstream:
                print(f"  {upstream}")
        
        if selector.downstream:
            print(f"\nDownstream dependencies:")
            for downstream in model.downstream:
                print(f"  {downstream}")


if __name__ == "__main__":
    main()