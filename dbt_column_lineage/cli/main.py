import argparse
import logging
import sys
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from dbt_column_lineage.artifacts.registry import ModelRegistry


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
    
    # Configure logging
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
    
    # Load the registry
    try:
        print(f"Loading registry from {catalog_path} and {manifest_path}")
        registry = ModelRegistry(str(catalog_path), str(manifest_path))
        registry.load()
        print(f"Registry loaded with {len(registry.get_models())} models")
    except Exception as e:
        print(f"Error loading registry: {e}")
        sys.exit(1)
    
    # Check if model exists
    try:
        model = registry.get_model(selector.model)
        print(f"Found model: {model.name}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Display basic model information based on selector
    if selector.column:
        # Column information
        if selector.column in model.columns:
            column = model.columns[selector.column]
            print(f"\nColumn: {column.name}")
            print(f"Data type: {column.data_type}")
            print(f"Description: {column.description}")
            
            # Show lineage if available
            if column.lineage:
                print("\nLineage:")
                for lineage in column.lineage:
                    print(f"  Transformation: {lineage.transformation_type}")
                    print(f"  Source columns: {', '.join(lineage.source_columns)}")
                    if lineage.sql_expression:
                        print(f"  SQL: {lineage.sql_expression}")
            else:
                print("\nNo lineage information available for this column.")
        else:
            print(f"Error: Column '{selector.column}' not found in model '{selector.model}'")
            sys.exit(1)
    else:
        # Model-level information
        print(f"\nModel: {model.name}")
        print(f"Schema: {model.schema_name}")
        print(f"Database: {model.database}")
        print(f"Columns: {', '.join(model.columns.keys())}")
        
        # Show dependencies based on direction
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