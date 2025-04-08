from typing import Dict, Set, Union
import click
from dbt_column_lineage.models.schema import Column, ColumnLineage
from .base import LineageStaticDisplay

class TextDisplay(LineageStaticDisplay):
    def display_column_info(self, column: Column) -> None:
        click.echo(f"\nColumn: {column.name}")
        click.echo(f"Type: {column.data_type}")
        if column.description:
            click.echo(f"Description: {column.description}")

    def display_upstream(self, refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]) -> None:
        if not refs:
            return

        click.echo("\nUpstream dependencies:")
        
        if 'sources' in refs and isinstance(refs['sources'], set) and refs['sources']:
            click.echo("  Sources:")
            for source in sorted(refs['sources']):
                click.echo(f"    {source}")

        if 'direct_refs' in refs and isinstance(refs['direct_refs'], set) and refs['direct_refs']:
            click.echo("  Direct references:")
            for ref in sorted(refs['direct_refs']):
                click.echo(f"    {ref}")

        for model_name, columns in refs.items():
            if model_name not in ('sources', 'direct_refs') and isinstance(columns, dict):
                click.echo(f"  Model {model_name}:")
                for col_name, lineage in columns.items():
                    click.echo(f"    {col_name}")

    def display_downstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        if not refs:
            return
            
        click.echo("\nDownstream dependencies:")
        for model_name, columns in refs.items():
            click.echo(f"  Model {model_name}:")
            for col_name, lineage in columns.items():
                click.echo(f"    {col_name}")

    def save(self) -> None:
        """No-op for text display as output is immediate."""
        pass