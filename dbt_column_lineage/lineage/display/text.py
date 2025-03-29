import click
from typing import Dict
from dbt_column_lineage.models.schema import Column, ColumnLineage
from dbt_column_lineage.lineage.display.base import LineageDisplay

class TextDisplay(LineageDisplay):
    def display_column_info(self, column: Column) -> None:
        click.echo(f"\nColumn: {column.name}")
        click.echo(f"Data type: {column.data_type}")
        if column.description:
            click.echo(f"Description: {column.description}")

    def display_upstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        click.echo("\nUpstream Lineage:")
        self._display_refs(refs)

    def display_downstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        click.echo("\nDownstream Lineage:")
        self._display_refs(refs)

    def _display_refs(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        if not refs:
            click.echo("  No lineage information available")
            return

        model_refs = {k: v for k, v in refs.items() if k not in ('sources', 'direct_refs')}
        if not model_refs:
            return

        for model_name, columns in sorted(model_refs.items()):
            click.echo(f"\n  Model: {model_name}")
            for col_name, lineage in sorted(columns.items()):
                click.echo(f"    Column: {col_name}")
                click.echo(f"      Transformation: {lineage.transformation_type}")
                if lineage.sql_expression:
                    click.echo(f"      SQL: {lineage.sql_expression}")