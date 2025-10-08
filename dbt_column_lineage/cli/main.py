import sys
from pathlib import Path
import click
import logging
from typing import Optional

from dbt_column_lineage.lineage.display import TextDisplay, DotDisplay
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.lineage.service import LineageService, LineageSelector
from dbt_column_lineage.lineage.display.base import LineageStaticDisplay


logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)

@click.command()
@click.option(
    '--select',
    help="Select models/columns to generate lineage for. Format: [+]model_name[.column_name][+]\n"
         "Examples:\n"
         "  stg_accounts.account_id+  (downstream lineage)\n"
         "  +stg_accounts.account_id  (upstream lineage)\n"
         "  stg_accounts.account_id   (both directions)"
)
@click.option(
    '--explore',
    is_flag=True,
    help="Start an interactive HTML server for exploring model and column lineage"
)
@click.option(
    '--catalog',
    type=click.Path(exists=True),
    default="target/catalog.json",
    help="Path to the dbt catalog file"
)
@click.option(
    '--manifest',
    type=click.Path(exists=True),
    default="target/manifest.json",
    help="Path to the dbt manifest file"
)
@click.option('--format', '-f', 
              type=click.Choice(['text', 'dot']), 
              default='text',
              help='Output format (text or dot graph)')
@click.option('--output', '-o', default='lineage',
              help='Output file name for dot format (without extension)')
@click.option('--port', '-p', 
              default=8000,
              help='Port to run the HTML server (only used with --explore)')
@click.option('--adapter',
              help='Override sqlglot dialect (e.g., tsql, snowflake, bigquery). If set, ignores adapter from manifest.')
def cli(select: str, explore: bool, catalog: str, manifest: str, format: str, output: str, port: int, adapter: Optional[str]) -> None:
    """DBT Column Lineage - Generate column-level lineage for DBT models."""
    if not select and not explore:
        click.echo("Error: Either --select or --explore must be specified", err=True)
        sys.exit(1)
    
    if select and explore:
        click.echo("Error: Cannot use both --select and --explore at the same time", err=True)
        sys.exit(1)

    try:
        service = LineageService(Path(catalog), Path(manifest), adapter=adapter)
        
        if explore:
            click.echo(f"Starting explore mode server on port {port}...")
            lineage_explorer = LineageExplorer(port=port)
            lineage_explorer.set_lineage_service(service)
            lineage_explorer.start()
            return
            
        selector = LineageSelector.from_string(select)
        model = service.registry.get_model(selector.model)
    
        if selector.column:
            if selector.column in model.columns:
                column = model.columns[selector.column]
                
                display: LineageStaticDisplay
                if format == 'dot':
                    display = DotDisplay(output, registry=service.registry)
                    display.main_model = selector.model
                    display.main_column = selector.column
                else:
                    display = TextDisplay()

                display.display_column_info(column)

                if selector.upstream:
                    upstream_refs = service._get_upstream_lineage(selector.model, selector.column)
                    display.display_upstream(upstream_refs)

                if selector.downstream:
                    downstream_refs = service._get_downstream_lineage(selector.model, selector.column)
                    display.display_downstream(downstream_refs)

                if format == 'dot':
                    display.save()
            else:
                available_columns = ", ".join(model.columns.keys())
                click.echo(f"Error: Column '{selector.column}' not found in model '{selector.model}'", err=True)
                sys.exit(1)
        else:
            model_info = service.get_model_info(selector)
            click.echo(f"\nModel: {model_info['name']}")
            click.echo(f"Schema: {model_info['schema']}")
            click.echo(f"Database: {model_info['database']}")
            click.echo(f"Columns: {', '.join(model_info['columns'])}")
            
            if model_info['upstream']:
                click.echo("\nUpstream dependencies:")
                for upstream in model_info['upstream']:
                    click.echo(f"  {upstream}")
                
            if model_info['downstream']:
                click.echo("\nDownstream dependencies:")
                for downstream in model_info['downstream']:
                    click.echo(f"  {downstream}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def main() -> None:
    cli()

if __name__ == "__main__":
    main()