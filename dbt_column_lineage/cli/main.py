import json
import sys
from pathlib import Path
import click
import logging
from typing import List, Optional

from dbt_column_lineage.lineage.changeset import (
    ChangesetBuilder,
    ColumnChange,
    build_changeset_report,
    build_git_changeset,
)
from dbt_column_lineage.lineage.display import TextDisplay, DotDisplay, JsonDisplay
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.lineage.display.markdown import render_changeset_markdown
from dbt_column_lineage.lineage.service import LineageService, LineageSelector
from dbt_column_lineage.lineage.display.base import LineageStaticDisplay


logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


@click.command()
@click.option(
    "--select",
    help="Select models/columns to generate lineage for. Format: [+]model_name[.column_name][+]\n"
    "Examples:\n"
    "  stg_accounts.account_id+  (downstream lineage)\n"
    "  +stg_accounts.account_id  (upstream lineage)\n"
    "  stg_accounts.account_id   (both directions)",
)
@click.option(
    "--explore",
    is_flag=True,
    help="Start an interactive HTML server for exploring model and column lineage",
)
@click.option(
    "--catalog",
    type=click.Path(exists=True),
    default="target/catalog.json",
    help="Path to the dbt catalog file",
)
@click.option(
    "--manifest",
    type=click.Path(exists=True),
    default="target/manifest.json",
    help="Path to the dbt manifest file",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["text", "dot", "json"]),
    default="text",
    help="Output format (text, dot graph, or machine-readable json)",
)
@click.option(
    "--output", "-o", default="lineage", help="Output file name for dot format (without extension)"
)
@click.option(
    "--port", "-p", default=8000, help="Port to run the HTML server (only used with --explore)"
)
@click.option(
    "--adapter",
    help="Override sqlglot dialect (e.g., tsql, snowflake, bigquery). If set, ignores adapter from manifest.",
)
def cli(
    select: str,
    explore: bool,
    catalog: str,
    manifest: str,
    format: str,
    output: str,
    port: int,
    adapter: Optional[str],
) -> None:
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
                if format == "dot":
                    display = DotDisplay(output, registry=service.registry)
                    display.main_model = selector.model
                    display.main_column = selector.column
                elif format == "json":
                    display = JsonDisplay()
                else:
                    display = TextDisplay()

                display.display_column_info(column)

                if selector.upstream:
                    upstream_refs = service._get_upstream_lineage(selector.model, selector.column)
                    display.display_upstream(upstream_refs)

                if selector.downstream:
                    downstream_refs = service._get_downstream_lineage(
                        selector.model, selector.column
                    )
                    display.display_downstream(downstream_refs)

                if format == "json" and isinstance(display, JsonDisplay):
                    # Impact analysis is the flagship capability; include it whenever
                    # downstream lineage was requested so the JSON is self-contained.
                    if selector.downstream:
                        display.set_impact(
                            service.get_column_impact(selector.model, selector.column)
                        )
                    display.save()

                if format == "dot":
                    display.save()
            else:
                available_columns = ", ".join(model.columns.keys())
                click.echo(
                    f"Error: Column '{selector.column}' not found in model '{selector.model}'",
                    err=True,
                )
                sys.exit(1)
        else:
            model_info = service.get_model_info(selector)
            click.echo(f"\nModel: {model_info['name']}")
            click.echo(f"Schema: {model_info['schema']}")
            click.echo(f"Database: {model_info['database']}")
            click.echo(f"Columns: {', '.join(model_info['columns'])}")

            if model_info["upstream"]:
                click.echo("\nUpstream dependencies:")
                for upstream in model_info["upstream"]:
                    click.echo(f"  {upstream}")

            if model_info["downstream"]:
                click.echo("\nDownstream dependencies:")
                for downstream in model_info["downstream"]:
                    click.echo(f"  {downstream}")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


@click.command()
@click.option(
    "--manifest",
    type=click.Path(exists=True),
    default="target/manifest.json",
    help="Path to the current (head) dbt manifest file",
)
@click.option(
    "--catalog",
    type=click.Path(exists=True),
    default="target/catalog.json",
    help="Path to the current (head) dbt catalog file",
)
@click.option(
    "--base-manifest",
    type=click.Path(exists=True),
    help="Path to the base (target-branch) manifest. Enables two-manifest diff.",
)
@click.option(
    "--base-catalog",
    type=click.Path(exists=True),
    help="Path to the base (target-branch) catalog. Defaults to catalog.json next "
    "to --base-manifest. Required for column-level (add/remove/type) diffing.",
)
@click.option(
    "--git-base",
    help="Fallback: diff changed .sql files against this git ref (e.g. main) when "
    "no base manifest is available. Reports touched models as logic changes.",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    help="Output format for the impact report",
)
@click.option("--adapter", help="Override sqlglot dialect (e.g., tsql, snowflake, bigquery).")
def impact(
    manifest: str,
    catalog: str,
    base_manifest: Optional[str],
    base_catalog: Optional[str],
    git_base: Optional[str],
    format: str,
    adapter: Optional[str],
) -> None:
    """Diff-driven impact: assess the blast radius of a whole change (PR).

    Provide a base manifest/catalog for the reliable two-manifest diff, or a
    --git-base ref for the git-diff fallback.
    """
    try:
        head_service = LineageService(Path(catalog), Path(manifest), adapter=adapter)

        base_service: Optional[LineageService] = None
        changes: List[ColumnChange]

        if base_manifest:
            resolved_base_catalog = base_catalog
            if not resolved_base_catalog:
                sibling = Path(base_manifest).parent / "catalog.json"
                if sibling.exists():
                    resolved_base_catalog = str(sibling)
            if not resolved_base_catalog:
                click.echo(
                    "Error: --base-catalog is required for two-manifest diff "
                    "(no catalog.json found next to --base-manifest).",
                    err=True,
                )
                sys.exit(1)

            base_service = LineageService(
                Path(resolved_base_catalog), Path(base_manifest), adapter=adapter
            )
            changes = ChangesetBuilder(base_service.registry, head_service.registry).build()
            source = "two-manifest"
        elif git_base:
            changes = build_git_changeset(head_service.registry, git_base)
            source = f"git-diff ({git_base})"
        else:
            click.echo(
                "Error: provide --base-manifest (two-manifest diff) or --git-base "
                "(git-diff fallback) to derive the changeset.",
                err=True,
            )
            sys.exit(1)

        aggregated = head_service.get_changeset_impact(changes, base_service=base_service)
        report = build_changeset_report(source, changes, aggregated)

        if format == "json":
            click.echo(json.dumps(report, indent=2, sort_keys=False))
        else:
            click.echo(render_changeset_markdown(report))

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)


def main() -> None:
    # Keep `cli` fully backward-compatible (existing --select/--explore usage and
    # tests target it directly) while exposing `impact` as a subcommand.
    argv = sys.argv[1:]
    if argv and argv[0] == "impact":
        impact.main(args=argv[1:], prog_name="dbt-col-lineage impact")
    else:
        cli()


if __name__ == "__main__":
    main()
