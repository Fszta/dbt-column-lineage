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
    git_changed_models,
    scope_changes_to_models,
)
from dbt_column_lineage.lineage.display import TextDisplay, DotDisplay, JsonDisplay
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.lineage.display.markdown import render_changeset_markdown
from dbt_column_lineage.lineage.service import LineageService, LineageSelector
from dbt_column_lineage.lineage.display.base import LineageStaticDisplay


logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


@click.command()
@click.version_option(package_name="dbt-col-lineage", message="%(version)s")
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

                display.display_coverage(service.get_coverage())

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
    "--scope-git",
    help="Restrict a two-manifest diff to models changed in `git diff <ref>...HEAD` "
    "(e.g. origin/main). Drops changes on models the branch didn't touch — useful "
    "when the base artifacts may be staler than the base ref.",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["markdown", "json"]),
    default="markdown",
    help="Output format for the impact report",
)
@click.option("--adapter", help="Override sqlglot dialect (e.g., tsql, snowflake, bigquery).")
@click.option(
    "--ci",
    is_flag=True,
    help="CI mode: post/update a sticky impact comment on the PR and apply the "
    "--fail-on severity gate as an exit code.",
)
@click.option(
    "--fail-on",
    type=click.Choice(["none", "exposures", "critical", "any"]),
    default="none",
    help="Severity gate for --ci: fail (exit 1) when the impact reaches this "
    "level. Defaults to 'none' (warn only, never block).",
)
@click.option(
    "--github-token",
    envvar="GITHUB_TOKEN",
    help="GitHub token for posting the PR comment (defaults to $GITHUB_TOKEN).",
)
@click.option(
    "--repo",
    envvar="GITHUB_REPOSITORY",
    help="owner/name of the repo (defaults to $GITHUB_REPOSITORY).",
)
@click.option(
    "--pr-number",
    type=int,
    help="Pull request number (defaults to the GitHub Actions event payload).",
)
def impact(
    manifest: str,
    catalog: str,
    base_manifest: Optional[str],
    base_catalog: Optional[str],
    git_base: Optional[str],
    scope_git: Optional[str],
    format: str,
    adapter: Optional[str],
    ci: bool,
    fail_on: str,
    github_token: Optional[str],
    repo: Optional[str],
    pr_number: Optional[int],
) -> None:
    """Diff-driven impact: assess the blast radius of a whole change (PR).

    Provide a base manifest/catalog for the reliable two-manifest diff, or a
    --git-base ref for the git-diff fallback. Add --ci to post the report as a
    sticky PR comment and gate the check with --fail-on.
    """
    try:
        head_service = LineageService(Path(catalog), Path(manifest), adapter=adapter)

        base_service: Optional[LineageService] = None
        changes: List[ColumnChange]
        # Whether structural checks (added/removed/type_changed) could run. They need a
        # real catalog on both sides; the two-manifest path decides this from the builder
        # below. The git-diff fallback is a separate, self-evident coarse mode, so it is
        # left as-is (no catalog note).
        structural_checks_available = True

        if scope_git and not base_manifest:
            click.echo(
                "Error: --scope-git only applies to the two-manifest diff "
                "(--base-manifest); the --git-base fallback is already file-scoped.",
                err=True,
            )
            sys.exit(1)

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
            builder = ChangesetBuilder(base_service.registry, head_service.registry)
            changes = builder.build()
            structural_checks_available = builder.structural_diff_available()
            source = "two-manifest"

            if scope_git:
                # Intersect the precise two-manifest changeset with the models the
                # branch actually touched, so a stale base artifact can't leak
                # already-merged changes into the report.
                scoped_models = git_changed_models(head_service.registry, scope_git)
                changes = scope_changes_to_models(changes, scoped_models)
                source = f"two-manifest scoped to git-diff ({scope_git})"
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
        report["coverage"] = head_service.get_coverage().model_dump()
        report["structural_checks_available"] = structural_checks_available

        if format == "json":
            click.echo(json.dumps(report, indent=2, sort_keys=False))
        else:
            click.echo(render_changeset_markdown(report))

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

    # CI wiring is deliberately outside the try/except above: a failure to render
    # the report is a hard error, but the CI gate deciding to fail the check
    # (exit 1) is a normal outcome we don't want to mask as "Error: 1".
    if ci:
        _run_ci(report, fail_on, github_token, repo, pr_number)


def _run_ci(
    report: dict,
    fail_on_value: str,
    token: Optional[str],
    repo: Optional[str],
    pr_number: Optional[int],
) -> None:
    """Post the sticky PR comment (best-effort) and exit per the severity gate."""
    from dbt_column_lineage.lineage.ci import (
        FailOn,
        gate_exit_code,
        post_sticky_comment,
        resolve_context,
        write_github_outputs,
    )

    # Expose machine-readable results to the composite action (via $GITHUB_OUTPUT)
    # before anything else, so downstream workflow steps get them even if the gate
    # trips (non-zero exit) below.
    write_github_outputs(report)

    body = render_changeset_markdown(report)
    context = resolve_context(token, repo, pr_number)
    if context is None:
        click.echo(
            "CI mode: no PR context resolved (need a GitHub token, repo and PR "
            "number) — skipping the sticky comment.",
            err=True,
        )
    else:
        try:
            outcome = post_sticky_comment(context, body)
            click.echo(
                f"CI mode: {outcome} impact comment on {context.repo}#{context.pr_number}.",
                err=True,
            )
        except Exception as exc:
            # A comment-post failure (network, permissions) shouldn't crash the
            # gate — report it and still apply the severity policy.
            click.echo(f"CI mode: failed to post PR comment: {exc}", err=True)

    fail_on = FailOn(fail_on_value)
    exit_code = gate_exit_code(report.get("summary", {}), fail_on)
    if exit_code != 0:
        click.echo(
            f"CI gate '--fail-on {fail_on.value}' tripped — failing the check.",
            err=True,
        )
    sys.exit(exit_code)


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
