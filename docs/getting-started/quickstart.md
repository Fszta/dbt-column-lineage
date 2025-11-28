# Quick Start

This guide will help you get started with DBT Column Lineage in just a few minutes.

## Prerequisites

Before using DBT Column Lineage, you need to generate dbt artifacts:

```bash
cd your-dbt-project

# Compile your dbt project
dbt compile

# Generate the catalog (required for column information)
dbt docs generate
```

This creates two files in your `target/` directory:

- `manifest.json` - Contains model definitions and dependencies
- `catalog.json` - Contains column-level metadata

!!! tip
    The catalog file is essential for column-level lineage. Make sure to run `dbt docs generate` before using this tool.

## Interactive Explorer (Recommended)

Launch the interactive web interface:

```bash
dbt-col-lineage --explore
```

If your artifacts are in a custom location:

```bash
dbt-col-lineage --explore \
    --manifest path/to/manifest.json \
    --catalog path/to/catalog.json \
    --port 8080  # Optional, defaults to 8000
```

This starts a local web server where you can explore your lineage interactively and perform impact analysis.

## Impact Analysis Workflow

1. **Select a column**: Choose a model and column from the sidebar
2. **Load lineage**: The graph will show how data flows through your transformations
3. **Analyze impact**: Click "Analyze Impact" to see:
   - Which models depend on this column
   - Which transformations use this column (and may need review)
   - Which dashboards and exposures will be impacted

This helps you understand the downstream effects of column changes before you make them.

## Common Use Cases

**Before modifying a column:**
- Understand which transformations depend on it
- Identify which dashboards need updates
- Plan your change strategy

**During refactoring:**
- Track the blast radius of schema changes
- Prioritize which models to update first
- Ensure no downstream dependencies are missed

!!! tip "Learn More"
    Check out the [Impact Analysis](../features/impact-analysis.md) guide for detailed information about using this feature.
