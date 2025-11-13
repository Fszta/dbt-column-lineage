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

This starts a local web server where you can explore your lineage interactively.

## Static Analysis

For terminal output or documentation, use static analysis:

### Text Format

```bash
# Show all dependencies for a model
dbt-col-lineage --select my_model --format text

# Show downstream dependencies for a specific column
dbt-col-lineage --select stg_orders.order_id+ --format text

# Show upstream dependencies for a column
dbt-col-lineage --select +final_table.revenue --format text
```

### DOT Format (GraphViz)

Generate DOT files that can be rendered as images:

```bash
dbt-col-lineage --select my_model --format dot --output my_lineage
```

Then render with GraphViz:

```bash
# Install GraphViz (if needed)
# macOS: brew install graphviz
# Ubuntu/Debian: sudo apt-get install graphviz

# Render as PNG
dot -Tpng my_lineage.dot -o my_lineage.png

# Render as SVG
dot -Tsvg my_lineage.dot -o my_lineage.svg
```

## Selection Syntax

The selection syntax follows this pattern: `[+]model_name[.column_name][+]`

| Syntax | Meaning | Example |
|--------|---------|---------|
| `model` | Both directions | `stg_orders` |
| `+model` | Upstream only | `+stg_orders` |
| `model+` | Downstream only | `stg_orders+` |
| `model.column` | Column-level | `stg_orders.order_id` |
| `+model.column` | Upstream column | `+fct_orders.total_revenue` |
| `model.column+` | Downstream column | `stg_transactions.amount+` |

## Common Use Cases

**Trace a column's origins:**
```bash
dbt-col-lineage --select +orders.total_amount --format text
```

**Understand impact of changes:**
```bash
dbt-col-lineage --select stg_customers.email+ --format text
```

**Generate documentation diagram:**
```bash
dbt-col-lineage --select dim_customers --format dot --output customers_lineage
dot -Tsvg customers_lineage.dot -o customers_lineage.svg
```

!!! warning "Column Names Are Case-Sensitive"
    Make sure to use the exact column name as it appears in your dbt models.
