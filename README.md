# DBT Column Lineage

[![Tests](https://github.com/Fszta/dbt-column-lineage/actions/workflows/test.yml/badge.svg)](https://github.com/Fszta/dbt-column-lineage/actions/workflows/test.yml)
[![PyPI Downloads](https://img.shields.io/pypi/dm/dbt-col-lineage?style=flat-square&logo=pypi)](https://pypi.org/project/dbt-col-lineage/)
[![Python Version](https://img.shields.io/pypi/pyversions/dbt-col-lineage?style=flat-square&logo=python)](https://pypi.org/project/dbt-col-lineage/)
[![License](https://img.shields.io/github/license/Fszta/dbt-column-lineage?style=flat-square)](LICENSE)

📖 **[Documentation](https://fszta.github.io/dbt-column-lineage/)** | 🐛 [Report Bug](https://github.com/Fszta/dbt-column-lineage/issues) | 💡 [Request Feature](https://github.com/Fszta/dbt-column-lineage/issues)

## Overview

DBT Column Lineage is a simple tool that helps you visualize and understand column-level data lineage in your dbt projects. It relies on dbt artifacts (manifest & catalog) and compiled sql parsing (to work as expected, it's mandatory to compile your project / run a `dbt docs generate` for catalog generation).

The tool offers several ways to view lineage:
- **Interactive Explorer**: A local web server providing an interactive UI to explore model and column lineage visually. **(Recommended)**
- **Impact Analysis**: Understand downstream effects of column changes, including affected models, transformations, and dashboards.
- **DOT**: Generates GraphViz dot files that can be rendered as static images.
- **Text**: Simple console output showing upstream and downstream dependencies for a specific model or column.


![DBT Column Lineage Demo - Concept](assets/demo_lineage.gif)


## Installation

```bash
pip install dbt-col-lineage
```

## Usage

First, ensure your dbt project is compiled and you have generated the catalog:

```bash
dbt compile
dbt docs generate
```

### 1. Interactive Exploration (Recommended)

To start the interactive lineage explorer:

```bash
dbt-col-lineage --explore \
    --manifest path/to/manifest.json \
    --catalog path/to/catalog.json \
    --port 8080  # Optional port selection
```

This will start a server (defaulting to port 8000). Open your web browser to the specified address (e.g., `http://127.0.0.1:8080`). You can then select models and columns from the sidebar to visualize their lineage directly in the UI.

**Impact Analysis**: After loading lineage for a column, click "Analyze Impact" to see which models, transformations, and dashboards will be affected if you change that column.

### 2. Static Output (Text or DOT)

To generate lineage for a specific model or column directly in the terminal or as a DOT file:

```bash
dbt-col-lineage --select '[+]model_name[.column_name][+]' \
    --manifest path/to/manifest.json \
    --catalog path/to/catalog.json \
    --format [text|dot] \
    --output filename
```

**Examples:**

```bash
# Downstream lineage for stg_transactions.amount in text format
dbt-col-lineage --select stg_transactions.amount+ --format text

# Upstream lineage for stg_accounts.id as a DOT file
dbt-col-lineage --select +stg_accounts.id --format dot --output upstream_account_id.dot

# Both directions for stg_orders in text format
dbt-col-lineage --select stg_orders --format text
```


### Options

- `--explore`: Starts the interactive web server for exploring lineage. Cannot be used with `--select` or `--format`.
- `--select`: Specify model/column for static analysis using the format `[+]model_name[.column_name][+]`. Cannot be used with `--explore`.
  - Add `+` suffix for downstream lineage (e.g., `stg_accounts.id+`)
  - Add `+` prefix for upstream lineage (e.g., `+stg_accounts.id`)
  - No `+` for both directions (e.g., `stg_accounts.id` or `stg_accounts` for model-level)
- `--catalog`: Path to the dbt catalog file (default: `target/catalog.json`)
- `--manifest`: Path to the dbt manifest file (default: `target/manifest.json`)
- `--format`, `-f`: Output format for static analysis (`text` or `dot`). Not used with `--explore`. (default: `text`)
- `--output`, `-o`: Output filename for `dot` format (without extension, default: `lineage`). Not used with `--explore`.
- `--port`, `-p`: Port for the interactive web server when using `--explore` (default: `8000`).
- `--adapter`: Override the SQL dialect used by the parser (sqlglot dialect name, e.g., `tsql`, `snowflake`, `bigquery`). When provided, this overrides the adapter detected from the dbt manifest.

## Limitations
- Doesn't support python models
- Some functions/syntax cannot be parsed properly, leading to models being skipped

## Compatibility

The tool has been tested with the following dbt adapters:
- Snowflake
- SQLite
- DuckDB
- MS SQLServer / TSQL


## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.