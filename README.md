# DBT Column Lineage

[![Tests](https://github.com/Fszta/dbt-column-lineage/actions/workflows/test.yml/badge.svg)](https://github.com/Fszta/dbt-column-lineage/actions/workflows/test.yml)


## Overview

DBT Column Lineage is a simple tool that helps you visualize and understand column-level data lineage in your dbt projects. It relies on dbt artifacts (manifest & catalog) and compiled sql parsing (to work as expected, it's mandatory to compile your project / run a `dbt docs generate` for catalog generation). 

The tool currently supports three output formats:
- **HTML**: Interactive web view for exploring column lineage relationships
- **DOT**: Generates GraphViz dot files that can be rendered as images
- **Text**: Simple console output showing upstream and downstream dependencies


![DBT Column Lineage Demo](assets/demo_lineage.gif)


## Installation

```bash
pip install dbt-col-lineage==0.1.0
```

## Usage

First, generate your dbt manifest and catalog files:

```bash
dbt compile
dbt docs generate
```

Then use the tool to analyze column lineage:

```bash
dbt-col-lineage --select stg_transactions.amount+ \
    --manifest path/to/manifest.json \
    --catalog path/to/catalog.json \
    --format html
```

### Options

- `--select`: Specify model/column to analyze using the format `[+]model_name[.column_name][+]`
  - Add `+` suffix for downstream lineage (e.g., `stg_accounts.id+`)
  - Add `+` prefix for upstream lineage (e.g., `+stg_accounts.id`)
  - No `+` for both directions (e.g., `stg_accounts.id`)
- `--catalog`: Path to the dbt catalog file (default: `target/catalog.json`)
- `--manifest`: Path to the dbt manifest file (default: `target/manifest.json`)
- `--format`, `-f`: Output format - `text`, `dot`, or `html` (default: `text`)
- `--output`, `-o`: Output filename for dot format (default: `lineage`)
- `--port`, `-p`: Port for the HTML server when using html format (default: `8000`)

## Limitations
- Doesn't support python models
- Some functions/syntax cannot be parsed properly, leading to models being skipped

## Compatibility

The tool has been tested with the following dbt adapters:
- Snowflake
- SQLite
- DuckDB

While it should work with other adapters as well (since it relies on dbt artifacts), these are the ones that have been specifically tested.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.