# Installation

## Requirements

- Python 3.9 or higher
- A dbt project with compiled artifacts

## Install from PyPI

The easiest way to install DBT Column Lineage is via pip:

```bash
pip install dbt-col-lineage
```

This will install the latest stable version and all required dependencies.

## Install from Source

If you want to contribute or use the latest development version:

```bash
# Clone the repository
git clone https://github.com/Fszta/dbt-column-lineage.git
cd dbt-column-lineage

# Install with Poetry
poetry install

# Or with pip
pip install -e .
```

## Verify Installation

Check that the CLI is properly installed:

```bash
dbt-col-lineage --help
```

You should see the help message with all available options.

## Next Steps

Now that you have DBT Column Lineage installed, let's get started with a quick example:

[:octicons-arrow-right-24: Quick Start](quickstart.md)

