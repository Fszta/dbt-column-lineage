# Installation

## Requirements

- Python 3.9 or higher
- A dbt project with compiled artifacts

## Install from PyPI

The easiest way to install Parrant is via pip:

```bash
pip install parrant
```

!!! note "Formerly `dbt-col-lineage`"
    Parrant was previously published as `dbt-col-lineage`. That package still installs the
    last release under the old name, and the `dbt-col-lineage` command keeps working (with a
    rename notice) for now — but new releases ship as `parrant`.

This will install the latest stable version and all required dependencies.

## Install from Source

If you want to contribute or use the latest development version:

```bash
# Clone the repository
git clone https://github.com/Fszta/parrant.git
cd parrant

# Install with Poetry
poetry install

# Or with pip
pip install -e .
```

## Verify Installation

Check that the CLI is properly installed:

```bash
parrant --help
```

You should see the help message with all available options.

## Next Steps

Now that you have Parrant installed, let's get started with a quick example:

[:octicons-arrow-right-24: Quick Start](quickstart.md)

