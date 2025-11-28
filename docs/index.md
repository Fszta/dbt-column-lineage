# DBT Column Lineage

Working with large dbt projects, I kept needing to answer a critical question: **"What happens if I change this column?"** Understanding the downstream impact of schema changes is essential for safe refactoring and confident deployments.

DBT Column Lineage provides powerful **impact analysis** to help you understand which models, transformations, and dashboards will be affected before you make changes. It uses column-level data lineage as the foundation for this analysis.

## What is DBT Column Lineage?

DBT Column Lineage analyzes your dbt project artifacts (manifest & catalog) and compiled SQL to build column-level data lineage. This lineage enables comprehensive impact analysis, showing you exactly what breaks when you modify a column.

![Demo](assets/demo_lineage.gif)

## Features

<div class="grid cards" markdown>

-   :material-chart-line:{ .lg .middle } __Impact Analysis__

    ---

    Understand downstream effects of column changes before making them. See which models, transformations, and dashboards will be affected.

-   :material-compass-outline:{ .lg .middle } __Interactive Explorer__

    ---

    A local web server with an intuitive UI to explore model and column lineage visually

-   :material-target:{ .lg .middle } __Column-Level Lineage__

    ---

    Track which source columns contribute to each downstream column, enabling precise impact assessment

</div>

## Quick Start

```bash
# Install
pip install dbt-col-lineage

# Start exploring
dbt-col-lineage --explore
```

[:octicons-arrow-right-24: Get Started](getting-started/quickstart.md){ .md-button .md-button--primary }

## Why Use Impact Analysis?

- **Safe Refactoring**: Understand downstream effects of schema changes before making them
- **Change Planning**: Identify which models and dashboards need updates
- **Risk Assessment**: See the blast radius of column modifications
- **Debugging**: Trace data issues to their source using column lineage
- **Data Governance**: Track sensitive data through your pipeline
