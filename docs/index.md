<div class="hero-section" markdown>

# Understand Impact Before You Change

Know exactly what breaks when you modify a column. Impact analysis powered by column-level data lineage for dbt projects.

<div class="hero-buttons" markdown="1">

[:octicons-rocket-24: Quick Start](getting-started/quickstart.md){ .md-button }
[:octicons-globe-24: Live Demo](https://dbt-column-lineage.onrender.com){ .md-button .md-button--primary }
[:octicons-book-24: View Features](features/impact-analysis.md){ .md-button }

</div>

</div>

## The Problem

Working with large dbt projects, I kept needing to answer a critical question: **"What happens if I change this column?"**

Understanding the downstream impact of schema changes is essential for safe refactoring and confident deployments. Without visibility into column-level dependencies, making changes becomes a risky guessing game.

## The Solution

DBT Column Lineage provides **impact analysis** powered by column-level data lineage. Before you modify a column, see exactly:

- Which models depend on it
- Which transformations use it (SUM, CASE, etc.)
- Which dashboards and exposures will break

![Demo](assets/demo_lineage.gif)

## Why It Matters

<div class="grid cards" markdown>

-   :material-shield-check:{ .md .middle } __Safe Refactoring__

    ---

    Make schema changes with confidence, knowing exactly what will be affected

-   :material-speedometer:{ .md .middle } __Faster Development__

    ---

    Stop guessing and start building. Understand dependencies instantly

-   :material-chart-line:{ .md .middle } __Impact Analysis__

    ---

    See the blast radius of changes before you deploy

</div>

## Quick Start

```bash
# Install
pip install dbt-col-lineage

# Start exploring
dbt-col-lineage --explore
```

[:octicons-arrow-right-24: Get Started](getting-started/quickstart.md){ .md-button .md-button--primary }
