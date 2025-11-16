# Impact Analysis

Understand the downstream effects of changing a column before you make the change.

## Overview

Impact Analysis helps you answer the critical question: **"What happens if I change this column?"**

When you select a column in the Interactive Explorer, you can analyze its impact across your entire dbt project. This feature shows you:

- **Which models and columns** depend on this column
- **Which transformations** (SUM, CASE, etc.) use this column and may need review
- **Which dashboards and exposures** will be impacted

## How to Use

1. Start the Interactive Explorer:
   ```bash
   dbt-col-lineage --explore
   ```

2. Select a model and column from the sidebar

3. Load the lineage graph

4. Click **"Analyze Impact"** in the card that appears on the graph

5. Review the impact analysis panel that opens on the right

## Understanding the Results

The impact analysis categorizes columns into:

- **Requires Review**: Columns with transformations (derived columns) that may break if the source column changes
- **Pass-through Columns**: Direct references that will automatically propagate changes
- **Affected Models**: All models in the dependency chain
- **Affected Exposures**: Dashboards and reports that may be impacted

![Impact Analysis Screenshot](../assets/impact-analysis.png)
*Impact Analysis panel showing column dependencies and transformations*

## Use Cases

**Before modifying a column:**
- Understand which transformations depend on it
- Identify which dashboards need updates
- Plan your change strategy

**During refactoring:**
- Track the blast radius of schema changes
- Prioritize which models to update first
- Ensure no downstream dependencies are missed

