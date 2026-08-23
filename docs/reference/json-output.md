# JSON Output

The `json` format emits the full lineage + impact result as a single machine-readable
object. It is the surface designed for **programmatic consumers** — CI scripts and AI
agents that need to reason about a change rather than read a report.

```bash
parrant --select stg_accounts.account_id+ \
  --manifest target/manifest.json \
  --catalog target/catalog.json \
  --format json
```

The selector direction controls which side is populated: `+model.col` fills `upstream`,
`model.col+` fills `downstream`, and `model.col` fills both.

## Shape

```json
{
  "model": "stg_accounts",
  "column": "account_id",
  "data_type": "INTEGER",
  "description": "Unique identifier for the account, cast to integer from the raw source id.",
  "model_description": "Staging model for account data.",
  "downstream": {
    "models": {
      "accounts_snapshot": {
        "account_id": {
          "source_columns": ["stg_accounts.account_id"],
          "transformation_type": "direct",
          "sql_expression": null,
          "description": null
        }
      }
    },
    "sources": [],
    "direct_refs": [],
    "exposures": [
      {
        "name": "transactions_dashboard",
        "type": "dashboard",
        "url": "https://example.com/reports/transactions",
        "description": "Main dashboard showing transaction data and metrics.",
        "depends_on_models": ["transactions"]
      }
    ]
  },
  "impact": {
    "summary": {
      "affected_models": 4,
      "affected_columns": 4,
      "affected_exposures": 2,
      "critical_count": 0,
      "low_impact_count": 3,
      "filter_count": 0
    },
    "affected_models": [
      {
        "name": "accounts_snapshot",
        "resource_type": "snapshot",
        "schema": "main",
        "database": "main",
        "description": "Snapshot of account data using SCD Type 2."
      }
    ],
    "affected_columns": [
      {
        "model": "int_transactions_enriched",
        "column": "amount_category",
        "transformation_type": "derived",
        "sql_expression": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END",
        "severity": "critical",
        "data_type": "TEXT",
        "description": "Bucketed amount label."
      }
    ],
    "affected_exposures": [ /* same shape as downstream.exposures[] */ ],
    "confidence": {
      "reachable_models": 6,
      "resolved_models": 6,
      "unanalyzable_models": 0,
      "no_column_info": 0,
      "parse_failed": 0,
      "no_column_info_models": [],
      "parse_failed_models": [],
      "level": "full"
    }
  },
  "coverage": {
    "models_in_manifest": 13,
    "models_in_catalog": 13,
    "parsed_ok": 13,
    "parse_failed": 0,
    "skipped_no_sql": 0,
    "not_in_catalog_count": 0,
    "failed_models": [],
    "skipped_models": [],
    "complete": true
  }
}
```

`upstream` (when requested with `+model.col`) has the same structure as `downstream`.

## Top-level fields

| Field | Type | Meaning |
|---|---|---|
| `model` | string | The selected model. |
| `column` | string | The selected column. |
| `data_type` | string \| null | Column type, from `catalog.json` when available. |
| `description` | string \| null | dbt-authored description of the selected column (from the manifest). |
| `model_description` | string \| null | dbt-authored description of the selected model. |
| `upstream` / `downstream` | object | Lineage in the requested direction (see below). |
| `impact` | object | The blast-radius analysis (see below). |
| `coverage` | object | How complete the analysis is (see below). |

## `downstream` / `upstream`

| Field | Type | Meaning |
|---|---|---|
| `models` | object | `model → column → {source_columns[], transformation_type, sql_expression, description}`. `transformation_type` is `direct` (pass-through), `renamed`, or `derived` (computed). `sql_expression` is set for derived columns. |
| `sources` | array | Referenced dbt sources. |
| `direct_refs` | array | Direct column references. |
| `exposures` | array | `{name, type, url, description, depends_on_models[]}` — business-facing consumers (dashboards, apps). |

## `impact`

### `impact.summary`

| Field | Meaning |
|---|---|
| `affected_models` | Count of downstream models whose output is affected. |
| `affected_columns` | Count of downstream columns affected. |
| `affected_exposures` | Count of affected business-facing exposures. |
| `critical_count` | Affected columns that **recompute derived logic** (the highest-risk band). |
| `low_impact_count` | Affected columns that are plain pass-through references. |
| `filter_count` | Dependents affected via a predicate (filter/join), not a projected column. |

### `impact.affected_columns[]`

| Field | Meaning |
|---|---|
| `model`, `column` | The affected downstream column. |
| `severity` | `critical` (recomputes derived logic), `low_impact` (pass-through reference), or `filter` (affected via a predicate). |
| `transformation_type` | `direct`, `renamed`, or `derived`. |
| `sql_expression` | The expression when the column is derived (null otherwise). |
| `data_type` | Column type when known. |
| `description` | dbt-authored description when present. |

`affected_models[]` carries `{name, resource_type, schema, database, description}`;
`affected_exposures[]` matches `downstream.exposures[]`.

## `coverage` and `impact.confidence` — is the answer complete?

Static analysis is only as complete as the artifacts allow. These blocks tell a consumer
**when the result is a lower bound** so an agent doesn't treat a partial answer as
"nothing breaks."

| Field | Meaning |
|---|---|
| `coverage.models_in_manifest` / `models_in_catalog` | Universe sizes. A catalog smaller than the manifest is expected (the catalog reflects warehouse-built state). |
| `coverage.parsed_ok` / `parse_failed` | How many models' SQL parsed. `failed_models[]` lists the ones that didn't. |
| `coverage.not_in_catalog_count` | Reachable models with no catalog entry (column types unknown). |
| `coverage.complete` | `true` only when every reachable model was analyzable. |
| `impact.confidence.level` | `full` or `partial` — the headline confidence for this impact result. |
| `impact.confidence.parse_failed_models` / `no_column_info_models` | The specific models that limited the analysis. |

!!! tip "Trust the coverage before the impact"
    When `coverage.complete` is `false` (or `confidence.level` is `partial`), the impact
    counts are a **lower bound** — there may be more downstream breakage in the models
    that couldn't be analyzed. Rebuild them (`dbt run --select <model>+ --empty && dbt docs generate`)
    and re-run for a complete answer.

## Feeding this to an AI agent

The bundled Claude Code skills under `skills/dbt-lineage/` are the reference
implementation: they wrap these commands and consume this JSON to answer
"what breaks if I change this column?" directly in an agent loop.
