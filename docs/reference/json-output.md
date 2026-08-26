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
      "no_column_info_truncated": false,
      "parse_failed_truncated": false,
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
| `impact.confidence.parse_failed_models` / `no_column_info_models` | The specific models that limited the analysis. In JSON output these are the **complete** lists — a fail-closed consumer that rebuilds "the models parrant couldn't analyze" never misses one, so their lengths always equal `parse_failed` / `no_column_info`. Only human-readable formats (the Markdown/PR comment) cap the rendered names. |
| `impact.confidence.parse_failed_truncated` / `no_column_info_truncated` | Always `false` in JSON output (the lists are complete). A display format sets these `true` when it elided names from the rendered list, so a reader knows more exist. |

!!! tip "Trust the coverage before the impact"
    When `coverage.complete` is `false` (or `confidence.level` is `partial`), the impact
    counts are a **lower bound** — there may be more downstream breakage in the models
    that couldn't be analyzed. Rebuild them (`dbt run --select <model>+ --empty && dbt docs generate`)
    and re-run for a complete answer.

## Rebuild selection and resolution status (`parrant impact`)

The `parrant impact` changeset command (which diffs a base- and head-branch pair of artifacts)
emits three extra top-level blocks in its `--format json` report — `selection`, `resolution`, and
`resolution_summary`. They answer, from the diff alone, **"which models must CI rebuild, and why?"**
They are always present, need no policy, and are derived purely from the change — nothing here is
re-walked or re-classified.

!!! note "Where these live"
    These blocks are top-level keys of the `parrant impact --format json` output (alongside
    `confidence` and, when a policy resolved, `policy_verdict`). They are **not** part of the
    single-column `parrant --select … --format json` lineage view shown above.

### `selection` — the minimal rebuild set

```json
"selection": {
  "has_rebuild": true,
  "rebuild_models": ["int_orders_enriched", "marts.orders"],
  "skippable_models": ["marts.orders_snapshot"],
  "rebuild_selector": "int_orders_enriched marts.orders",
  "confidence_level": "full",
  "widened_to_all_reachable": false
}
```

| Field | Meaning |
|---|---|
| `has_rebuild` | Whether any model must be rebuilt. **Branch on this**, never on the emptiness of `rebuild_selector` (see the sentinel note below). |
| `rebuild_models` | Every model that must be rebuilt — sorted, de-duplicated dbt node names. |
| `skippable_models` | The reachable complement: models reached **only** by a provably additive/pass-through change at full confidence. Informational — the consumer decides whether to actually skip them. |
| `rebuild_selector` | `rebuild_models` space-joined into a single string — a drop-in for `dbt build --select $(...)`. It is the empty string `""` exactly when `has_rebuild` is `false`. |
| `confidence_level` | Mirrors `confidence.level` (`full` or `partial`). |
| `widened_to_all_reachable` | `true` when the analysis could not prove anything safe to skip and widened the rebuild set to every reachable model (see below). |

**The rebuild rule is fail-closed.** A model is in `rebuild_models` when any of the following holds:

- it is one of the edited models themselves;
- it is reached by a change that is **not** provably additive (anything `removed` / `type_changed` /
  `logic_changed`, or a change parrant could not resolve — the conservative default);
- parrant **could not analyze** it (it appears in `confidence.no_column_info_models` or
  `parse_failed_models`).

Only a proven-additive change at **full** confidence lets a reached model land in `skippable_models`.

**Widening.** If `confidence.level` is `partial`, `rebuild_models` widens to every reachable model and
`skippable_models` is emitted empty, with `widened_to_all_reachable: true`. This is the honest "we
could not prove anything safe to skip" state — the consumer still gets a valid, non-empty selector
rather than a false green.

!!! warning "The empty-selector sentinel"
    When nothing must be rebuilt, `has_rebuild` is `false` **and** `rebuild_selector` is `""`. Always
    gate on `has_rebuild`: passing an empty string to `dbt build --select ""` selects **nothing** and
    exits green, which would silently skip a build that was actually needed. Branch on the boolean, not
    the string.

!!! tip "Validate the names, fail closed on any you can't resolve"
    `rebuild_selector` carries dbt **node names**. The consumer should validate them against `dbt ls`
    and treat any name it cannot resolve (a renamed, removed, brand-new, or Python model) as
    fail-closed — rebuild it. That resolution step is the consumer's responsibility, not parrant's.

**Honesty invariants** (a consumer can rely on these):

- `rebuild_models ∪ skippable_models` equals the reachable set exactly — every reachable model has a
  disposition, none is in both.
- `skippable_models` is non-empty **only** at full confidence with nothing truncated.
- `has_rebuild` is `true` if and only if `rebuild_models` is non-empty, and `rebuild_selector` is `""`
  if and only if `has_rebuild` is `false`.
- The result is deterministic: the same artifacts and base always produce the same sorted lists.

### `resolution` — per-model column-resolution status

Every reachable model gets one entry, so a consumer can see **why** a model is in `rebuild_models`:
genuinely reached by a breaking change, or forced in because parrant couldn't resolve its columns.

```json
"resolution": {
  "marts.orders":        { "status": "catalog_backed", "reason": null },
  "int_orders_enriched": { "status": "parsed",         "reason": null },
  "marts.events_wide":   { "status": "no_column_info", "reason": "star_off_cte" },
  "stg_raw__blob":       { "status": "parse_failed",   "reason": "unsupported_sql" },
  "int_python_scores":   { "status": "unresolved",     "reason": "python_model" }
}
```

| `status` | Meaning |
|---|---|
| `catalog_backed` | Columns come from the dbt catalog (warehouse truth). **Resolved.** |
| `parsed` | Columns recovered by parsing the model's SQL. **Resolved.** |
| `no_column_info` | No columns available to trace (e.g. a `SELECT *` off a CTE). **Unanalyzable.** |
| `parse_failed` | The model had SQL the parser could not read. **Unanalyzable.** |
| `unresolved` | Not analyzable in principle from SQL (e.g. a Python model). **Unanalyzable.** |

`reason` is a coarse, advisory hint at *why* an unanalyzable model could not be resolved — one of
`star_off_cte`, `star_modifier`, `missing_catalog`, `python_model`, `unsupported_sql`, or `other`, and
`null` for a resolved model. It is a prioritization signal only: it never upgrades a status and is
never consulted by the rebuild decision. Stripping it leaves every status and every rebuild decision
unchanged.

### `resolution_summary` — the aggregate roll-up

```json
"resolution_summary": {
  "reachable": 312,
  "catalog_backed": 240,
  "parsed": 40,
  "no_column_info": 25,
  "parse_failed": 6,
  "unresolved": 1,
  "rebuild_forced_by_nonresolution": 32,
  "top_reasons": [
    { "reason": "star_off_cte", "count": 21 },
    { "reason": "missing_catalog", "count": 7 }
  ]
}
```

| Field | Meaning |
|---|---|
| `reachable` | Size of the reachable set (equals the number of entries in `resolution`). |
| `catalog_backed` / `parsed` / `no_column_info` / `parse_failed` / `unresolved` | Per-status counts. They reconcile exactly with `confidence`: `no_column_info` + `unresolved` equals `confidence.no_column_info`, and `parse_failed` equals `confidence.parse_failed`. |
| `rebuild_forced_by_nonresolution` | How many models in `rebuild_models` were forced in because parrant couldn't resolve them (status not `catalog_backed`/`parsed`), rather than by a proven reaching change. Always `≤ len(rebuild_models)`. |
| `top_reasons` | The coarse reasons ranked by frequency — a ranked backlog of the SQL shapes causing the most unanalyzable models. |

Tracking `rebuild_forced_by_nonresolution` as a fraction of `rebuild_models` tells you the ceiling on
how much a selective build can ever skip: a small fraction means most of the cone is provably
additive and skips are plentiful; a large one means the win is capped until column resolution
improves, and `top_reasons` points at which SQL shapes to fix first.

## Feeding this to an AI agent

The bundled Claude Code skills under `skills/dbt-lineage/` are the reference
implementation: they wrap these commands and consume this JSON to answer
"what breaks if I change this column?" directly in an agent loop.
