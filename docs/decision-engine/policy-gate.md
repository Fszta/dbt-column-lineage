# Policy gate guide

The policy gate is a **metadata-agnostic rule engine**. You write rules — `predicate → action` —
over *any* dbt `meta`, the change kind, the semantic breaking signal, and the lineage reach; the
engine evaluates them against a pull request's changeset and returns a **verdict**:
`block`, `warn`, or `allow`, plus a selective build/test set and any notifications to route.
A `block` is always a [**`block-until`**](#a-block-is-a-block-until-not-a-dead-end): it states
how it clears and lifts itself on the next push, so the gate is an exit path, not a wall.

> **Every term below is defined once in the [Glossary](glossary.md)** — actions, gate
> decisions, reach kinds/mechanisms, operators, and the fail-safe knobs, with the code enum ↔
> UI ↔ docs alignment pinned.

!!! abstract "The tool ships the engine — you ship the rules"
    No metadata key is privileged. `critical`, `pii`, `readable_by`, `tier` are **example
    consumer configs**, not built-ins. The worked examples below are exactly that: real,
    shipped example policies you copy and adapt to your own `meta` conventions.

## Quick start

Author a `policy.yml`, then gate on it:

```bash
dbt-col-lineage impact \
  --manifest target/manifest.json --catalog target/catalog.json \
  --base-manifest base/manifest.json --base-catalog base/catalog.json \
  --policy policy.yml \
  --fail-on policy
```

- `--policy policy.yml` resolves the rules and attaches a `policy_verdict` to the report.
- `--fail-on policy` makes the check **exit 1** when the verdict is `block` (and only `block`).

**Resolution order** (first found): the explicit `--policy PATH`, then a
`./dbt-col-lineage.policy.yml` in the working directory. No policy found → the engine is inert
and the tool falls back to the legacy `safe`/`review`/`block` verdict (fully backward compatible).

!!! danger "A broken policy fails loudly"
    A present-but-invalid policy (unknown `version`, malformed predicate) raises an error and
    fails the run — it is **never** silently treated as "no policy". A governance gate must not
    disappear because someone fat-fingered the YAML.

## The rule model

A policy is a `version` header, optional `defaults`, and a list of `rules`. Each rule is a
**predicate** (when does it apply?) mapped to one or more **actions** (what happens when it does?).

```yaml
version: 1                       # schema version; the engine rejects unknown majors
defaults:
  on_missing_meta: fail_closed   # fail_closed | fail_open | skip  (default fail_closed)
  on_error: fail_closed          # how operator/type mismatches resolve
rules:
  - id: my-rule                  # required, unique — appears in the verdict
    description: What this rule protects.
    scope: change                # change (per changed column) | aggregate (once for the PR)
    predicate: <predicate-tree>
    action: <action-or-list>
    on_missing_meta: fail_open   # optional per-rule override of the default
```

Each rule is evaluated against **every changed column** (`scope: change`, the default), or once
against the whole changeset (`scope: aggregate`, for project-wide rules). The changed column a
rule fired on is recorded in the verdict, so the report can say *which* change tripped it.

### Predicates

A predicate is a tree of **leaf conditions** combined with `all` (AND), `any` (OR), and `not`.
`all`/`any` take a list; `not` takes a single node. A bare leaf is itself a valid predicate.

```yaml
predicate:
  all:                                 # AND — every child must hold
    - meta: { key: pii, op: is_true }
    - reach:
        kind: model
        where:
          any:                         # OR — inside the reach's inner predicate
            - meta: { key: audience, op: eq, value: public }
            - meta: { key: readable_by, op: not_subset_of, value: [COMPLIANCE, PAYMENT_OPS] }
    - not:
        change: { field: kind, op: eq, value: added }
```

A leaf condition matches on exactly one of **four axes**:

#### `change` — facts about the changed column itself

| Field | Values |
|---|---|
| `kind` | `added`, `removed`, `type_changed`, `logic_changed` |
| `semantic` | `equivalent`, `meaning_changed`, `indeterminate` |
| `breaking` | boolean — `true` for anything not `equivalent` (folds in `indeterminate`/absent) |
| `model`, `column` | string match (`eq` / `in` / `matches`) |

```yaml
change: { field: breaking, op: is_true }
```

#### `meta` — a dbt `meta` key on the changed model or column

```yaml
meta: { key: pii, op: is_true }
meta: { key: governance.tier, op: eq, value: gold }   # dotted path into nested meta
```

Keys are dotted paths resolved against the node's merged meta (dbt's `config.meta` over
top-level `meta`). Missing-key handling is governed by [`on_missing_meta`](#fail-safe-defaults).

#### `reach` — a *quantified* condition over the change's downstream reach { #reach-conditions }

"Does this change reach a downstream object whose own meta satisfies an inner predicate?"

```yaml
reach:
  kind: model                  # model | column | exposure — what to scan
  mechanism: [derived_recompute, rowset_filter]   # optional: restrict by how it propagates
  where:                       # inner predicate, evaluated against each reached object's meta
    meta: { key: critical, op: is_true }
  min_count: 1                 # require at least N matches (default 1)
```

- `reach.kind` picks the downstream object type. **BI dashboards surface as `kind: exposure`**
  (Metabase is the first supported connector) — see the [cross-boundary guide](metabase.md).
- `reach.mechanism` filters by how the change propagates, using the tool's existing taxonomy:
  `derived_recompute` (the value is recomputed), `rowset_filter` (used in a filter/join),
  `renamed_passthrough`, `direct_passthrough`. This is the "recompute vs pass-through"
  distinction that powers selective rebuilds.
- `reach.where` matches on the **reached object's** `meta.*` (and, for exposures, its `type` /
  `owner` / `name`).

#### `structural` — booleans the pipeline already computes

```yaml
structural: { fact: provable_test_break }   # the change orphans a dbt test
```

| Fact | True when… |
|---|---|
| `provable_test_break` | the change removes/renames a column a dbt test still targets |
| `touches_exposure` | the change reaches ≥1 exposure |
| `reaches_anything` | the change reaches ≥1 downstream node |

### Operators

`meta` and `change` string conditions take an `op`:

| Operator | Meaning |
|---|---|
| `exists` / `absent` | key present / absent |
| `is_true` / `is_false` | truthy / falsy |
| `eq` / `ne` | scalar equality |
| `in` / `not_in` | scalar ∈ / ∉ a list |
| `matches` | regex full-match (strings) |
| `intersects` | list shares ≥1 element with the given list |
| `subset_of` / `not_subset_of` / `superset_of` | list containment |
| `gt` / `ge` / `lt` / `le` | numeric comparison |

### Actions

A rule emits one or more actions:

| Action | Effect on the verdict |
|---|---|
| `block` | contributes `block` to the gate (most-severe-wins). |
| `warn` | contributes `warn` — advisory; never causes a non-zero exit. |
| `add-to-build-set` | adds the reached (or subject) models to the selective **build set**. |
| `add-to-test-set` | adds the reached models (or their tests) to the selective **test set**. |
| `notify` | appends a notification intent (`channel`, `target`, `message`) for your CI to route. |

Actions can carry parameters:

```yaml
action:
  - type: add-to-build-set
    include: reached                 # reached | subject | both
    mechanism: [derived_recompute]   # optional: only nodes reached this way
  - type: notify
    channel: slack
    target: "#data-governance"
    message: "PII {change.model}.{change.column} reaches a non-allowlisted reader"
```

`message` supports a small, safe interpolation vocabulary — `{change.model}`, `{change.column}`,
`{reach.count}`, `{rule.id}` — no arbitrary code.

### How multiple rules combine into one verdict

- **Gate decision: most-severe-wins.** `block > warn > allow`. Any `block` → the verdict blocks.
- **Build/test sets: union.** Every fired `add-to-build-set` / `add-to-test-set` contributes;
  the engine dedups across rules.
- **Notifications: accumulate** (deduped by `channel`/`target`/`message`).
- **No short-circuit.** All rules evaluate (so the build/test sets are complete), and there is
  **no priority/override** in v1 — a `warn` can never cancel a `block`.

## Worked examples (the shipped example policies)

These live in the repo under `tests/resources/policies/` and are the canonical starting points.

### 1. PII must not reach a reader outside the allowlist

The offline analogue of a a PII-exposure gate gate, expressed as pure config: a change to a
PII-tagged column that reaches a downstream model whose declared readers are **not** a subset of
the allowlist blocks the PR and notifies governance.

```yaml
version: 1
rules:
  - id: pii-outside-allowlist
    description: >
      A change to a PII-tagged column that reaches a consumer readable by a role
      outside the compliance allowlist must block (offline PII-exposure check).
    scope: change
    predicate:
      all:
        - meta: { key: pii, op: is_true }          # subject column is PII
        - reach:
            kind: model                             # scan reached downstream models
            where:
              meta:
                key: readable_by                    # roles the mart exposes to
                op: not_subset_of
                value: [COMPLIANCE, PAYMENT_OPS]
    action:
      - type: block
      - type: notify
        channel: slack
        target: "#data-governance"
        message: "PII {change.model}.{change.column} reaches a non-allowlisted reader"
```

Note the fail-safe subtlety: because the reach uses a **value** operator (`not_subset_of`) on
`readable_by`, a mart that *forgot* to declare its readers resolves to *unknown → risk present*
under `fail_closed`, so it **blocks** — a mart with no declared readers is treated as if it
exposes to everyone. (See [fail-safe defaults](#fail-safe-defaults).)

### 2. A breaking change reaching a critical mart

Expresses `critical:true` gating — and it is **breaking-aware**: a proven-equivalent refactor
that reaches a critical mart does *not* block. When it does block, it also schedules a selective
rebuild of only the descendants that actually recompute.

```yaml
version: 1
rules:
  - id: breaking-reaches-critical
    description: A breaking change that reaches a critical mart must block.
    scope: change
    predicate:
      all:
        - change: { field: breaking, op: is_true }   # semantic != equivalent (fail-safe)
        - reach:
            kind: model
            where:
              meta: { key: critical, op: is_true }
    action:
      - type: block
      - type: add-to-build-set
        include: reached
        mechanism: [derived_recompute, rowset_filter]   # rebuild only what recomputes
```

### 3. Reproduce the legacy `--fail-on tests` gate

The provable-break signal is a built-in **structural** fact, not a hardcoded verdict. The old
`--fail-on tests` behaviour is one rule:

```yaml
version: 1
rules:
  - id: provable-break-blocks
    description: A change that provably breaks a dbt test must block (legacy --fail-on tests).
    scope: change
    predicate:
      structural: { fact: provable_test_break }
    action:
      - type: block
```

## Fail-safe defaults

The engine is **fail-closed by default**: an undecidable input biases toward *blocking* so a
governance gate never silently passes a risky change.

| Situation | `fail_closed` (default) | `fail_open` | `skip` |
|---|---|---|---|
| **Missing meta key** on the node a rule inspects | for a *blocking* rule, resolves toward "unknown = risk = fire" | resolves `False` (rule can't fire on absence) | the rule is skipped for that subject |
| **Operator/type mismatch** (e.g. `subset_of` on a scalar) | governed by `on_error` — `True` for blocking rules, `False` otherwise | — | — |
| **Unresolved reach** (a removed column with no base catalog) | reach resolves `True` (can't prove it *doesn't* reach) | reach resolves `False` | — |

The asymmetry is deliberate: **blocking rules bias toward firing; non-blocking rules bias toward
staying silent** — the safety mechanism over-blocks but never manufactures spurious warnings.

!!! warning "Gate on `change.breaking`, not `change.semantic == …`"
    `change.semantic` is always a *present* value, so `change.semantic eq meaning_changed`
    resolves to a plain `False` for an `indeterminate`/absent semantic — **no fail-closed bias**.
    A consumer who wants "treat anything possibly-breaking as breaking" must gate on
    **`change.breaking is_true`**, which folds `indeterminate` and absent into breaking. This is
    the single most important fail-safe rule to internalize.

!!! tip "Absent-boolean is by design, not a hole"
    `is_true` / `exists` on a *missing* key resolves `False` — a model that forgot `critical:true`
    is **not** blocked. A metadata gate can't police metadata it wasn't given; that "miss = blind"
    risk is one the consumer accepts. To get fail-closed-on-missing instead, use a **value**
    operator on the reached object (like `readable_by not_subset_of …` in example 1) so a missing
    key becomes *unknown → risk → block*. Opposite behaviours from one engine, purely by operator
    choice.

### Isolating a subset of exposures (the `fail_open` + guard pattern)

To write a rule that fires only on a *specific* class of reached exposure — e.g. only Metabase
dashboards, not dbt-native exposures — combine `on_missing_meta: fail_open` with a `meta` guard
in the reach `where`. Under `fail_closed`, dbt-native exposures (which lack the guard meta) would
be treated as risk and block regardless; `fail_open` lets the guard do its job. See the
[cross-boundary guide](metabase.md#isolating-metabase-only-rules) for the worked example.

## Reading the verdict

### A block is a `block-until`, not a dead end

The point of the gate is *action-driven awareness*, so a `block` answers not just "you may not
merge" but **"blocked until when?"**. Every block is a **`block-until`**: the gate is stateless
and re-runs on **every push**, so a block **clears itself** the moment the change stops tripping
the rule that fired it — no manual override, no ticket, no re-approval. The Markdown verdict says
exactly this, so the person hitting it sees the exit path.

| Blocked until… | How it clears | Cost |
|---|---|---|
| the change is no longer breaking | revert it, or make it a proven-`equivalent` refactor → the `change.breaking` predicate goes false on the next push | free (self-clearing) |
| it no longer reaches the flagged object | narrow the change, or the reached model/dashboard is retired/re-pointed → the `reach` predicate goes false | free (self-clearing) |
| the downstream / schema absorbs it | evolve the consumer (add the column, widen the type, update the test) → the `structural`/`reach` predicate goes false | free (self-clearing) |

All three are **self-clearing** — they need no new tool feature, because the predicate simply
evaluates to false on the next run. That is the whole release model today.

!!! note "Manual-override release paths are deliberately not built (yet)"
    Two *other* exit paths are conceivable — **block-until-acknowledged** (an owner signs off via
    a PR label the gate honors) and **block-until-proven** (a `/data-diff` result proves the
    values are unchanged). Both require the gate to consume a **new external input**, which would
    cut against the offline / zero-credential guarantee and add an override concept the engine
    deliberately omits in v1 (a `warn` can never cancel a `block`; see
    [how rules combine](#how-multiple-rules-combine-into-one-verdict)). They are noted here as
    possible future work, **not** currently available — the self-clearing paths above are the
    supported way to release a block.

### Exit code

| Invocation | Exit 0 | Exit 1 |
|---|---|---|
| `--fail-on policy` | verdict is `warn` or `allow` | verdict is **`block`** |
| any other `--fail-on` value | policy runs but doesn't gate | (the chosen gate decides) |

Only `block` fails the check under `--fail-on policy`; `warn` and `allow` never do. If you pass
`--fail-on policy` with no resolvable policy, the tool warns that the gate can never fire.

### JSON (`--format json`)

When a policy resolved, the report gains a `policy_verdict` block — the flagship machine-facing
surface:

```json
{
  "policy_verdict": {
    "decision": "block",
    "hits": [
      { "rule_id": "pii-outside-allowlist", "decision": "block",
        "change_model": "dim_account_holders", "change_column": "email",
        "matched_reach": ["account_holders"], "actions": ["block", "notify"] }
    ],
    "build_set": ["dim_accounts"],
    "test_set": ["fact_revenue"],
    "notifications": [
      { "channel": "slack", "target": "#data-governance", "message": "PII …" }
    ],
    "evaluated_rules": 4,
    "fired_rules": 2
  }
}
```

### Markdown / PR comment

When a policy is present, the report adds a **Policy verdict** section — fired rules grouped by
decision (block first), each naming the subject change and the reach it matched, followed by the
**selective build set / test set** and the notify intents. It deliberately references node
*names* only; it never re-lists the downstream blast radius (the impact section above it owns
that). A `block` verdict leads with a one-line **"blocked until…"** note stating how the block
clears (see [above](#a-block-is-a-block-until-not-a-dead-end)), so the PR comment carries the
release path, not just the obstacle.

## In CI (GitHub Action)

The composite action exposes a `policy` input and policy outputs:

```yaml
- uses: Fszta/dbt-column-lineage@v0
  with:
    manifest: artifacts/head/manifest.json
    catalog: artifacts/head/catalog.json
    base-manifest: artifacts/base/manifest.json
    base-catalog: artifacts/base/catalog.json
    policy: policy.yml
    fail-on: policy         # block the PR when the verdict is block
```

Outputs for downstream steps: `policy_decision` (`block`/`warn`/`allow`), `build_set_size`,
`test_set_size` — for example, feed the build set into a selective `dbt build`.
