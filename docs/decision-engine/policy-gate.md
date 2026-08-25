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

!!! tip "Scaffold the policy — don't start from a blank file"
    `parrant policy init` reads your manifest + catalog and writes a heavily-commented,
    safe-by-construction `./parrant.policy.yml` keyed only to signals it confirmed exist,
    so it runs green on day one. Start there rather than authoring from scratch — see
    [`policy init`](#scaffold-a-starter-policy-policy-init) below and the
    [Policy recipes](policy-recipes.md) page for the blessed copy-paste rules it emits.

Author (or scaffold) a `policy.yml`, then gate on it:

```bash
parrant impact \
  --manifest target/manifest.json --catalog target/catalog.json \
  --base-manifest base/manifest.json --base-catalog base/catalog.json \
  --policy policy.yml \
  --fail-on policy
```

- `--policy policy.yml` resolves the rules and attaches a `policy_verdict` to the report.
- `--fail-on policy` makes the check **exit 1** when the verdict is `block` (and only `block`).

**Resolution order** (first found): the explicit `--policy PATH`, then a
`./parrant.policy.yml` in the working directory. No policy found → the engine is inert
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
  on_meaning_changed: block      # block | warn | allow — gate a proven meaning change (optional)
  on_indeterminate: warn         # block | warn | allow — gate an unprovable change  (optional)
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

### Semantic-severity defaults (no rules needed)

`on_meaning_changed` and `on_indeterminate` are built-in knobs that gate the **semantic axis**
directly — no hand-written rule required. Each maps a column's classification straight to a gate
contribution:

| Knob | Fires on | Values |
|---|---|---|
| `on_meaning_changed` | a column the AST diff proved *changed meaning* (`semantic: meaning_changed`) | `block` · `warn` · `allow` |
| `on_indeterminate` | a column that *could not be proven safe* (`semantic: indeterminate`) | `block` · `warn` · `allow` |

They exist to be tuned **independently**: a proven-breaking change need not carry the same weight
as could-not-prove-safe. The common shape is *block the proven one, warn the unprovable one*:

```yaml
version: 1
defaults:
  on_meaning_changed: block
  on_indeterminate: warn
# no rules: — the gate is driven entirely by the two knobs
```

Both default to unset, so a policy that omits them behaves exactly as before. `allow` (or leaving
a knob unset) contributes nothing. Structural add/remove/type changes carry no semantic and are
untouched by these knobs — they're governed by provable breaks and your rules. A knob contribution
folds into the same [most-severe-wins](#how-multiple-rules-combine-into-one-verdict) combination as
every rule (appearing in the verdict as `builtin:on_meaning_changed` / `builtin:on_indeterminate`),
so a rule can only ever raise the decision, never lower one.

!!! note "Why not block `meaning_changed` by default?"
    The no-policy gate deliberately keeps `block` reserved for *provable* test breaks, because the
    conservative canonicalizer can over-classify a redundant-paren or commutative-reorder rewrite as
    `meaning_changed`. These knobs are the opt-in for teams that have decided a meaning change should
    hard-block in their project.

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

A leaf condition matches on exactly one of **five axes**:

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

#### `inferred_meta` — a meta key resolved by folding UPSTREAM lineage { #inferred-meta-conditions }

Same shape as `meta` (`key` / `op` / `value`), but the value is *inferred* from the column's
**lineage** rather than read only from its own declared meta — so a classification declared
**once** (e.g. `pii: true` on a staging column) is inherited by every downstream column that
derives from it, without re-tagging each one.

```yaml
inferred_meta: { key: pii, op: is_true }        # true if PII anywhere upstream (unless declassified)
inferred_meta: { key: secret, op: is_true }
```

Resolution, per column:

1. **Own meta wins.** If the column declares its own value for the key, that value is used
   verbatim — this is the seed, the override, and the **declassification** point (a downstream
   `pii: false` on a hashed/masked column stops propagation). This is a **column-level** notion:
   only a column's own meta seeds or declassifies `inferred_meta.*` — a **model-level** tag does not.
   (Seed your sources at column grain.)
2. **Otherwise fold the upstream source columns**, combining **most-restrictively** per a
   key-specific strategy:
    - `pii` — ordered `true > unknown > false`: any upstream `true` ⇒ `true`; else any
      unresolved upstream ⇒ `unknown`; else `false`.
    - `secret` — boolean **OR** over upstream (any upstream `true` ⇒ `true`); an own `false`
      still wins by rule 1.
    - Any **other** key falls back to the most-restrictive (`pii`-style) fold.
3. **Otherwise** — no own meta *and* no resolvable upstream (a root/source column that was never
   seeded, or a chain that never reaches a declared value) — the value is **UNKNOWN**.

An **UNKNOWN** inferred value is treated exactly like a missing plain `meta` key: it is routed
to [`on_missing_meta`](#fail-safe-defaults). This differs from `meta.*`, where `is_true` /
`is_false` on an absent key read as `False`: for `inferred_meta.*`, an unprovable classification is
UNKNOWN for **every** operator (including `is_true`), so "we could not prove this column is *not*
PII" is never silently read as passing. `inferred_meta` is evaluated at **column** grain, so in a
`reach.where` only a `reach.kind: column` object resolves; a reached model/exposure is UNKNOWN.

`inferred_meta.*` is purely additive — `meta.*` is unchanged and still reads only the node's own
declared meta.

##### Rolling `inferred_meta.*` out — advisory by default { #inferred-meta-rollout }

Introduce `inferred_meta.*` rules with **`on_missing_meta: skip`** (advisory) — *not* to block.
Because an un-seeded column folds to UNKNOWN, a rule under `skip` simply drops for the columns
whose classification cannot yet be proven (reported in `skipped_missing_meta`) while still
firing on the ones it *can* prove. That lets a rule roll out across teams that have not finished
documenting their meta **without failing their builds** — it warns/advises where it has signal
and stays quiet where it doesn't.

Making an `inferred_meta.pii is_true` rule **blocking** is a deliberate opt-in, and it only behaves
sensibly once the sources are **seeded** at column grain. Without seeds nearly every column
folds to UNKNOWN, and under `fail_closed` a blocking rule fires on almost all of them — which is
the fail-safe direction, but not a useful gate. So: seed your `pii` / `secret` sources first,
run the rule advisory (`skip`) to see where the signal lands, and only then promote it to
blocking. UNKNOWN always resolves via the fail-safe knob — there is no separate "block
everything" mode.

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

!!! tip "See what fail-safe would *actually* do before you arm it — `policy test`"
    Fail-safe semantics are impossible to verify by reading the YAML: the same naive
    `meta.pii is_true` guard can block every changed column under `fail_closed` and silently
    allow everything under `skip`. **[`policy test`](#backtest-a-policy-before-you-arm-it-policy-test)**
    replays a candidate policy over your git history and reports, per rule, how many firings were
    driven by a fail-safe UNKNOWN rather than a proven match — so you never arm a rage-blocking or
    silently-dead rule across a large repo.

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

## Scaffold a starter policy — `policy init`

Authoring three-valued fail-safe YAML from a blank file is the adoption cliff, and a naive starter
makes it worse (a value-comparison block rule on a `meta` key most of your models lack
[rage-blocks every PR](#fail-safe-defaults) under `fail_closed`). `policy init` removes the blank
page: it introspects your manifest + catalog and writes a heavily-commented,
**safe-by-construction** starter policy keyed *only* to signals the scan confirmed exist.

```bash
parrant policy init \
  --manifest target/manifest.json --catalog target/catalog.json
# → wrote ./parrant.policy.yml
```

The default output path is `./parrant.policy.yml` — exactly the path
[`impact` and `policy test` auto-resolve](#quick-start), so the generated file is picked up with no
`--policy` flag. **The file is yours** — it is code scaffolded into your repo, not a tool-managed
preset that can change under you. Edit it freely.

### What it emits

| Section | Behaviour |
|---|---|
| Header | Ownership note, a pointer to run [`policy test`](#backtest-a-policy-before-you-arm-it-policy-test) before arming anything, and the two footguns (presence vs. value operators; the fail-safe glossary) stated plainly. |
| `defaults` | `on_missing_meta: fail_closed`, `on_error: fail_closed` — the safe closed posture, emitted with the explanatory comment. |
| `provable-break-block` | Emitted **enabled** *iff* the scan found column-targeted dbt tests. This is a pure structural fact (never `UNKNOWN`), so it is safe to block on day one — it can only fire on a real breakage. Emitted commented, with the reason, when no tests are found. |
| `exposure-guard` | Emitted **enabled** (`warn`) *iff* the scan found exposures. Emitted commented otherwise. |
| Meta templates | For every dbt-`meta` key the scan actually found, a **commented** `warn` template using the presence operator `is_true`, prefixed with the key's **real coverage** (`present on N/total models (P%)`). Never enabled, never a block, never a value comparison — so uncommenting one can never rage-block. |
| Footer | Next steps: run `policy test`, uncomment a template once its coverage is useful, and promote `warn` → `block` only after the backtest proves it fires on real matches. |

The enabled rules are **tool-owned structural signals only** — facts the pipeline computes that can
never silently no-op. Meta-keyed rules always ship commented, because the tool never guesses your
governance intent. If the manifest has neither tests nor exposures, `policy init` emits a valid file
with `rules: []` and an honest note that nothing could be safely auto-enabled — an honest empty
policy, never a fake guard.

!!! danger "The scaffold never emits a fail-open default"
    `policy init` only ever writes the safe `fail_closed` posture — a gate that opens whenever it is
    unsure is not a gate. The permissive fail-safe mode is described in the header glossary but is
    deliberately never written as a copy-pasteable value.

### Options

| Option | Default | Purpose |
|---|---|---|
| `--manifest <path>` | `target/manifest.json` | the dbt manifest to scan (run dbt first). |
| `--catalog <path>` | `target/catalog.json` | the dbt catalog to scan. |
| `--adapter <dialect>` | auto-detected | override the sqlglot dialect (e.g. `tsql`, `snowflake`, `bigquery`). |
| `-o`, `--output <path>` | `./parrant.policy.yml` | where to write the generated policy (the path `load_policy` auto-resolves). |
| `--force` | off | overwrite an existing file at `--output` instead of refusing. |
| `--stdout` | off | print the generated policy to stdout instead of writing a file (never touches disk). |

`policy init` **refuses to overwrite** an existing policy without `--force`, so a hand-edited file is
never silently clobbered; `--stdout` prints without touching the filesystem at all.

The generated file arrives **green** — the enabled rules are safe-by-construction — but the meta
templates are yours to arm. Once you have it, replay it with
[`policy test`](#backtest-a-policy-before-you-arm-it-policy-test) and reach for the blessed
[Policy recipes](policy-recipes.md) when you want to graft in a rule the scan couldn't auto-enable.

## Backtest a policy before you arm it — `policy test`

A policy's fail-safe behaviour is unverifiable by inspection (see the [tip above](#fail-safe-defaults)),
and a rule that never fires is a silent no-op. `policy test` makes both visible **offline**: it
replays a candidate policy over your recent history and reports, per rule, what the gate *would*
have ruled — before you make it a required check.

```bash
parrant policy test \
  --manifest target/manifest.json --catalog target/catalog.json \
  --policy policy.yml \
  --last 30                                    # replay the last 30 commits
```

It is deterministic, offline, and **sequential** — no dbt run, no warehouse, no credentials. The
head registry is built **once** (the one meaningful cost) and reused for every point, so replaying
30–50 commits is cheap; per-point progress (`replaying i/N <ref>`) streams to stderr so a long run
is observably alive.

!!! note "`policy test` is a subcommand, not a flag"
    `parrant policy test …` is dispatched on the leading `policy` word. It is entirely
    separate from the **`--policy`** *option* on `parrant impact` (which gates a single
    live PR) — the two never shadow each other.

### Choosing what to replay

Supply **exactly one** change source:

| Source | Meaning |
|---|---|
| `--git-range <base>..<head>` | replay each commit in the range as one changeset (default head = `HEAD`). On squash-merge repos one commit ≈ one PR. |
| `--last <N>` | sugar for `--git-range HEAD~N..HEAD` — the last `N` commits. |
| `--changesets <dir>` | replay a directory of saved changeset JSONs (fixtures / a CI corpus). |

Other options:

| Option | Default | Purpose |
|---|---|---|
| `--policy <path>` | *(required)* | the candidate policy to backtest. A present-but-invalid file **fails loudly** — never treated as "no policy". |
| `--manifest` / `--catalog` | `target/manifest.json` / `target/catalog.json` | the **head** artifacts — the registry replayed against every point. |
| `--repo-dir <dir>` | current directory | the git repo for `--git-range` / `--last`. Independent of the manifest/catalog paths (which may live under `target/`). |
| `--adapter <dialect>` | auto-detected | override the sqlglot dialect. |
| `--format table\|json\|markdown` | `table` | `table` for terminals; `json` / `markdown` for a CI artifact or an agent over MCP. |
| `--fail-on none\|any-block\|regression` | `none` | exit-code gate — see [below](#gating-a-policy-change-pr-in-ci). |
| `--baseline <path>` | — | a saved prior backtest JSON to diff against (required by `--fail-on regression`). |

### The report — the trust instrument

The per-rule aggregate is the artifact you arm the gate on. Every `table` / `markdown` run leads
with a coverage-honest totals line — `N PR(s) replayed (E evaluated, K skipped) — B would BLOCK,
W would WARN; avg blast radius …` — so a low-coverage run is never mistaken for a clean pass, then
one row per rule:

| rule_id | would-BLOCK PRs | would-WARN PRs | fired-total | fail-safe UNKNOWN | flag |
|---|---|---|---|---|---|
| provable-break-block | 2 | – | 2 | 0 | |
| exposure-guard | – | 14 | 1315 | 0 | |
| pii-guard (naive) | **18** | – | 3169 | **3169** | **FAIL-SAFE ONLY (never a proven match)** |
| tier-guard | – | – | 0 | 0 | **DEAD (never fired)** |

Two columns carry the whole point:

- **fail-safe UNKNOWN** — firings that resolved via a fail-safe `UNKNOWN` (a blocking rule under
  `fail_closed` firing on an undecidable predicate) rather than a proven `TRUE` match. A rule that
  blocks *mostly* via fail-safe defaults is the **rage-block footgun**: it looks armed but is
  really just blocking everything it can't decide. The **flag** column calls it out — `FAIL-SAFE
  ONLY (never a proven match)` when every firing was fail-safe, or `partly fail-safe` when some
  were.
- **DEAD (never fired)** — a guard that never fired across the whole range is a silent no-op. This
  is usually the honest signal that the `meta` key it inspects **isn't populated** on your models
  ("your `pii-guard` matched 0 subjects across your last 50 PRs" → go tag your columns).

`--format json` emits the full typed `BacktestReport`: the totals, the `rule_stats` above, and a
per-point drill-down (`points[]`) carrying each commit's decision, blast radius, the rules it
fired, a `sample_reach`, plus `unmapped_changes` and any per-point `parse_failures` — coverage
honesty, never hidden.

!!! danger "What a git-diff backtest does *not* prove (the fidelity note)"
    Every run prints a **fidelity note**, and it is deliberately blunt. In the default git-diff
    mode the tool diffs whole `.sql` files, so **every change is classified `indeterminate` by
    construction** and the head registry is HEAD's replayed against older diffs (models added or
    renamed since a commit surface as `unmapped_changes`). Rules keyed on reach / `meta` /
    `change.kind` / `on_indeterminate` fire normally and **can block**. What this version does
    **not** exercise: the **provable-break** block tier and the **semantic meaning-change** block
    tier — both need a per-commit before-state. The note says so plainly; per-commit base-manifest
    validation of those tiers ships in a later release. In `--changesets` mode the note is
    branched too: the provable-break tier is still unexercised, and a `meaning_changed` block fires
    only if a saved changeset already carried that classification.

### Gating a policy-change PR in CI

Because the backtest itself has an exit code, `policy test` can gate the PR that *changes the
policy* — a policy that would newly block N historical PRs is a regression worth a human look.

| `--fail-on` | Exit 1 when… |
|---|---|
| `none` (default) | never — report only. |
| `any-block` | any replayed PR would block under the candidate policy. |
| `regression` | any rule's would-BLOCK count **rose** versus `--baseline`. |

`--fail-on regression` **requires** `--baseline`; running it without one exits 1 with a clear
message rather than silently passing (a regression gate with no baseline validates nothing), and a
malformed `--baseline` file is rejected loudly. Capture a baseline by saving a `--format json` run,
then compare later runs against it:

```bash
parrant policy test --policy policy.yml --last 50 --format json > baseline.json
# … on the policy-change PR:
parrant policy test --policy policy.yml --last 50 \
  --fail-on regression --baseline baseline.json
```

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

!!! note "The audited release path: an in-code override"
    Beyond self-clearing, there is one supported way to *acknowledge* a fired verdict without
    changing the code that tripped it: an **[override pragma](#overriding-a-verdict-the-in-code-escape-hatch)**
    (`-- lineage:allow-change` / `-- lineage:allow-break`). It lives in the PR's own SQL, so it
    stays offline / zero-credential and is diffed and reviewed like any code — and it only ever
    *lowers* severity (it is a post-evaluation cap, not a rule; a `warn` action still can never
    cancel a `block` action inside the engine — see
    [how rules combine](#how-multiple-rules-combine-into-one-verdict)).

    Two *external-input* release paths remain deliberately unbuilt: **block-until-acknowledged**
    (an owner signs off via a PR label the gate honors) and **block-until-proven** (a `/data-diff`
    result). Both would require the gate to consume a new external input, cutting against the
    offline / zero-credential guarantee — noted as possible future work, **not** available today.

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
        "matched_reach": ["account_holders"], "actions": ["block", "notify"],
        "fired_on_unknown": false, "unknown_cause": null }
    ],
    "build_set": ["dim_accounts"],
    "test_set": ["fact_revenue"],
    "notifications": [
      { "channel": "slack", "target": "#data-governance", "message": "PII …" }
    ],
    "evaluated_rules": 4,
    "fired_rules": 2,
    "unresolved_reach_count": 0,
    "skipped_missing_meta": 0
  }
}
```

Each hit carries `fired_on_unknown` (with `unknown_cause` — `missing` / `error`): `true` means the
rule fired because a [fail-safe knob](#fail-safe-defaults) resolved an **UNKNOWN**, not because it
proved a match. The verdict-level `unresolved_reach_count` / `skipped_missing_meta` count what the
policy left **undecided**. Both are surfaced in the PR comment (below) so a fail-safe block never
reads as a proven one.

### Markdown / PR comment

When a policy is present, the report adds a **Policy verdict** section — a **"Why this verdict"**
breakdown of the rules that fired, grouped by decision (block first). Each row names the subject
change, a capped sample of the reach it matched, and — the load-bearing bit — an **honesty
marker**:

- `✓ proven match` — the rule proved its predicate against real data.
- `⚠️ **fired on a fail-safe default** (meta missing / evaluation error)` — the rule fired only
  because a [fail-safe knob](#fail-safe-defaults) resolved an **UNKNOWN**. A fail-safe block reads
  very differently from a proven one, on purpose: a confident-looking ruling that was actually
  *undecided* is exactly the trust this gate is sold on.

When an [override](#overriding-a-verdict--the-in-code-escape-hatch) capped a hit, the row shows the
`original → effective (overridden: reason)` delta so the audit trail is visible. Below the rules
come the **selective build set / test set** and the notify intents.

Whenever the policy left anything **undecided**, a LOUD **Coverage** footer states it plainly —
`Coverage: N columns undecided (missing meta), M reaches unresolved — these did NOT count as
safe.` — so a fail-closed block driven by unknowns is never folded into a clean pass.

The section deliberately references node *names* only; it never re-lists the downstream blast radius
(the impact section above it owns that). A `block` verdict leads with a one-line **"blocked until…"**
note stating how the block clears (see [above](#a-block-is-a-block-until-not-a-dead-end)), so the PR
comment carries the release path, not just the obstacle.

The **default gate** (no policy) explains itself too: each flagged column carries the compact
semantic reason it was flagged, alongside the provable-break diagnostics. `impact --explain`
*expands* the human comment with the `base → head` expression trace; the JSON always carries the
full explanation regardless of the flag.

## Overriding a verdict — the in-code escape hatch

A gate with no escape hatch gets turned off, not tuned. The moment the verdict flags a change the
author knows is fine ("yes, this recomputes downstream — it's intended, and downstream was updated
in the same PR"), the tempting move is to disarm the whole gate (`--fail-on none`). An **override
pragma** is the alternative: an inline SQL comment in the head model that acknowledges *one specific
change*, with a mandatory reason. It is **explicit, in the diff, reviewed like any other code, and
logged** — never a silent global off-switch.

```sql
select
  -- lineage:allow-change reason="renamed from amount_cents; all downstream refs updated in this PR"
  amount as amount_eur,
  ...
```

Two verbs, deliberately distinct so a soft acknowledgement can **never** silence a hard break:

| Pragma | What it downgrades | Can it touch a provable break? |
|---|---|---|
| `-- lineage:allow-change [column=<col>] reason="…"` | a REVIEW / WARN contribution for that column to *allow* (the common case). | **No** — never. |
| `-- lineage:allow-break [column=<col>] reason="…"` | a provable BLOCK — the only verb that can — down to REVIEW (no-policy gate) or WARN (policy gate). | Yes, and only that far — **never to safe**. A provably-broken test always leaves a visible mark + audit record. |

`reason=` is **mandatory**: an override with no non-empty reason is dropped and warned (an override
that isn't justified has no audit value).

### Scope resolution

Which changed column a pragma excuses, in priority order:

1. an explicit `column=<name>` argument, else
2. the column whose SELECT expression the comment is attached to / immediately precedes, else
3. **model scope** — a pragma placed above the model's first statement with no `column=` arg excuses
   every changed column in that model (logged as a model-level override, so the broader reach is
   visible).

Pragmas are **head-only** by design: the override lives in the PR's own SQL, so it is diffable and
reviewed like any code. Source lines in the report are relative to the **compiled** SQL.

!!! danger "Fail-safe invariants (non-negotiable — this is a gate)"
    - An override only ever **lowers** severity, never raises it.
    - `allow-change` **cannot** touch a provable BLOCK — only `allow-break` can, and only
      BLOCK→REVIEW/WARN, never →safe.
    - On the same column, `allow-break` **wins** over `allow-change` (hard beats soft).
    - A malformed / reasonless / unknown-verb pragma is **dropped with a loud warning** and never
      changes the ruling silently.
    - On a rename, adjacency resolves to the *new* column name, so a break stays armed unless you
      name the old column explicitly (`column=<old_name>`) or use model scope. Overriding the wrong
      side is a no-op, not an accidental unblock.

### It moves `--fail-on tests` (the whole point)

An `allow-break` that acknowledges a provable break **excuses that break** from the gate: the
excused break drops out of `provable_breaks` / `provable_break_count`, so `--fail-on tests` (and
`--fail-on policy` on a provable-break rule) clears — exit 0 — while the acknowledgement stays
visible in the report. `allow-change` never excuses a break. Run with
[`--no-overrides`](#auditing-override-reliance) to see the raw gate the override cleared.

### Every honored override is logged

Nothing is ever silently suppressed. A honored override is surfaced on every surface:

- **JSON** — the impact report gains four blocks:

    | Block | Contents |
    |---|---|
    | `overrides` | honored overrides: `{model, column, verb, reason, downgraded_from, downgraded_to, source_line, scope}` |
    | `ineffective_overrides` | pragmas that landed on a *real* changed column but changed nothing, with a fix `hint` (e.g. `allow-break` on an added column) |
    | `stale_overrides` | pragmas with **no matching change** at all — dead excuses to prune |
    | `override_warnings` | dropped malformed / reasonless / unknown-verb pragmas (strings) |

    Under the policy engine, a capped `RuleHit` also carries `overridden: true`,
    `original_decision`, and `override_reason`, so the verdict says exactly which rule the override
    suppressed.

- **Markdown / PR comment** — an override section renders, in order: **⚠️ Override pragmas IGNORED**
  first and unfolded (a dropped pragma must be noticed), then **⚠️ Overrides applied** (each with
  its `from → to` delta and reason), **⚠️ Overrides with no effect** (ineffective, with the hint),
  and a folded **Stale overrides** list to prune. A capped policy hit reads
  *"override suppressed WARN (reason: …)"* inline.

- **Action** — the composite action emits an `overrides_applied` output (count of honored
  overrides). See [In CI](#in-ci-github-action).

### Auditing override reliance

`impact --no-overrides` evaluates the changeset **as if no pragma were present** — the raw gate.
Use it to answer "what would the gate say without the excuses?" and to measure override reliance
over history (a rising `allow-break` volume is a signal the gate is mis-tuned, not merely escaped).
With no pragma present, the output is byte-identical to the default run.

```bash
parrant impact \
  --manifest target/manifest.json --catalog target/catalog.json \
  --base-manifest base/manifest.json --base-catalog base/catalog.json \
  --fail-on tests --no-overrides        # ignore every -- lineage:allow-* pragma
```

## In CI (GitHub Action)

The composite action exposes a `policy` input and policy outputs:

```yaml
- uses: Fszta/parrant@v0
  with:
    manifest: artifacts/head/manifest.json
    catalog: artifacts/head/catalog.json
    base-manifest: artifacts/base/manifest.json
    base-catalog: artifacts/base/catalog.json
    policy: policy.yml
    fail-on: policy         # block the PR when the verdict is block
```

Outputs for downstream steps: `policy_decision` (`block`/`warn`/`allow`), `build_set_size`,
`test_set_size` — for example, feed the build set into a selective `dbt build`. The action also
emits `overrides_applied`: the number of honored override pragmas that lowered the ruling on this
run (always present; `0` when none fired).

To evaluate the **raw gate** in CI — ignoring every `-- lineage:allow-*` pragma, e.g. to backtest
override reliance — set the `no-overrides` input to `true` (threads through to `--no-overrides`).
