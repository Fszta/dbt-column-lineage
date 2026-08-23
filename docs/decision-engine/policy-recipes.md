# Policy recipes

Blessed, copy-paste starter policies for the [policy gate](policy-gate.md). Each is
**safe-by-construction**: it uses a presence operator (`is_true` / `exists`), so it never
rage-blocks on a `meta` key your models don't carry.

!!! tip "Start with `policy init`, not a blank file"
    ```bash
    dbt-col-lineage policy init --manifest target/manifest.json --catalog target/catalog.json
    ```
    `policy init` reads your manifest + catalog and writes a heavily-commented
    `./dbt-col-lineage.policy.yml` keyed only to signals it confirmed exist — the structural
    rules below arrive enabled, and every dbt-`meta` key you actually use arrives as a commented
    template prefixed with its real coverage. This page is the reference for the recipes it emits
    and a few more you can graft in. The generated file is **yours** — edit it freely.

!!! danger "Run `policy test` before you arm a `block`"
    Before you flip any rule from `warn` to `block` — or uncomment a `meta`-keyed template —
    replay it over your recent history:
    ```bash
    dbt-col-lineage policy test --last 20 --policy dbt-col-lineage.policy.yml
    ```
    This shows what each rule **would** have done and, the headline column, how many firings were
    driven by a fail-safe `UNKNOWN` rather than a proven match. A rule that blocks mostly via
    fail-safe defaults is the [rage-block footgun](policy-gate.md) — catch it here, not in CI.

## The footgun, in one paragraph

Under the default `on_missing_meta: fail_closed`, a **value comparison** (`eq`, `in`, `gt`,
`matches`, …) on a key a model *doesn't declare* resolves `UNKNOWN`, and a blocking rule then
fires on **everything** that lacks the key. A **presence operator** (`is_true`, `is_false`,
`exists`, `absent`) is *total*: it resolves `FALSE` on a missing key, so the rule simply doesn't
match. **Every recipe here uses a presence operator.** Reach for a value comparison only after
`policy test` proves your key coverage.

---

## 1. Block provable test breaks

The day-1 governance win, and the only tier objective enough to block on sight: a change that
removes or renames a column a dbt test targets **will fail the next `dbt build`**. This is a
proven structural fact (never `UNKNOWN`), so blocking on it can only ever fire on a real breakage.

```yaml
version: 1
defaults:
  on_missing_meta: fail_closed
  on_error: fail_closed
rules:
  - id: provable-break-block
    scope: aggregate
    predicate: { structural: { fact: provable_test_break } }
    action: [{ type: block }]
```

## 2. Warn (and notify) when a change reaches an exposure

Flag any change that reaches a dbt exposure (a dashboard / downstream consumer) for human review —
without gating the merge. `warn`, not `block`: `touches_exposure` can be `UNKNOWN` on unresolved
reach, and a non-blocking rule never fires on `UNKNOWN`, so this never manufactures a spurious
warning.

```yaml
  - id: exposure-guard
    predicate: { structural: { fact: touches_exposure } }
    action:
      - { type: warn }
      - type: notify
        channel: slack
        target: "#analytics-eng"
        message: "{change.model}.{change.column} reaches {reach.count} exposure(s) — please review."
```

## 3. Critical-mart review

Warn when a change reaches a model your team flagged `critical`. This assumes a **boolean flag**
`meta: { critical: true }` on the model, so it uses `is_true`:

```yaml
  - id: critical-mart-guard
    predicate:
      reach:
        kind: model
        where: { meta: { key: critical, op: is_true } }
    action: [{ type: warn }]
```

!!! note "`is_true` vs `exists`"
    `is_true` matches only a model where `critical` is **present AND truthy**. A model without the
    key — *or* one that set `critical: false` — does **not** match. That is what you want for a
    flag: `exists` would also match `critical: false`, wrongly reporting the change as reaching a
    critical model. Use `exists` only for a key whose mere presence is the signal.

## 4. PII guard (the correctly-written one)

The rule most likely to be mis-written into a rage-block. A change to a column your team tagged
`pii` should get extra scrutiny. Tag PII columns with `meta: { pii: true }` and gate with the
**presence** operator `is_true` — **never** `eq true`:

```yaml
  - id: pii-change-guard
    predicate: { meta: { key: pii, op: is_true } }
    action:
      - { type: warn }
      - type: notify
        channel: slack
        target: "#data-governance"
        message: "PII column {change.model}.{change.column} changed — governance review required."
```

!!! danger "Why `is_true`, and why `policy test` first"
    `meta: { key: pii, op: eq, value: true }` looks equivalent but is **not**: on the ~92% of
    columns that never declared `pii` (see above), `eq` resolves `UNKNOWN`, and under
    `fail_closed` a `block` action then fires on **every** untagged column — it rage-blocks every
    PR. `is_true` resolves `FALSE` on those columns, so the rule targets *only* real PII. Even so,
    run `policy test --last 20` before promoting this to `block`, to confirm it fires only on the
    columns you expect.

---

## Promoting `warn` → `block`

Every meta-keyed recipe here ships as `warn`. Promote it to `block` only once
`policy test` shows, over your real history, that it fires **exclusively on proven matches** and
never via a fail-safe `UNKNOWN`. That evidence — not intuition — is the bar for arming a gate.
See the [policy gate guide](policy-gate.md) and the [glossary](glossary.md) for the full
operator / fail-safe reference.
