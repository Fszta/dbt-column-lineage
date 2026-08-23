# Things to know

A short list of subtleties that are easy to get wrong. Each one is intentional; internalizing them
is the difference between trusting the engine and fighting it. If you read only one page beyond
[How it works](how-it-works.md), read this one.

## 1. Gate on `change.breaking`, not an equality match on `semantic`

This is the single most important rule. The `semantic` field is always a *present* value, so a
policy condition matching `semantic eq meaning_changed` resolves to a plain **False** for an
`indeterminate` (or absent) semantic — it silently lets the unprovable case through.

`change.breaking` is the fail-safe roll-up: it is **true for everything that isn't a proven
`equivalent`** (so it folds in `meaning_changed`, `indeterminate`, *and* absent). To catch
everything possibly-breaking, always gate on:

```yaml
change: { field: breaking, op: is_true }
```

## 2. `equivalent` means provably-unchanged *output*, not unchanged *text*

Semantic categorization is an **output gate, not a source-text gate**. It canonicalizes each
expression before comparing, so a set of edits are correctly reported `equivalent` even though a
human sees the text change — boolean reordering (`a AND b` ↔ `b AND a`), de Morgan / idempotence,
constant folding to the same value, `BETWEEN` ↔ an equivalent range, type aliases (`int` ↔
`integer`), comments/whitespace. This is **intended and safe**: the value is provably unchanged, so
there is nothing to rebuild. Don't mistake an `equivalent` verdict for a missed change.

Conversely, a few edits look harmless but can't be proven equivalent *type-safely*, so the engine
errs to breaking — e.g. arithmetic reordering (`a + b` vs `b + a`, since floats/overflow don't
commute) and quoted-vs-unquoted identifiers on case-sensitive warehouses. That is the fail-safe
stance, not a bug.

## 3. Absent boolean meta reads as *False* — by design

`is_true` / `exists` on a **missing** key resolves to `False`. So a model that simply *forgot*
`critical: true` is **not** blocked by a rule keyed on `meta.critical is_true`. A metadata gate
can't police metadata it was never given; that "miss = blind" risk is one the consumer accepts.

If you want the opposite — *fail-closed on a missing key* — key the rule on a **value operator over
a reached object** instead. For example, "PII reaches a mart whose `readable_by` is **not a subset
of** the allowlist" blocks even when `readable_by` is absent, because a missing value resolves to
*unknown → risk → block*. Same engine, opposite behaviour, purely by operator choice.

## 4. Fail-closed vs fail-open: how undecidable inputs resolve

When a rule can't be decided (missing key, unresolved reach, operator/type mismatch), the outcome
is governed by `on_missing_meta` / `on_error`:

- **`fail_closed`** (default): for a *blocking* rule, an undecidable leaf biases toward
  "unknown = risk = fire". The gate over-blocks rather than passing a risky change.
- **`fail_open`**: an undecidable leaf resolves `False` — the rule only fires on explicitly-tagged
  nodes. (This is the pattern for "act **only** on Metabase dashboards", combined with a
  `source == metabase` guard.)
- **`skip`**: the rule is skipped for that subject and counted, for honesty.

The asymmetry is deliberate: **blocking rules bias toward firing; non-blocking rules bias toward
staying quiet** — the safety mechanism over-blocks but never manufactures spurious warnings.

## 5. Cross-boundary reach is only as good as its snapshot — and says so

Metabase reach comes from a point-in-time `metabase-extract` snapshot, so the engine is honest
about two things instead of fabricating certainty:

- **Precision** — a dashboard reached through a query-builder card is **column-precise** (the exact
  field is known); one reached through a `select *` native card is **table-grain** (reached, but the
  specific column is unproven). Both are real reach; only the second is imprecise, and it's labelled.
- **Staleness** — a snapshot older than the freshness threshold is flagged **stale** and reported
  as degraded, but it is **still used in full**: the reach is gated on, not discarded, so a block
  driven by an old snapshot is *explainable* rather than silently trusted-as-fresh. (Only an
  *absent* snapshot means dbt-only reach.) Dashboard reach is never fabricated.

So a fail-closed block driven by a stale or coarse snapshot reads *as such* — you can see why it
fired.

## 6. A block is a `block-until` — it clears itself

A `block` is an exit path, not a wall. The gate is stateless and re-runs on **every push**, so a
block lifts the moment the change stops tripping the rule — revert it, make it a proven-`equivalent`
refactor, evolve the downstream/schema to absorb it, or stop it reaching the flagged object. No
ticket or re-approval, and no external input. The verdict states how to clear it. Inside the engine
there is deliberately **no** priority/suppression between rules — a `warn` action can never cancel a
`block` action. The one audited way to *acknowledge* a fired verdict without changing the code is an
in-code [override pragma](../policy-gate.md#overriding-a-verdict-the-in-code-escape-hatch), which
lives in the PR's SQL and only ever lowers severity.

## 7. `block`/`warn` the *action* vs `block`/`warn` the *decision*

The same words appear on two levels. A rule emits **actions** (`block`, `warn`, `add-to-build-set`,
…); the engine combines all fired actions **most-severe-wins** into one **decision**
(`block` > `warn` > `allow`). An action named `block` is what *drives* a `block` decision — but the
decision is the single ruling, and it's what `--fail-on policy` gates on. The
[Glossary](../glossary.md#4-policy-actions-what-a-fired-rule-contributes) pins both.

---

Once these are second nature, the how-to guides ([Policy gate](../policy-gate.md),
[Semantic](../semantic-categorization.md), [Cross-boundary](../metabase.md)) are just mechanics.
