# Glossary — every term, pinned at every layer

A governance gate is only trustworthy if every label means exactly one thing, and means the
same thing in the code, the CLI/JSON, the explorer UI, and these docs. This page is the single
source of truth for that vocabulary. Each term lists its **code enum** (the value you write in
`policy.yml` or read in JSON), how it reads in the **UI**, and a precise definition.

The chain the vocabulary describes, in order:

```
 change kind ──► semantic class ──► breaking? ──► reach (kind × mechanism, precision) ──► policy action ──► gate decision
 (what changed)  (did output move?) (fail-safe)   (what it touches, how)                  (rule effect)     (the ruling)
```

---

## 1. Change kinds — *what* changed about a column

**Code:** `ChangeKind` (`dbt_column_lineage/lineage/changeset.py`) · **JSON:** `by_change[].kind`,
`change.kind` in a policy predicate.

| Value (`kind`) | Definition | Breaking by construction? |
|---|---|---|
| `removed` | The column existed in the base and is gone in the head. | **Yes** — always. |
| `type_changed` | The column's data type changed (from `catalog.json`). | **Yes** — always. |
| `logic_changed` | The column still exists with the same type, but its defining SQL expression changed. | **Only if its [semantic class](#2-semantic-classes-did-the-output-actually-move) is not `equivalent`.** |
| `added` | The column is new in the head (absent in the base). | **No** — a new column breaks nothing downstream. |

Ranked by blast-radius risk `removed > type_changed > logic_changed > added`. When one column is
touched several ways, the **highest-ranked** kind is reported (`ChangeKind.priority`).

Only `logic_changed` carries a [semantic class](#2-semantic-classes-did-the-output-actually-move);
the other three encode their breaking-ness in the kind itself, so their `semantic` is `null`.

---

## 2. Semantic classes — did the output *actually* move?

**Code:** `SemanticChangeKind` (`models/schema.py`), computed in `semantic_diff.py` · **JSON:**
`by_change[].semantic`, `change.semantic` in a policy predicate.

The AST-diff engine compares the base and head SQL *expressions* for a `logic_changed` column and
classifies the relationship. It answers **"does the column's output change?"** — not "did the
source text change?" (a reformat or a provable simplification is reported `equivalent`).

| Value (`semantic`) | Definition | Breaking? |
|---|---|---|
| `equivalent` | The two expressions are **provably** the same output (canonicalization / safe simplification proved it). Cosmetic-only. | **No** |
| `meaning_changed` | The expression's meaning **provably** changed — downstream values may differ. | **Yes** |
| `indeterminate` | The engine **could not prove** either way (unparseable SQL, a comparison it can't make type-safely). | **Yes** (fail-safe) |

!!! info "This is expression classification, not value diffing"
    The engine classifies the *expression*. Whether the values *actually* differ on real
    warehouse data is a different job (a data-diff tool). See the guardrails in the
    [overview](overview.md#what-has-not-changed-the-guardrails).

### `breaking` — the derived, fail-safe convenience

**Code:** `SemanticChangeKind.is_breaking` · **JSON/policy:** `change.breaking` (boolean).

`breaking` is **not** a stored class — it is derived: **everything except a proven `equivalent`
is breaking.** So `meaning_changed`, `indeterminate`, and an *absent* semantic all fold into
`breaking = true`. This is the fail-safe rule of the whole product: *anything not proven safe is
treated as breaking.*

!!! warning "Gate on `change.breaking`, not `change.semantic == …`"
    `change.semantic eq meaning_changed` resolves to a plain `false` for an `indeterminate`/absent
    semantic — it does **not** fold in the unproven case. To catch "anything possibly breaking,"
    always gate on **`change.breaking is_true`**. (See the [policy guide](policy-gate.md#fail-safe-defaults).)

In the **explorer**, the subject column's `breaking` renders as one of three chips: *Breaking
change* (`meaning_changed`), *Breaking — unproven* (`indeterminate`), or *Proven equivalent*
(`equivalent`).

---

## 3. Reach — *what* a change touches, and *how*

"Reach" is the set of downstream objects a change propagates to. A policy `reach` condition is
quantified: *"does this change reach an object of `kind` (optionally via `mechanism`) whose own
`meta` satisfies an inner predicate?"*

### 3.1 Reach kinds — the *type* of downstream object

**Code:** `ReachKind` (`models/schema.py`) · **JSON/policy:** `reach.kind`.

| Value (`kind`) | The downstream object |
|---|---|
| `model` | A dbt model the change reaches. |
| `column` | A specific downstream model column. |
| `exposure` | A dbt exposure **or a BI dashboard** (dashboards surface as exposure-kind reach; Metabase is the first supported connector — see [cross-boundary](metabase.md)). |

### 3.2 Mechanisms — *how* the change propagates

**Code:** `Mechanism` (`models/schema.py`), mapped from a column's `transformation_type` by
`_MECHANISM_LABELS` (`service.py`) · **JSON/policy:** `reach.mechanism`, `add-to-build-set`'s
`mechanism` filter.

This is the **"recompute vs pass-through"** distinction that powers *selective* rebuilds.

| Value (`mechanism`) | From `transformation_type` | Meaning |
|---|---|---|
| `derived_recompute` | `derived` | The downstream value is **recomputed** from the changed column (an aggregation, a CASE, arithmetic). |
| `rowset_filter` | `filter` | The changed column is used in a **filter/join/QUALIFY** — it shapes the row set without projecting its value. |
| `renamed_passthrough` | `renamed` | The value passes through under a **new name**. |
| `direct_passthrough` | `direct` | The value passes through **unchanged**. |

An unrecognized type is bucketed under its raw value (never silently dropped).

### 3.3 Reach precision (cross-boundary) — *how exactly* a BI dashboard is reached

> These fields are named for the current connector (**Metabase**), but the concept is
> connector-agnostic — see [Cross-boundary](metabase.md).

**Code:** `MetabaseCard.precision` (`models/schema.py`) · **JSON:** an exposure entry's
`precision` · **UI:** *column-precise* / *table-level* captions on the dashboard card.

| Value (`precision`) | Definition |
|---|---|
| `column` | The exact warehouse column the card reads is known — the reach is column-precise. |
| `table` | Only the *table* the card reads is known (native `select *`, complex SQL) — a valid dashboard-reach signal, but not column-precise. |
| `none` | No warehouse relation resolved at all — counted, never guessed. |

A column-precise dashboard entry carries **`via_columns`** (the changed dbt column → card field
chain: `model`, `column`, `card_id`, `role`) and **`via_cards`** (the card ids it was reached
through). A `table`-grain reach carries `via_cards` but an empty `via_columns` — honest: reached,
but which column is unproven. See the [cross-boundary guide](metabase.md).

---

## 4. Policy actions — what a *fired rule* contributes

**Code:** `ActionKind` (`models/schema.py`) · **JSON/policy:** an `action`'s `type`.

An action is the effect a rule adds to the verdict when its predicate matches. A rule may emit
several; effects **accumulate** across all fired rules.

| Value (`type`) | Effect |
|---|---|
| `block` | Contributes `block` to the [gate decision](#5-gate-decisions-the-ruling) (most-severe-wins). |
| `warn` | Contributes `warn` — advisory; never causes a non-zero exit. |
| `add-to-build-set` | Adds the reached (or subject) models to the selective **build set** (`dbt build --select …`). |
| `add-to-test-set` | Adds the reached models (or their tests) to the selective **test set**. |
| `notify` | Appends a notification intent (`channel`, `target`, `message`) for your CI to route. |

> **Action ≠ decision.** `block`/`warn` are *actions a rule emits*; the gate *decision* below is
> the single ruling those actions combine into. The two share the words "block"/"warn" on
> purpose — an action named `block` is exactly the thing that drives a `block` decision.

---

## 5. Gate decisions — the *ruling*

**Code:** `GateDecision` (`models/schema.py`) · **JSON:** `policy_verdict.decision`, each
`hits[].decision` · **UI:** the `POLICY: BLOCK/WARN/ALLOW` banner and panel.

The single ruling the engine emits for the whole change, combined **most-severe-wins**
(`block > warn > allow`) across every fired rule.

| Value (`decision`) | Severity | Meaning | Exit under `--fail-on policy` |
|---|---|---|---|
| `block` | 2 | At least one `block` action fired. | **Exit 1** |
| `warn` | 1 | No `block`, but at least one `warn` fired. Advisory. | Exit 0 |
| `allow` | 0 | No rule fired (or only non-decision actions). The gate passes. | Exit 0 |

!!! note "The legacy verdict is a different axis"
    Without a `--policy`, the tool falls back to its original heuristic verdict —
    `safe` / `review` / `block` (`verdict.decide_verdict`). That is a **separate** three-value
    ruling for the no-policy path. The policy engine's `block`/`warn`/`allow` above is the
    metadata-agnostic replacement; the two never mix in one report. (See
    [backward compatibility](overview.md#backward-compatibility).)

---

## 6. Fail-safe knobs — how *undecidable* inputs resolve

**Code:** `MissingMetaPolicy` (`models/schema.py`) · **JSON/policy:** `defaults.on_missing_meta`,
`defaults.on_error`, and the per-rule overrides.

| Value | On a missing meta key / unresolved reach |
|---|---|
| `fail_closed` (default) | For a *blocking* rule, an undecidable leaf resolves toward "unknown = risk = fire." The gate over-blocks rather than silently passing. |
| `fail_open` | An undecidable leaf resolves `false` — the rule can't fire on absence. For "act only on explicitly-tagged nodes." |
| `skip` | The rule is skipped for that subject and recorded as `skipped_missing_meta` (honesty counter). |

`on_missing_meta` governs a *missing key / unresolved reach*; `on_error` governs an
*operator/type mismatch*. Full semantics: [policy guide → fail-safe defaults](policy-gate.md#fail-safe-defaults).

---

## 7. Operators — how a `meta` / `change` condition matches

**Code:** `Operator` (`models/schema.py`) · **JSON/policy:** a leaf condition's `op`.

| `op` | Meaning | Value type |
|---|---|---|
| `exists` / `absent` | key present / absent | — |
| `is_true` / `is_false` | truthy / falsy | — |
| `eq` / `ne` | scalar equality | scalar |
| `in` / `not_in` | scalar ∈ / ∉ a list | list |
| `matches` | regex full-match (strings) | string (regex) |
| `intersects` | list shares ≥1 element with the given list | list |
| `subset_of` / `not_subset_of` / `superset_of` | list containment | list |
| `gt` / `ge` / `lt` / `le` | numeric comparison | number |

---

## 8. Confidence & coverage — can I *trust* the answer is complete?

Orthogonal to every ruling above: honesty signals about the inputs, so a fail-safe block driven
by unknowns reads as such rather than as fabricated certainty.

| Term | Code | Values | Meaning |
|---|---|---|---|
| Coverage `complete` | `Coverage.complete` (`models/schema.py`) | `true` / `false` | Whether every project model was parsed/cataloged, i.e. the DAG was fully analyzable. |
| Impact confidence `level` | `ImpactConfidence.level` | `full` / `partial` | `full` = the impact list is a complete accounting; `partial` = a **lower bound** (some reachable downstream models couldn't be analyzed at the column level). |
| Metabase reach `level` | `MetabaseReachConfidence.level` | `full` / `partial` / `absent` | Trust in the appended dashboard reach: snapshot staleness × column-precise vs table-grain resolution. `absent` = no snapshot. |
| `stale` | `MetabaseReachConfidence.stale` | `true` / `false` | The Metabase snapshot is older than the freshness threshold (or absent). Reach is reported as **degraded**, never fabricated. |

---

## See also

- [Overview](overview.md) — how the layers fit together.
- [Semantic categorization](semantic-categorization.md) — the AST-diff engine (§1–2).
- [Policy gate](policy-gate.md) — rules, actions, decisions, fail-safe (§3–7).
- [Cross-boundary (Metabase)](metabase.md) — reach kinds and precision (§3).
