# Semantic categorization

When the tool diffs two dbt manifests, it doesn't just notice *that* a column's SQL changed —
it decides *whether the change matters*. Each `logic_changed` column carries a **semantic**
signal that answers one question:

> **Does the column's output change, or is the edit cosmetic?**

This is what lets the tool say *"this refactor is provably a no-op, don't block it"* — and,
just as importantly, *"I can't prove this is safe, so treat it as breaking."*

> These classes, and how `breaking` derives from them, are pinned in the
> [Glossary](glossary.md#2-semantic-classes-did-the-output-actually-move) alongside every other
> term in the pipeline.

## The three categories

| Semantic value | Meaning | Breaking? |
|---|---|---|
| `equivalent` | The new expression is **provably** the same as the old one. | No |
| `meaning_changed` | The expression's meaning changed — the value may differ downstream. | **Yes** |
| `indeterminate` | The tool could not prove equivalence (unparseable / ambiguous). | **Yes (fail-safe)** |

The derived `breaking` flag folds this down: **anything that is not `equivalent` is breaking.**
Structural changes (a column added, removed, or retyped) carry no semantic value — they're
categorized by their `kind`, not by expression comparison.

!!! warning "Gate on `breaking`, not on a specific semantic value"
    An equality match on `semantic` resolves `False` for an `indeterminate` (or absent) semantic,
    so it silently lets the unprovable case through. Always gate on `change.breaking` (which folds
    `indeterminate` and absent into breaking). This is the single most important fail-safe rule —
    see [Things to know §1](concepts/gotchas.md) and the [policy fail-safe section](policy-gate.md#fail-safe-defaults).

## It's an *output* gate, not a source-text gate

The categorizer answers *"does the column's output change?"* — **not** *"did the source text
change?"*. It canonicalizes each expression before comparing, so a set of edits are correctly
reported **`equivalent`** even though a human eye would see the text change:

- boolean reordering — `a AND b` ↔ `b AND a`
- de Morgan's law, idempotence (`x AND x` → `x`)
- constant folding to the same value
- `BETWEEN` ↔ an equivalent range comparison
- type aliases — `int` ↔ `integer`
- comment / whitespace / formatting-only changes

This is intended and safe for a **data-output** gate: the value is provably unchanged, so
there is nothing to rebuild. Don't mistake an `equivalent` verdict for a missed change — it's
the tool proving the edit is cosmetic.

!!! note "Where it deliberately says *breaking*"
    Some edits look harmless but can't be proven equivalent *type-safely*, so the tool errs
    toward breaking:

    - **arithmetic reordering** — `a + b` vs `b + a` is reported **breaking** (commutativity
      can't be proven without knowing the operand types; floats and overflow don't commute).
    - **quoted vs unquoted identifiers** on case-sensitive dialects (e.g. Snowflake) — a
      semantically real difference, so **breaking**.

    This is the fail-safe stance in action: a false "breaking" wastes a review; a false
    "equivalent" would silently ship a value change.

## How it drives "rebuild selectively"

The semantic signal is what makes selective rebuilding *safe*:

- A **proven-equivalent** refactor that reaches even a `critical` mart does **not** need a
  rebuild — the output is unchanged by construction. A policy rule that gates on
  `change.breaking` will simply not fire.
- A **breaking** change is the only thing that flows into a policy's build/test set. Combined
  with the [reach mechanism filter](policy-gate.md#reach-conditions), you can rebuild *only*
  the descendants that actually recompute the value, not the whole downstream cone.

That's the "diff cheaply, rebuild selectively" principle: the expensive rebuild scales with the
number of *genuinely breaking* changes, not the size of the diff.

## Where you see it

- **JSON** — each affected column carries `semantic` and `breaking`; each `ColumnChange`
  in the changeset does too.
- **The policy engine** — the `change.semantic` and `change.breaking` predicate axes read it
  directly (see the [policy gate guide](policy-gate.md)).
- **The explorer** — breaking columns get an amber badge; proven-equivalent ones get a
  de-emphasized neutral check (see [Explorer](explorer.md)).
