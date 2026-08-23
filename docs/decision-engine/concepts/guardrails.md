# Guardrails & non-goals

The decision engine is built inside a small set of deliberate constraints. They are not
limitations to work around — they are what make the tool trustworthy, portable, and safe to put in
front of a merge gate. Knowing them tells you what a verdict *means* and what it deliberately does
*not* claim.

## The four guardrails

### 1. Offline, zero-credential at gate time
The impact/policy run reads only your dbt artifacts (`manifest.json`, `catalog.json`) and, if you
opt in, a `metabase_lineage.json` snapshot. It **never connects to your warehouse and never runs
dbt**. A pull-request check needs no secrets and no infrastructure.

The one credentialed step — `metabase-extract`, which snapshots Metabase — is a *separate*,
scheduled job. The gate consumes its output file offline, exactly like it consumes dbt's artifacts.
(See [Cross-boundary](../metabase.md).) The offline guarantee is structural: the gate path imports
the artifact readers and join logic only, never a credentialed client.

### 2. Metadata-agnostic — the tool ships the engine, you ship the rules
No metadata key is privileged. `critical`, `pii`, `tier`, `readable_by` are **example consumer
configs**, never built-ins. The tool ships a generic rule engine and the rule schema; your
`policy.yml` expresses *your* org's conventions over *any* dbt `meta`. Nothing about your taxonomy
is hardcoded, so the same engine serves every team without a fork.

### 3. Expression classification, not value diffing
Semantic categorization classifies the **SQL expression** — "does the logic that produces this
column change?" It is an **output gate, not a source-text gate**: a reformatting or a provable
simplification is reported `equivalent` even though the text changed. What it does **not** do is
check whether the values on real warehouse rows would actually differ. That is a different,
warehouse-level question (a data-diff tool's job). The engine tells you the *expression* changed
meaning; proving the *data* changed is out of scope by design.

### 4. Fail-safe — anything not proven safe is treated as breaking
The gate biases toward over-blocking. An unparseable expression, an undecidable comparison, a
missing input — all resolve toward *breaking* / *blocking*, never toward silently passing. A false
"breaking" costs a reviewer a second look; a false "equivalent" would ship a value change unnoticed.
The whole system is tuned so the first can happen and the second cannot. (The precise rules live in
[Things to know](gotchas.md) and the [policy fail-safe section](../policy-gate.md#fail-safe-defaults).)

## What it is *not* (non-goals)

| It does **not**… | Because… | That job belongs to… |
|---|---|---|
| run dbt or query your warehouse | the gate must stay offline / zero-credential | your dbt build / warehouse |
| prove whether **values** actually differ on real data | it classifies the expression, not the data | a warehouse **data-diff** tool |
| ship an opinion about which `meta` keys matter | it is metadata-agnostic | **your** `policy.yml` |
| run or replace your dbt **tests** | it gates on *change impact*, not data quality | `dbt test` (it can *schedule* a selective test set, not execute it) |
| maintain a live lineage store | it recomputes from artifacts each run (no drift) | the artifacts themselves |

## Why this matters

These guardrails are what let you trust a `block`: it means *"a change whose expression provably
changed meaning (or couldn't be proven safe) reaches something your own rules flagged"* — computed
without touching your warehouse, without any privileged assumptions about your metadata, and biased
so it would rather over-warn than miss. When a verdict is uncertain, the tool says so (see the
confidence and cross-boundary honesty signals in the [Glossary](../glossary.md#8-confidence-coverage-can-i-trust-the-answer-is-complete))
rather than fabricating certainty.

---

!!! note "For contributors: lineage is a swappable backend"
    Column lineage today is computed by the tool's own SQLGlot engine, but every product layer
    (semantic, policy, reach) depends on a stable `LineageProvider` interface, not on how lineage is
    computed. That seam means the lineage backend can be swapped (e.g. for dbt Fusion's column
    lineage, once stable) without touching the layers above. Lineage computation is treated as a
    commodity; the durable investment is the decision engine on top of it. This is an architecture
    concept — end users never see it.
