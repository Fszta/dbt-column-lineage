<p align="center">
  <img src="assets/parrant-logo.svg" alt="Parrant" width="96" height="96">
</p>

<h1 align="center">Parrant</h1>

<p align="center">
  <strong>Know what breaks — and what to rebuild.</strong><br>
  A change-impact <em>decision engine</em> for dbt. It categorizes <strong>breaking vs cosmetic</strong>
  changes, gates PRs on <strong>your</strong> policy over any dbt <code>meta</code>, and follows impact
  past dbt's edge into your <strong>BI tools</strong> — offline, from your artifacts, no warehouse,
  no <code>dbt run</code>. Built on column-level lineage.
</p>

<p align="center">
  <em>The type-checker for your dbt PRs. Parry the breaking changes, warrant the safe ones.</em>
</p>

> **Formerly `dbt-col-lineage`.** Same tool, new name. `pip install parrant` (the old
> `dbt-col-lineage` package and command still work for now).

<p align="center">
<a href="https://github.com/Fszta/parrant/actions/workflows/test.yml"><img src="https://github.com/Fszta/parrant/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
<a href="https://pypi.org/project/parrant/"><img src="https://img.shields.io/pypi/v/parrant?style=flat-square&logo=pypi" alt="PyPI Version"></a>
<a href="https://pypi.org/project/parrant/"><img src="https://img.shields.io/pypi/dm/parrant?style=flat-square&logo=pypi" alt="PyPI Downloads"></a>
<a href="https://pypi.org/project/parrant/"><img src="https://img.shields.io/pypi/pyversions/parrant?style=flat-square&logo=python" alt="Python Version"></a>
<a href="LICENSE"><img src="https://img.shields.io/github/license/Fszta/parrant?style=flat-square" alt="License"></a>
</p>

<p align="center">
📖 <a href="https://fszta.github.io/parrant/"><strong>Documentation</strong></a> &nbsp;·&nbsp;
🚀 <a href="https://dbt-column-lineage.onrender.com">Live Demo</a> &nbsp;·&nbsp;
🐛 <a href="https://github.com/Fszta/parrant/issues">Report Bug</a> &nbsp;·&nbsp;
💡 <a href="https://github.com/Fszta/parrant/issues">Request Feature</a>
</p>

<!-- TODO(rebrand): rename the Render demo service so the URL becomes parrant.onrender.com (infra step, not code) -->

Change one column in a large dbt project and you're guessing. **Which models recompute? Which
dashboards break? Should CI even let it merge?** Parrant answers that on every PR — column by
column, gated by your own policy — and posts one verdict the whole team can read before they
approve.

![The verdict: a column-level blast-radius comment on a dbt pull request](assets/pr-comment.png)

It reads only your dbt artifacts (`manifest.json` + `catalog.json`) and parses the compiled SQL
statically with [sqlglot](https://github.com/tobymao/sqlglot). **It never connects to your
warehouse and never runs dbt** — so it runs in ~a second, on any runner, including the ones where
warehouse-connected tools can't: fork PRs, air-gapped and least-privilege CI, and agent loops.

---

## Not just model-level lineage

dbt gives you *model*-level lineage. When you rename, retype, or drop a single **column**, the
dbt DAG can't tell you which downstream columns, transformations, or dashboards actually break —
it only knows model A feeds model B. That's the gap this closes.

|  | **dbt docs / DAG** | **Parrant** |
|---|---|---|
| Lineage granularity | Model → model | **Column → column** |
| *"What breaks if I change `orders.amount`?"* | Guess from the model graph | **Exact affected columns, models & exposures** |
| Cosmetic vs. real change | ❌ | ✅ tags a refactor `EQUIVALENT`, a real edit `breaking` |
| Reaches your BI layer | ❌ | ✅ follows impact into Metabase dashboards & cards |
| Verdict on the PR | ❌ | ✅ sticky comment + policy-gated block/warn |
| Machine-readable for AI agents | ❌ | ✅ one JSON document, built for automation |
| Needs a warehouse connection | `dbt docs serve` | **No — reads artifacts, runs anywhere** |

---

## The verdict on every dbt PR

Point it at CI and it posts a **sticky comment on the pull request** showing exactly what a change
breaks — the downstream models, columns, and business-facing **exposures** (dashboards, apps) it
touches — so every reviewer sees the impact before they approve. Optionally **fail the check**
when a change is too risky.

```yaml
# .github/workflows/impact.yml
permissions:
  pull-requests: write        # so the check can post its comment

# ...build base- and head-branch dbt artifacts, then:
- uses: Fszta/parrant@v0
  with:
    manifest: artifacts/head/manifest.json
    catalog: artifacts/head/catalog.json
    base-manifest: artifacts/base/manifest.json
    base-catalog: artifacts/base/catalog.json
    fail-on: none             # start non-blocking; flip to tests|exposures|policy once trusted
```

**How the loop works:** a PR opens → CI builds dbt artifacts for the base and PR branches → the
action diffs them, traces every affected column, model, and exposure, and posts **one** sticky
comment (found-and-updated via a hidden marker, so re-runs edit the same comment instead of
spamming the thread).

**The severity gate (`fail-on`)** decides when an impactful change should block the PR. It starts
off — comment-only — and you tighten it as you learn to trust it:

| `fail-on`   | Blocks the PR when… |
|-------------|---------------------|
| `none`      | never — comment only (**default**, the safe on-ramp) |
| `tests`     | a change **provably breaks a dbt test** (removes/renames a column a `not_null`/`unique`/`relationships` test still targets) — the objective, false-positive-free level to block on (needs `base-manifest`) |
| `exposures` | a change reaches a business-facing **exposure** (dashboard / app) |
| `critical`  | a downstream column **recomputes derived logic** (not just a pass-through) |
| `policy`    | your own [`policy.yml`](#the-decision-engine) says so (see the decision engine below) |
| `any`       | any downstream column is affected at all |

The action also emits step outputs for your own gating/reporting: `verdict` (`safe`/`review`/`block`),
`affected_models`, `affected_columns`, `affected_exposures`, `provable_breaks`, `tripped_level`,
`policy_decision`, `build_set_size`, `test_set_size`, and `overrides_applied`.

**Escape hatch, not off-switch.** When the gate flags a change the author knows is fine, an in-code
**override pragma** in the head model acknowledges *that one change* with a mandatory reason —
`-- lineage:allow-change reason="…"`, or `-- lineage:allow-break reason="…"` for the one thing that
can lower a provable-break block. It lives in the PR's SQL (diffable, reviewed, logged), so the
first false positive tunes the gate instead of disarming it repo-wide. Every honored override is
surfaced in the PR comment and counted in `overrides_applied`; run `parrant impact --no-overrides`
(or set the action's `no-overrides: true`) to see the raw gate. See the
[override guide](https://github.com/Fszta/parrant/blob/main/docs/decision-engine/policy-gate.md#overriding-a-verdict-the-in-code-escape-hatch).

Pin `@v0` for updates within the current major (like `actions/checkout@v4`), or an exact release —
`@v0.17.1` — for reproducible builds. The action installs the CLI bundled at whichever ref you pin,
so the tool always matches the tag. Complete runnable workflows live at
[`docs/examples/impact-pr-check.yml`](docs/examples/impact-pr-check.yml) (base + head built in CI)
and [`docs/examples/impact-pr-check-s3-prod.yml`](docs/examples/impact-pr-check-s3-prod.yml)
(prod artifacts pulled from S3, no compiled SQL needed).

---

## Why it runs where other tools can't

Four guardrails, held on purpose — they're what make the verdict trustworthy enough to gate on:

- **Offline, zero-credential.** Reads `manifest.json` + `catalog.json` only. Never connects to the
  warehouse, never runs dbt. So it runs on fork PRs, air-gapped/regulated runners, least-privilege
  CI, and in per-call agent loops — the places a warehouse connection can't reach.
- **Metadata-agnostic.** The tool ships the *engine*; **you** ship the rules. No metadata key is
  privileged — `critical`, `pii`, `tier` are example configs, never built-ins.
- **Expression classification, not value diffing.** It classifies whether a column's *logic*
  changed, statically. "Will the actual values differ?" is a data-diff's job (clone → rebuild →
  diff); Parrant never queries data.
- **Fail-safe.** Anything not *proven* safe is treated as breaking. A false-breaking is noise; a
  false-safe is a silent regression — so the gate always errs toward flagging.

---

## Quick start (local)

```bash
pip install parrant
```

Generate your dbt artifacts once — this is the only step that touches dbt, and it still never
connects to your warehouse:

```bash
dbt compile          # produces target/manifest.json
dbt docs generate    # produces target/catalog.json (column metadata)
```

Run the impact report against your base branch — the same verdict CI posts, in your terminal:

```bash
# Reliable two-manifest diff (base branch vs. current)
parrant impact \
    --manifest target/manifest.json --catalog target/catalog.json \
    --base-manifest base/manifest.json --base-catalog base/catalog.json

# Git-diff fallback when only one manifest is available
parrant impact --git-base main
```

It derives the *set* of changed columns for the branch and reports one consolidated blast radius,
ranked by severity — **`removed` > `type_changed` > `logic_changed` > `added`**. It defaults to a
human-readable Markdown summary; add `--format json` for the machine-readable report, `--explain`
to see *why* each verdict was reached, and `--ci` to post the sticky PR comment and apply the gate.

> Works even when `manifest.json` has no embedded `compiled_code` (e.g. from `dbt parse`), as long
> as `target/compiled/**` exists — it falls back to the compiled SQL on disk.

---

## The decision engine

The impact report is the foundation; on top of it Parrant turns a PR into a **decision**, on the
principle *"diff cheaply, rebuild selectively"* — CI cost should scale with a change's true blast
radius, not the size of your DAG. Every layer below is **additive**: skip the flags and the tool
behaves exactly as before.

| Capability | The question it answers |
|---|---|
| **Semantic categorization** | *Did this column's output actually change, or is the edit cosmetic?* |
| **Policy gate** | *Given my org's rules, should this change block, warn, or build/test something?* |
| **Cross-boundary (BI)** | *Will this column change break **that dashboard** — past dbt's edge?* |

**Breaking, or just cosmetic?** — It diffs the SQL *expression*, not the text. A whitespace,
comment, alias, or paren-only refactor is proven `EQUIVALENT` and **doesn't flag**; a change that
shifts meaning — or that can't be proven safe — is tagged `breaking`. Kills the false-positive
noise that erodes trust in a gate.

**Your rules, your gate.** — Author a versioned `policy.yml`: a predicate over *any* dbt `meta`
(inherited down lineage via `inferred_meta`), the change kind, the semantic signal, the lineage
reach, and dbt's own resolved `config` (e.g. `config.grants.select`) → an action
(`block` / `warn` / `build` / `test` / `notify`). Your governance becomes CI, keyed to *your*
metadata. `critical` below is *your* key, not a built-in:

```yaml
# policy.yml — a breaking change reaching a critical mart blocks, and rebuilds only what recomputes
version: 1
rules:
  - id: breaking-reaches-critical
    scope: change
    predicate:
      all:
        - change: { field: breaking, op: is_true }   # proven-equivalent refactors don't trip it
        - reach: { kind: model, where: { meta: { key: critical, op: is_true } } }
    action:
      - type: block
      - type: add-to-build-set
        include: reached
        mechanism: [derived_recompute, rowset_filter]  # rebuild only the descendants that recompute
```

```bash
parrant impact --base-manifest base/manifest.json --base-catalog base/catalog.json \
    --policy policy.yml --fail-on policy
```

More patterns — PII allowlists, executive-dashboard gates, notify actions — in the
[policy recipes](https://fszta.github.io/parrant/decision-engine/policy-recipes/).

- **Scaffold a starter policy** — `parrant policy init` reads your manifest + catalog and writes a
  heavily-commented, safe-by-construction `parrant.policy.yml` keyed only to signals the scan
  confirmed exist. It runs green on day one — no rage-block.

  ```bash
  parrant policy init --manifest target/manifest.json --catalog target/catalog.json
  ```
- **Backtest before you arm it** — `parrant policy test` replays a candidate policy over your recent
  git history (`--last 30`) or a saved changeset corpus and reports, per rule, what the gate *would*
  have ruled — including how many firings were driven by a fail-safe UNKNOWN rather than a proven
  match, and which rules never fired at all. Offline and deterministic.

  ```bash
  parrant policy test --policy policy.yml --last 30
  ```
- **Cross-boundary (Metabase) impact** — a separate credentialed `parrant metabase-extract` step
  snapshots Metabase into `metabase_lineage.json`; the offline gate then answers *"will this column
  change break that dashboard?"* by folding dashboards and cards into the same reach the policy
  engine scans. Metabase is the first supported BI connector; the credentialed step is fully
  optional — remove it and the tool stays 100% usable and zero-credential.

Full guides: **[Decision Engine docs](https://fszta.github.io/parrant/decision-engine/overview/)**.

---

## Machine-readable output (built for agents & automation)

The same deterministic core that gates CI is agent-shaped. Emit any column's lineage and downstream
impact as a **single JSON document** — a stable contract you can pipe into an LLM tool call, a CI
script, or your own tooling:

```bash
parrant --select stg_accounts.account_id+ --format json \
    --manifest target/manifest.json --catalog target/catalog.json
```

Selector grammar (works for `text`, `json`, and `dot` output on the root command):

| Selector | Meaning |
|---|---|
| `+model.col` | upstream only (where the value comes from) |
| `model.col+` | downstream only (what it feeds) |
| `model.col`  | both directions |

The JSON splits `upstream`/`downstream` into `models`, `sources`, `direct_refs`, and `exposures`,
plus an `impact` block summarising the affected models, columns, and exposures. Use
`--format dot` for Graphviz. (`parrant impact` has its own `--format markdown|json`.)

---

## Explore it visually

Prefer a picture? Launch the interactive explorer — no flags needed, it reads `target/` by default:

```bash
parrant --explore
```

Open `http://127.0.0.1:8000`, pick a column, and click **Analyze Impact** to see the columns that
need review, the pass-through columns, and the affected models and exposures.
**[Try the live demo →](https://dbt-column-lineage.onrender.com)** — no install required.

![Interactive column-lineage explorer](assets/demo_lineage.gif)

![Impact analysis in the explorer](assets/impact-analysis.png)

Point the explorer at a change (`--base-manifest`, `--git-base`, `--policy`, `--metabase`) and it
surfaces the same decision layer the PR gate uses.

---

## Compatibility

Works with **any [sqlglot](https://github.com/tobymao/sqlglot) dialect** via `--adapter`
(auto-detected from your manifest by default). Verified against **Snowflake**, **DuckDB**,
**SQLite**, and **MS SQL Server / TSQL**; on BigQuery, Redshift, Postgres, etc., pass
`--adapter <dialect>` if auto-detection needs a nudge.

## Limitations

- Python models are not supported.
- Some SQL functions/syntax can't be parsed and cause the affected model to be skipped — surfaced
  as reduced *coverage/confidence* in the report, never a silent gap.

## Documentation

Full CLI reference — every flag (`--scope-git`, `--github-token`/`--repo`/`--pr-number`, the
complete `impact`, `policy`, and `metabase-extract` surfaces), output formats, the policy DSL, and
CI recipes — lives at **[fszta.github.io/parrant](https://fszta.github.io/parrant/)**.

## License

MIT
