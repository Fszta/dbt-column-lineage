<p align="center">
  <img src="assets/parrant-logo.svg" alt="Parrant" width="96" height="96">
</p>

<h1 align="center">Parrant</h1>

<p align="center">
  <strong>The type-checker for your dbt PRs.</strong><br>
  Catches breaking column changes in your dbt project before they merge.
</p>

<p align="center">
  <code>pip install parrant</code>
</p>

<p align="center">
<a href="https://github.com/Fszta/parrant"><img src="https://img.shields.io/github/stars/Fszta/parrant?style=flat-square&logo=github" alt="GitHub Stars"></a>
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

![The verdict: a column-level blast-radius comment on a dbt pull request](assets/pr-comment.png)

Change one column in a large dbt project and you're guessing. **Which models recompute? Which
dashboards break? Should CI even let it merge?** Parrant answers that on every PR — column by
column, gated by your own policy — and posts one verdict the whole team can read before they
approve.

It reads only your dbt artifacts (`manifest.json` + `catalog.json`) and parses the compiled SQL
statically with [sqlglot](https://github.com/tobymao/sqlglot). **It never connects to your
warehouse and never runs dbt** — so it runs in ~a second, on any runner, including the ones where
warehouse-connected tools can't: fork PRs, air-gapped and least-privilege CI, and agent loops.

## Contents

- [Quick start](#quick-start) — preview the CI verdict locally
- [Use it in CI](#use-it-in-ci) — the verdict on every PR
- [Why parrant](#why-parrant) — and how it compares
- [The decision engine](#the-decision-engine) — policy-gate your own rules
- [More surfaces](#more-surfaces) — JSON for agents & the visual explorer
- [Compatibility & limitations](#compatibility--limitations)

---

## Quick start

Preview the exact verdict CI will post — right in your terminal.

```bash
pip install parrant
```

Generate your dbt artifacts once. `dbt docs generate` is the one upstream step that queries your
warehouse (to build the catalog) — **parrant itself never connects**:

```bash
dbt compile          # produces target/manifest.json
dbt docs generate    # produces target/catalog.json (column metadata)
```

Run the impact report against your base branch — the same verdict CI posts, in your terminal:

```bash
# Zero-setup: diff against your base branch using the artifacts you just built
parrant impact --git-base main

# Exact CI-grade diff: compare base- and head-branch artifacts (what the CI action runs).
# Build base/ from your main branch the same way, or pull it from your artifact store.
parrant impact \
    --manifest target/manifest.json --catalog target/catalog.json \
    --base-manifest base/manifest.json --base-catalog base/catalog.json
```

It derives the *set* of changed columns for the branch and reports one consolidated blast radius,
ranked by severity — **`removed` > `type_changed` > `logic_changed` > `added`**. It defaults to a
human-readable Markdown summary; add `--format json` for the machine-readable report, `--explain`
to see *why* each verdict was reached, and `--ci` to post the sticky PR comment and apply the gate.

> Works even when `manifest.json` has no embedded `compiled_code` (e.g. from `dbt parse`), as long
> as `target/compiled/**` exists — it falls back to the compiled SQL on disk.

---

## Use it in CI

Point it at CI and it posts a **sticky comment on the pull request** showing exactly what a change
breaks — the downstream models, columns, and business-facing **exposures** (dashboards, apps) it
touches — so every reviewer sees the impact before they approve. Optionally **fail the check** when
a change is too risky.

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
    fail-on: none             # start non-blocking; flip to tests|policy once trusted
```

A PR opens → CI builds dbt artifacts for the base and PR branches → the action diffs them, traces
every affected column, model, and exposure, and posts **one** sticky comment (updated in place on
re-runs, never spamming the thread).

The severity gate (`fail-on`) starts **off** and you tighten it as you learn to trust the tool:

| `fail-on` | Blocks the PR when… |
|-----------|---------------------|
| `none`    | never — comment only (**default**, the safe on-ramp) |
| `tests`   | a change **provably breaks a dbt test** — the most conservative, objective level to gate on |
| `policy`  | your own [`policy.yml`](#the-decision-engine) says so |

> `exposures`, `critical`, and `any` levels, the step outputs (`verdict`, `affected_models`, …),
> the in-code override pragma, and version-pinning guidance are all in the
> [docs](https://fszta.github.io/parrant/).

Building base + head artifacts in CI is a few steps — copy a complete, runnable workflow from
[`docs/examples/impact-pr-check.yml`](docs/examples/impact-pr-check.yml) (build both in CI) or
[`docs/examples/impact-pr-check-s3-prod.yml`](docs/examples/impact-pr-check-s3-prod.yml) (pull prod
artifacts from S3, no compiled SQL needed).

---

## Why parrant

dbt gives you *model*-level lineage. When you rename, retype, or drop a single **column**, the dbt
DAG can't tell you which downstream columns, transformations, or dashboards actually break — it
only knows model A feeds model B. That's the gap this closes.

|  | **dbt docs / DAG** | **Parrant** |
|---|---|---|
| Lineage granularity | Model → model | **Column → column** |
| *"What breaks if I change `orders.amount`?"* | Guess from the model graph | **Exact affected columns, models & exposures** |
| Cosmetic vs. real change | ❌ | ✅ tags a refactor `EQUIVALENT`, a real edit `breaking` |
| Reaches your BI layer | ❌ | ✅ follows impact into Metabase dashboards & cards |
| Verdict on the PR | ❌ | ✅ sticky comment + policy-gated block/warn |
| Machine-readable for AI agents | ❌ | ✅ one JSON document, built for automation |

The tools that go deeper than dbt docs fall into two camps. **Data-diff tools** (recce, datafold)
rebuild your models and compare *values* — accurate, but they need warehouse compute. **Static
column-lineage** tools (dbt Cloud's column-level lineage, and SQLMesh — which, like parrant, derives
lineage with sqlglot) don't touch the warehouse for lineage, but they mean adopting dbt Cloud's paid
tier or migrating your project to the SQLMesh framework, and they give you a *graph*, not a verdict.
Parrant is the drop-in that turns a change into a **pass/block decision on the PR** — on your
existing dbt-core repo, fully offline:

|  | dbt Cloud CLL | SQLMesh | recce · datafold | **Parrant** |
|---|---|---|---|---|
| Column-level analysis | ✅ static | ✅ static (sqlglot) | ✅ value-diff | ✅ static (sqlglot) |
| Drop-in on existing dbt-core (no SaaS tier / no migration) | ❌ | ❌ | ✅ | ✅ |
| Offline, no warehouse credentials | ❌ | ✅ | ❌ | ✅ |
| Turns a change into a **PR gating verdict** | ❌ | ❌ | partial | ✅ |

Two guardrails make the verdict trustworthy enough to gate on:

- **Expression classification, not value diffing.** It classifies whether a column's *logic*
  changed, statically — a whitespace, comment, alias, or paren-only refactor is proven `EQUIVALENT`
  and **doesn't flag**. "Will the actual values differ?" is a data-diff's job.
- **Fail-safe.** Anything not *proven* safe is treated as breaking. A false-breaking is noise; a
  false-safe is a silent regression — so the gate always errs toward flagging.

---

## The decision engine

The impact report is the foundation; on top of it Parrant turns a PR into a **decision**, on the
principle *"diff cheaply, rebuild selectively."* Every layer is **additive** — skip the flags and
the tool behaves exactly as before.

| Capability | The question it answers |
|---|---|
| **Semantic categorization** | *Did this column's output actually change, or is the edit cosmetic?* |
| **Policy gate** | *Given my org's rules, should this change block, warn, or build/test something?* |
| **Cross-boundary (BI)** | *Will this column change break **that dashboard** — past dbt's edge?* |

**Your rules, your gate.** Author a versioned `policy.yml`: a predicate over *any* dbt `meta`
(no key is privileged — `critical` below is *your* key, not a built-in), the change kind, the
semantic signal, and the lineage reach → an action (`block` / `warn` / `build` / `test` / `notify`):

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
      - type: add-to-build-set   # rebuild only the descendants that actually recompute
        include: reached
```

- **Scaffold a safe starter** — `parrant policy init` writes a heavily-commented policy keyed only
  to signals your manifest+catalog confirm exist. It runs green on day one — no rage-block.
- **Backtest before you arm it** — `parrant policy test --policy policy.yml --last 30` replays a
  candidate policy over recent git history and reports, per rule, what the gate *would* have ruled.
- **Cross-boundary (Metabase) impact** — an optional credentialed `parrant metabase-extract` step
  folds dashboards and cards into the same reach the policy engine scans. Remove it and the tool
  stays 100% usable and zero-credential.

Full guides: **[Decision Engine docs](https://fszta.github.io/parrant/decision-engine/overview/)**.

---

## More surfaces

**JSON for agents & automation.** The same deterministic core that gates CI is agent-shaped — emit
any column's lineage and downstream impact as a **single JSON document**, a stable contract you can
pipe into an LLM tool call, a CI script, or your own tooling:

```bash
parrant --select stg_accounts.account_id+ --format json \
    --manifest target/manifest.json --catalog target/catalog.json
```

| Selector | Meaning |
|---|---|
| `+model.col` | upstream only (where the value comes from) |
| `model.col+` | downstream only (what it feeds) |
| `model.col`  | both directions |

Selectors work for `text`, `json`, and `dot` output. (Full JSON shape in the
[docs](https://fszta.github.io/parrant/).)

**Explore it visually.** Prefer a picture? Launch the interactive explorer — no flags needed, it
reads `target/` by default:

```bash
parrant --explore
```

Pick a column, click **Analyze Impact**, and see the columns that need review, the pass-through
columns, and the affected models and exposures.
**[Try the live demo →](https://dbt-column-lineage.onrender.com)** — no install required.

![Interactive column-lineage explorer](assets/demo_lineage.gif)

---

## Compatibility & limitations

Works with **any [sqlglot](https://github.com/tobymao/sqlglot) dialect** via `--adapter`
(auto-detected from your manifest by default).

- **Verified:** Snowflake, DuckDB, SQLite, MS SQL Server / TSQL.
- **Best-effort:** BigQuery, Redshift, Postgres, etc. — pass `--adapter <dialect>` if
  auto-detection needs a nudge.

Known limits (surfaced honestly, never silently):

- **BI reach is Metabase-only** today — Looker / Tableau / Mode are not yet supported.
- Python models are not supported.
- Some SQL functions/syntax can't be parsed and cause the affected model to be skipped — surfaced
  as reduced *coverage/confidence* in the report, never a silent gap.

Full CLI reference — every flag, output format, the policy DSL, and CI recipes — lives at
**[fszta.github.io/parrant](https://fszta.github.io/parrant/)**.

---

<p align="center">
  <strong>Found this useful?</strong> ⭐ <a href="https://github.com/Fszta/parrant">Star the repo</a>
  · <a href="https://github.com/Fszta/parrant/blob/main/CHANGELOG.md">Changelog</a>
  · <a href="https://github.com/Fszta/parrant/issues">Contribute</a>
</p>

<p align="center">
  <sub>
    Built and run in production against a real analytics dbt project.
    Formerly <code>dbt-col-lineage</code> — the old package and command still work.
    <em>Parrant = parry + warrant.</em> · MIT License
  </sub>
</p>
