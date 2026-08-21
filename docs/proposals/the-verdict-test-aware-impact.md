# Product Proposal — "The Verdict": test-aware, provable, blockable column impact

**Status:** Draft for review — §0 revision added 2026-08-20 after a positioning challenge round
**Author:** Multi-agent product exploration (synthesized)
**Date:** 2026-08-20

---

## 0. Framing update (revision) — "the always-on baseline", and The Verdict as its keystone

A second challenge round pressure-tested a sharper thesis: *the durable value is the most
comprehensive change-impact view that needs **no warehouse**, because in real life you often can't
touch prod data (PII/governance, separate accounts, external fork PRs) and at scale you can't afford
to re-run/re-materialize to diff.* Verdict of that round (unanimous): **the direction is right; the
word "holistic" is wrong.**

- **Adopt the positioning, not the adjective.** Offline/artifacts-only is the moat and the
  distribution mechanism — incumbents (Datafold, Recce, dbt Cloud CLL) *structurally cannot run* in a
  fork PR, an air-gapped/regulated runner, or an agent's edit loop. But "most **holistic** view" has
  no definition-of-done (scope-creep vs the single-maintainer guardrail), markets a completeness the
  artifacts can't back (value-correctness is unreachable offline; recall is catalog/rename-bounded),
  and fights the real moat, which is **honesty**, not breadth.
- **The framing to adopt:** *the trustworthy, always-on offline baseline — the type-checker to the
  warehouse's test suite.* Runs on every PR/fork/agent-loop in ~1s to **prove** what breaks offline
  and **honestly flag** what only a warehouse can confirm. The lower-bound isn't an apology; it's the
  division of labor (we say REVIEW → *then* you reach for the warehouse tool).
- **The container: one Impact Report.** A single deterministic, confidence-tagged impact object
  (per-changeset / per-column / whole-repo), rendered three ways off one core — PR ruling, agent JSON,
  explorer — **stacked by descending certainty**, each claim stamped `proven` / `structural` /
  `heuristic` / `needs-warehouse`, the whole document bounded by one coverage floor. "Holistic" is
  redefined operationally as *the certainty-tagged union of everything statically derivable* — never
  "we see everything."
- **The Verdict survives as the keystone.** It is the *only* `proven` tier and the only thing that may
  drive BLOCK; it disciplines the report so "comprehensive" can't rot into the rejected lineage
  *viewer*. Previously-secondary pieces get **promoted to first-class** because they make the report
  daily-valuable on the common SAFE/REVIEW PR (fixing the rare-event ceiling in §10, risk 6):
  owner routing (restore `Exposure.owner`), mechanism-split blast radius (surface already-parsed
  `star_sources` + `predicate_lineage`), and the agent-native MCP object.
- **Admission rule (guardrail).** A signal earns a place only if **(a)** provable offline → may BLOCK,
  or **(b)** explicitly advisory *and* printed under the coverage floor. No new long-lived surface is
  added in the name of breadth. Breadth lives in the tagline, not the roadmap.
- **Honest correction to the premise:** don't claim "they can't access prod" (incumbents run against
  CI/staging). Claim the precise walls where offline is the *only* option: **fork/external-contributor
  PRs, regulated/air-gapped runners, least-privilege/separate-account CI, cost-frozen windows, and
  per-call agent loops.**

Everything below (the mechanism, MVP, risks) stands unchanged — this section only resets the *why* and
the neighbor priority. **Build order is unaffected:** manifest-first recall → test-node ingestion (the
Verdict) → cheap absorptions → the Impact Report envelope → MCP.

### Implementation status (2026-08-20)

Branch **`feat/test-aware-impact`**. MVP step 1 (foundation) is **implemented + verified** (240 tests
green): `TestNode` in `schema.py`, `ManifestReader.get_tests()`, and a `(model, column) -> [TestNode]`
reverse index (`ModelRegistry.get_column_tests`), with unattributable tests counted (not guessed) for
the coverage floor. Fixture: real dbt column tests added to `dbt_test_project` + manifest regenerated
offline (DuckDB). **Confirmed dbt 1.10 test-node shape:** top-level `column_name` + `attached_node`
(`model.<pkg>.<name>`), plus `test_metadata.name` / `test_metadata.kwargs` (`to`/`field` for
`relationships`); singular/custom tests carry no `test_metadata` and are excluded from the column
index. Not yet built: the classifier (step 3), rendering (step 4), and `FailOn.TESTS` (step 5).

---

## 1. The one-line idea

Add a **provable, block-worthy tier** on top of the impact analysis we already have — so the PR
comment doesn't just say *"this reaches 9 models and 2 dashboards, go check"* but leads with
*"this **will** break the `not_null` test on `orders.customer_id`, owned by @jane"* — a verdict
objective enough to **block a PR**, computed entirely from `manifest.json` with **no warehouse
and no credentials**.

We do this by reading the one asset we already have on disk and completely ignore today:
**dbt's own test graph** (~1000 test nodes on the real Swan project, read *nowhere* in the code).

> **This is additive, not a replacement.** The tool's existing capabilities are *not* being
> sidelined — they become the tiers of one **Impact Report**, ranked by certainty:
>
> | Tier | Feature | Ruling it drives |
> |---|---|---|
> | Foundation | **Column lineage** (shipped) | — the graph everything reads |
> | Structural | **Blast radius / impact** (shipped): reaches N models·columns·exposures, derived recompute, row-set/filter | **REVIEW** |
> | Reach | **Exposures + owners** (shipped, now @-routed) | **REVIEW** |
> | Provable | **Test breaks — "the Verdict"** (this proposal) | **BLOCK** |
> | Interactive | **The explorer** (shipped) | the human lens on the *same* data |
>
> The test-verdict is the **sharp tip** that earns the right to *block* CI. It does not remove
> the blast-radius report (that stays as the REVIEW tier) or the explorer (the interactive
> surface). "Topology" isn't discarded — it's demoted below a tier that can *prove* breakage.
> The rest of this document details only the new tip, because that is the net-new work; the
> existing tiers are described in `docs/features/impact-analysis.md`.

---

## 2. The question everyone asks first: "how do you use tests without running them?"

**We never run a test. We read what it *declares*.**

A dbt test is a *declaration* in `manifest.json`. A `not_null` test on `orders.customer_id` is
stored as a test node whose `attached_node`/`depends_on` is `orders` and whose target is
`column_name = customer_id`. dbt compiles it (at build time) into SQL roughly like:

```sql
select customer_id from orders where customer_id is null
```

If a PR **renames or removes `customer_id`**, that compiled SQL now references a column that no
longer exists — it *will* error on the next `dbt build`. We know that **for certain, statically**,
just from the declaration. This is exactly how a type-checker (`tsc`, `rustc`) tells you a renamed
symbol broke its callers **without executing the program**.

The whole design rests on separating two questions that look similar but are completely different:

| Question | Example | Provable offline? | Tier |
|---|---|---|---|
| **Is the test still *valid*?** (referential / compile break) | You rename `customer_id`; `not_null(customer_id)` now targets a column that doesn't exist | ✅ **YES** — it's in the manifest | **BLOCK** |
| **Is a downstream `ref` still resolvable?** | You remove `customers.customer_id`; a `relationships` test / a downstream `SELECT customer_id` can't resolve it | ✅ **YES** | **BLOCK** |
| **Will the test still *pass* against data?** | You change a `CASE` branch; column still exists but may now produce nulls | ❌ NO — needs the warehouse | **ADVISORY (warn only)** |

We claim **only the first two** as blocking breaks. The third — logic changed but the column and
its references still exist — stays advisory ("this may change values downstream, review it"). That
line is the product: it is what lets a team flip the gate from *warn* to *block* without fear.

**What we explicitly do NOT do:** predict whether a number will be wrong. That needs the warehouse
and is Datafold/Recce's ground. Chasing it forfeits the one open square (see §7).

---

## 3. Where the tool stands today (and the gap)

The impact engine (`LineageService.get_column_impact`) returns, for a changed column, the set of
downstream models / columns / exposures, with severity decided by a single fuzzy heuristic:

```python
is_critical = transformation_type == "derived"   # service.py:570
```

Two consequences:

- **It's the part that structurally needs a warehouse to be correct.** "Is `derived` reach actually
  a problem?" is a data question. So the signal can only ever *warn*.
- **No team blocks CI on a heuristic.** A gate nobody dares enable isn't a gate.

Meanwhile, verified against the code:

- `artifacts/manifest.py:53` ingests only `resource_type in ("model", "snapshot")`; the registry's
  `_MODEL_LIKE_RESOURCE_TYPES` is `{model, snapshot, seed}`. **Test nodes are read nowhere.** On the
  real Swan manifest that is ~1000 test nodes (`not_null`, `unique`, `relationships`,
  `accepted_values`, …) of pure, unused signal.
- `exposure.owner` **is** parsed (`registry.py:197`, `Exposure.owner` at `schema.py:30`) and shown in
  the explorer — but is dropped from the CLI/markdown/JSON impact path. So we already know *who to
  tell* and throw it away.

---

## 4. The feature: a two-tier PR ruling

The PR comment (and JSON) leads with **one ruling** — **SAFE / REVIEW / BLOCK** — then splits into
two clearly separated tiers:

```
BLOCK — 1 provable break, 2 exposures affected

  Provable breaks (block-worthy)
    error[BREAK-TEST]  renaming orders.customer_id breaks relationships test
                       relationships_fct_payments_customer_id__ref_customers
                       models/marts/_payments.yml:42
                       Owner: @jane.doe

  Advisory (heuristics — will not block)
    ~ 6 downstream columns recompute derived logic from orders.customer_id  [collapsed]

  Coverage: verdict saw 168 of 171 reachable models; 3 unparsed (see below).
```

- **BLOCK** is earned *only* by a provable break (§2, tiers 1–2). It drives a new `--fail-on tests`
  gate level.
- The old `derived`-reach heuristic is **not deleted** — it's demoted below the fold as advisory,
  which can warn but never block.
- Every provable break is a **compiler-style diagnostic**: a code (`error[BREAK-TEST]` /
  `error[BREAK-REF]`), the exact offending node id, the `file:line` to fix (from the test's
  `original_file_path`), and the **@owner** to notify.
- The existing `ImpactConfidence` block prints the coverage floor under the ruling, so BLOCK is
  never claimed blind.

**Moment of use.** A reviewer opens a PR that renames `orders.customer_id`. Instead of leaving the
PR to grep `.yml` files and guess who cares, they read three lines, `@`-mention Jane, request
changes — and *trust* the BLOCK because it names the exact test that will fail on the next build.
They never open a `.yml` file.

---

## 5. How it works (grounded in the shipped engine)

Three moves, all riding existing machinery:

1. **Ingest the test graph (new raw material).** Extend `manifest.py:53` to also read
   `resource_type == "test"` nodes: pull `test_metadata.name`, the target `column_name` (top-level or
   from `test_metadata.kwargs`), and the target model (`attached_node` / `depends_on.nodes`). Build a
   reverse index `get_column_tests(model, column) -> [TestNode]` in `registry.py`, backed by a new
   `TestNode` pydantic model in `models/schema.py` (not loose dicts). This alone lights up the ~1000
   unused test nodes.

2. **Classify provable breaks.** `changeset.py` already emits `ColumnChange` with the `ChangeKind`
   ladder (`REMOVED` > `TYPE_CHANGED` > `LOGIC_CHANGED` > `RENAMED` > `ADDED`). For each **REMOVED /
   RENAMED** change, `get_column_impact` already returns the BFS-reachable downstream `model.column`
   set. Intersect that with the test index:
   - a removed/renamed column that is the **target of a test** → `BREAK-TEST` (the test can no longer
     reference a valid column);
   - a removed/renamed column that is **directly `SELECT`ed / `ref`erenced downstream** → `BREAK-REF`.
   Each becomes a `BreakFinding` (new schema type, `kind = provable`) carrying the exact node id.
   Removed-but-unreferenced-and-untested stays **SAFE**. `LOGIC_CHANGED` (column still exists) stays
   **advisory**. This replaces the `is_critical == "derived"` driver at `service.py:570`.

3. **Render + gate + route.** Restore `exposure.owner` into the impact dicts (~2 lines; already
   loaded). Restructure `display/markdown.py` into *ruling → provable diagnostics → collapsed
   advisory*, with the `ImpactConfidence` floor printed under the ruling. Add `FailOn.TESTS` to
   `lineage/ci.py` (extending the existing `NONE/EXPOSURES/CRITICAL/ANY` enum + `gate_exit_code` +
   machine outputs), reusing the **shipped** sticky find-or-update comment machinery — **zero new
   long-lived surfaces.**

The same deterministic core (sqlglot parse + BFS + static manifest lookup) feeds both the CI gate and
the planned MCP server.

---

## 6. The one hard prerequisite

**Manifest-first registry must land with, or before, the blocking path.** Today the catalog-first
registry under-reports badly — on the real Swan target it sees **8 of 175** affected models. A BLOCK
gate on ~85%-low recall would read a real break as a confident **SAFE** — a false negative, which is
*strictly worse* than no gate. Until recall is trustworthy, **ship warn-only.** (This is already the
#2 item on the roadmap; it is now a blocker for the blocking path, not a nice-to-have.)

---

## 7. Why this is the right bet (positioning)

- **White space.** No incumbent ships *free + OSS + artifacts-only + column-level + drop-in on
  existing dbt + a pre-merge PASS/FAIL that reads dbt's own test graph.* Datafold/Recce need a live
  warehouse (can't run in a fork PR). dbt Cloud's column-level lineage is Enterprise-gated + post-run,
  and `state:modified` CI is model-granular (misses column changes). SQLMesh does static AST-diff but
  requires abandoning dbt for its plan/apply, and **has publicly named per-downstream-column
  classification as *unshipped future work*** — this delivers exactly that, today, in
  consumer-oriented framing ("a test fires / a dashboard breaks") vs SQLMesh's compute-oriented "must
  backfill."
- **Defensibility is positional + honesty.** The moat is being first, correct, and *honest about
  coverage* as an OSS niche — plus incumbents being structurally committed away from this square. The
  `ImpactConfidence` floor is a trust signal warehouse tools cannot match offline.
- **Don't build:** a pure lineage *viewer* (re-litigates the settled wedge, commoditized), or
  value/data-level "will the number be wrong" prediction (forfeits the open square to the warehouse
  camp — keep it deferred as a future "authoritative mode").

---

## 8. MVP (ordered)

0. **[prerequisite]** manifest-first registry. Blocking path ships warn-only until this lands.
1. Ingest `resource_type == "test"` nodes → `get_column_tests` reverse index + `TestNode` schema.
2. Restore `exposure.owner` into impact output (~2 lines — cheapest win, unlocks owner routing).
3. Classifier, narrowest scope: `REMOVED`/`RENAMED` × (`not_null` + `unique` + `relationships`) tests
   attached to the column, plus direct downstream broken `ref`. Everything else stays advisory.
4. Restructure `markdown.py` into the three-tier ruling with the `ImpactConfidence` floor.
5. Add `FailOn.TESTS` + machine outputs; default stays **warn**, teams opt into `--fail-on tests`.

**Defer:** enforced-contract type checks (0 contract-enforced models in Swan — no near-term payoff),
`accepted_values`/`expression`/`unique_combination`, `SELECT *` expansion, and all MCP wiring.

---

## 9. Sequencing with shipped + planned surfaces

- **CI gate:** a strict *extension* of the shipped surface (reuses the sticky-comment machinery,
  extends the `FailOn` enum). Spends **zero** of the ≤2-new-surfaces/year budget.
- **Explorer:** no change — keep it as the demo/credibility artifact per the maintainer guardrail.
- **MCP (planned next):** build it *on top of* the verdict core — the `BreakFinding`/verdict JSON is
  already agent-shaped, so one truth feeds a CI gate (blocking) and an in-loop coding agent (offline
  JSON verdict). Do **not** ship MCP over today's wrong counts — that standardizes the lie.
- **Runner-up / sequel — "Ripple Fix":** don't just report the break, emit the git-applyable rename
  codemod + per-owner repair plan. Highest ceiling, perfect MCP payload — but remediation is only
  trustworthy once detection is. Sequence: *manifest-first → The Verdict → MCP → Ripple Fix*, all on
  one engine.

---

## 10. Honest risks

1. **Correctness is a prerequisite this feature doesn't own** — recall is bounded by the registry
   (8-of-175 today). A BLOCK that reads a real break as SAFE destroys trust faster than any false
   positive. → ship warn-only until manifest-first lands.
2. **Test→lineage precision is a false-positive minefield.** A test attached downstream only proves
   breakage if the changed upstream column *actually flows* into the tested column through renames /
   `SELECT *`. → require BFS-reachability; keep ambiguous cases advisory, never provable. One false
   BLOCK gets the gate disabled forever.
3. **Rename is *inferred* in dbt, not declared** (remove+add fingerprint match). A mis-detected rename
   over-BLOCKs. → only high-confidence renames feed provable diagnostics; shaky ones stay advisory.
4. **Test-node shape drift** — `column_name` sometimes lives in `test_metadata.kwargs`; singular /
   custom tests have no attached column; `dbt_utils`/`dbt_expectations` vary by version. → the index
   degrades honestly, counting "N unattributable tests" in the coverage floor rather than guessing.
5. **`relationships` tests are two-sided** (tested column vs `to`/`field` referenced column) —
   attributing a break to the wrong side risks a false FAIL. → conservative direction handling.
6. **Event-rate ceiling** — most PRs read SAFE/REVIEW; daily indispensability only fires on the rarer
   column-breaking PR. Owner routing + the compiler-diagnostic format keep it valued even in warn mode.

---

## 11. The through-line

**Crown the existing impact analysis with a provable tier — using dbt's own tests as the proof.**
The blast radius, exposures, and explorer stay; the test graph (the biggest unused asset on disk)
adds the one tier that turns *"here's the blast radius, go check"* into *"this will fail
`relationships_fct_payments_customer_id`, @jane owns it"* — the only signal objective enough to earn
a *blocking* gate, and one no warehouse-free competitor ships. Tests are the tip; the layered Impact
Report (§1) is the product.
