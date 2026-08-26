# Cross-boundary impact (Metabase)

dbt lineage stops at dbt's edge. But the question a reviewer actually cares about is one hop
further out:

> **Will this column change break *that dashboard*?**

The cross-boundary feature follows impact **past dbt's edge into your BI layer** — from a changed
dbt column, through the BI cards that read it, to the dashboards those cards live on — and folds
those dashboards into the *same* reach model the policy engine already scans. A reached dashboard
becomes just another reachable **exposure**.

!!! info "Metabase is the first supported BI connector"
    The cross-boundary *model* is **BI-tool-agnostic** — a reached dashboard is just an `exposure`,
    matched by the same policy [reach axis](policy-gate.md#reach-conditions) with no new surface.
    **Metabase** is the connector shipped today (and currently the only one); adding another BI
    tool means a new *extractor* that writes the same shape of snapshot, not new policy concepts.
    This guide covers the **Metabase** connector specifically — its extract step, artifact, and
    resolvers.

## The two-step model

Impact runs must stay **offline and zero-credential** (they read only your dbt artifacts). So
Metabase lineage is captured exactly like dbt's own artifacts: a separate, credentialed step
produces a snapshot file that the gate then consumes offline.

```
  ┌── credentialed, scheduled ──┐        ┌──────── offline, zero-credential ────────┐
  Metabase API                            manifest.json + catalog.json
      │                                          │
  metabase-extract  ──►  metabase_lineage.json ──►  impact --metabase …  ──►  verdict
      │                    (the snapshot = the store)
  (the only step with credentials)
```

1. **`metabase-extract`** (credentialed) snapshots Metabase into `metabase_lineage.json`.
2. **`impact --metabase metabase_lineage.json`** (offline) joins that snapshot into the dbt reach
   and gates on it — **no Metabase credentials** at gate time.

The offline guarantee is structural: the gate path imports only the artifact reader and the join
logic, never the credentialed client.

## Step 1 — `metabase-extract`

Run this on a schedule (e.g. the same job that publishes your dbt artifacts). It hits the
Metabase API, resolves every card down to warehouse `schema.table.column`, attaches the
dashboards each card appears on, and writes the snapshot.

```bash
parrant metabase-extract \
  --metabase-url https://metabase.example.com \
  --metabase-api-key "$METABASE_API_KEY" \
  --database-id 2 \
  --manifest target/manifest.json \
  --output metabase_lineage.json
```

| Option | Purpose |
|---|---|
| `--metabase-url` | Metabase base URL (env: `METABASE_URL`). Required. |
| `--metabase-api-key` | API key auth, v0.49+ (env: `METABASE_API_KEY`). |
| `--metabase-username` / `--metabase-password` | Session-auth fallback (env vars of the same name). |
| `--database-id` | Restrict to these Metabase database id(s) — the warehouse dbt targets. Repeatable. Required. |
| `--manifest` | dbt `manifest.json` — supplies the SQL dialect for the native-SQL resolver. Required. |
| `--adapter` | Override the SQL dialect for the native resolver. |
| `--output` / `-o` | Snapshot path (default `metabase_lineage.json`). |
| `--include-archived` | Include archived cards. |
| `--dashboard-meta-file` | JSON mapping dashboards → consumer `meta` (see below). |
| `--fail-under` | Exit non-zero if resolution coverage is below this ratio. |
| `--previous` | A prior `metabase_lineage.json` snapshot for **incremental reuse** — dashboards whose `updated_at` is unchanged skip the detail fetch (see below). |
| `--max-workers` | Concurrency for the dashboard detail fan-out (default `8`). |

Credentials come from env/flags and are **never** written into the snapshot — only the non-secret
base URL is stamped in provenance.

### Two resolvers, one anchor

- **Query-builder (MBQL) cards** resolve column-precise from Metabase's structured Field/Table
  metadata — no SQL parsing.
- **Native-SQL cards** are parsed with the same SQLGlot engine the tool already uses, with
  template tags (`{{snippet}}`, `{{#card}}`) expanded first. Ambiguous SQL (`select *`, complex
  queries) **degrades to table grain** rather than dropping the card.

Both terminate at a warehouse relation, which joins to a dbt model via the manifest's
`relation_name`. From there the existing column-lineage carries the change forward.

### Where dashboard `meta` comes from

Metabase dashboards don't natively carry rich `meta` (like `tier` or `owner`). You supply that
mapping — the tool never hardcodes a taxonomy — via `--dashboard-meta-file`, a JSON document
keyed by collection or dashboard:

```json
{ "by_collection": { "4": { "tier": "executive", "owner": "cfo-office" } },
  "by_dashboard":  { "55": { "tier": "executive" } } }
```

Those keys become the `meta.*` your policy `reach.where` matches on.

### Coverage honesty

`metabase-extract` prints how much it resolved:

```
Wrote metabase_lineage.json: 812 cards (604 column-precise, 158 table-only,
50 unresolved), 141 dashboards. Coverage 94%.
```

- **column-precise** — the exact column is known.
- **table-only** — the card reads the changed column's *table* but the exact column couldn't be
  proven (`select *` etc.). Still a valid dashboard-reach signal.
- **unresolved** — no warehouse relation resolved; counted, never guessed.

Use `--fail-under 0.8` to fail the extract if resolution drops below a threshold.

### Scaling the extract — scoping, concurrency, incremental reuse

On a large instance (hundreds of dashboards, thousands of cards) the extract does three things
to stay fast and honest:

- **Connection scoping now _filters_ cards.** A card whose query targets a Metabase database
  outside `--database-id` is **dropped** — absent from the snapshot and not counted in coverage
  — rather than resolved against the wrong warehouse. (Previously every card was included; a
  foreign-connection card only polluted the artifact and dragged coverage down.) A malformed card
  with no `database` at all keeps the old behaviour of being resolved. A dashboard left with no
  in-scope cards is dropped entirely.
- **Dashboards are fetched concurrently.** The per-dashboard detail fetch (`dashcards`) is the
  N+1 cost at scale, so it fans out over a bounded thread pool. Tune with `--max-workers`
  (default `8`); a single dashboard or `--max-workers 1` runs sequentially. A per-dashboard
  failure fails the whole extract loudly — a partial snapshot is never emitted.
- **Incremental reuse skips unchanged dashboards.** Pass yesterday's snapshot via `--previous`
  and any dashboard whose Metabase `updated_at` matches the previous snapshot is **reused**
  without a detail round-trip. Reuse is still honest: card ids are re-intersected against the
  currently in-scope cards (a card that left the connection is dropped from the reused dashboard),
  and `meta` is **always** recomputed from the fresh `--dashboard-meta-file` mapping — never
  copied from the previous snapshot — so a taxonomy change lands even on a reused dashboard.
  Reuse requires the **same `--database-id` scope** as the `--previous` snapshot: if the scope
  changed, reuse is disabled and the run does a full (correct) refetch, so a card newly entering
  scope on an unedited dashboard is never silently missed. (A missing `--previous` path is treated
  as a cold start too, so a scheduled job can pass the flag unconditionally.)

This pairs directly with the daily S3 pattern below: **download yesterday's snapshot, pass it as
`--previous`, upload the new one.** The first run of the day still pays the full fetch; subsequent
runs only fetch the dashboards that actually changed.

!!! note "Snapshot schema is now `schema_version: 2`"
    v2 stamps a per-card / per-dashboard `updated_at` so a later extract can reuse unchanged
    entities. The offline gate reads **both** v1 and v2 snapshots — a v1 snapshot (no `updated_at`)
    loads unchanged and simply can't be used as a `--previous` base for reuse (everything refetches).

    **Rollout order: upgrade consumers before producers.** Every new extract now emits v2. An
    older `parrant` install only accepts v1 and will **hard-fail** on a v2 snapshot (by design —
    the gate never silently drops reach). So upgrade the `parrant` on your PR-gate runners to a
    v2-aware version *before* the scheduled job starts publishing v2 snapshots to the shared store.

### Scheduling & where the snapshot lives

`metabase-extract` is a **one-shot command** — the tool ships no scheduler and the gate never
triggers it. Running it on a cadence, and putting `metabase_lineage.json` somewhere the gate can
read it, is **your** deployment choice. The snapshot is a **build artifact** (like dbt's own
`manifest.json` / `catalog.json`), not source you edit.

**Recommended — publish to an artifact store, fetch at gate time.** Run the extract on a schedule
(daily is usually plenty — Metabase changes less often than your models), ideally in the same job
that publishes your dbt artifacts, and upload the snapshot to the same store (S3/GCS, or a CI
artifact). The PR check downloads the latest and runs `impact --metabase`:

```
 scheduled job (e.g. daily)                    PR check (per push)
 ──────────────────────────                    ───────────────────
 metabase-extract  →  s3://…/metabase_lineage.json  →  (download)  →  impact --metabase … --policy …
 (credentials live here only)                          (offline, zero-credential)
```

This keeps git clean, confines credentials to the scheduled job, and — importantly — gates each PR
against the **latest production** Metabase, not a snapshot frozen to whenever the branch was cut.

!!! tip "Why not commit the snapshot to the repo?"
    Auto-committing a regenerated JSON on a schedule works, but it churns git history, ties the
    snapshot's freshness to the PR branch, and needs a bot with repo-write + credentials. If you
    do commit it (for simplicity, no artifact store), put it on a **dedicated branch/path**, not
    on `main`, so it never fights your source history or open PRs.

**Freshness is your responsibility, but the gate is honest about it.** If the scheduled job misses
a run, the snapshot is flagged **stale** (older than 24h) — but the reach is **still used in full**:
every reached dashboard is gated on exactly as if fresh. The flag is a warning, not a discard, so a
block driven by an old snapshot is *visible* rather than silently trusted-as-fresh. (Only a
*missing* snapshot falls back to dbt-only reach — see [Step 2](#step-2-gate-on-the-snapshot-offline).)
Pair the flag with `--fail-under` on the extract so a degraded snapshot never ships in the first
place.

## Step 2 — gate on the snapshot (offline)

Feed the snapshot to the impact run. Metabase dashboards that read a changed column (directly, or
via its dbt downstream) surface as **exposure-kind reach**, matchable by a policy `reach` rule.

```bash
parrant impact \
  --manifest target/manifest.json --catalog target/catalog.json \
  --base-manifest base/manifest.json --base-catalog base/catalog.json \
  --metabase metabase_lineage.json \
  --policy policy.yml --fail-on policy
```

A present-but-invalid snapshot fails loudly; a missing one degrades gracefully to dbt-only reach.

!!! note "The GitHub Action doesn't wire `--metabase`"
    The composite action exposes `policy` but not `metabase`. To gate on cross-boundary reach in
    CI, invoke the CLI directly in a step (`parrant impact --metabase … --policy … --fail-on policy`)
    after fetching the snapshot, rather than using the action's inputs.

## Writing a cross-boundary policy rule

Because Metabase dashboards arrive as `kind: exposure`, they need **no new policy surface** — the
[reach axis](policy-gate.md#reach-conditions) already handles them. Each dashboard carries
`source: metabase`, its `meta`, the `via_cards` it was reached through, and the reach `precision`
(`column` or `table`).

### Example — a breaking change reaching an executive dashboard must block

```yaml
version: 1
# A BREAKING dbt column change that reaches a Metabase dashboard tagged meta.tier=executive
# must BLOCK — the CFO board cannot silently break.
rules:
  - id: breaking-reaches-executive-dashboard
    description: >
      A breaking change that reaches an executive-tier Metabase dashboard must block and
      notify the exec data channel.
    scope: change
    on_missing_meta: fail_open
    predicate:
      all:
        - change: { field: breaking, op: is_true }
        - reach:
            kind: exposure
            where:
              all:
                - meta: { key: source, op: eq, value: metabase }
                - meta: { key: tier, op: eq, value: executive }
            min_count: 1
    action:
      - type: block
      - type: notify
        channel: slack
        target: "#data-exec-alerts"
        message: "BLOCK {change.model}.{change.column} breaks {reach.count} executive dashboard(s)"
```

Table-grain reach still fires this rule — the dashboard is reached even when the exact column
isn't proven, which is the buyer-facing answer. A stricter, column-precise variant can add
`reach.mechanism: [rowset_filter]` where native column resolution is column-precise.

### Isolating Metabase-only rules

This is the key pattern. To fire **only** on Metabase dashboards (not dbt-native exposures),
combine two things:

1. `on_missing_meta: fail_open` on the rule, and
2. a `meta: { key: source, op: eq, value: metabase }` guard in the reach `where`.

Why both? dbt-native exposures don't carry a `source` meta. Under the default `fail_closed`, that
missing key would be treated as risk and the rule would block on dbt exposures too. `fail_open`
makes a missing key resolve `False`, so the `source == metabase` guard reliably narrows the rule
to Metabase-sourced reach. The reach carries `source: metabase` provenance precisely so this guard
is dependable.

## Coverage and staleness honesty

Cross-boundary reach is only as trustworthy as the snapshot behind it, so the tool surfaces two
independent honesty signals — the same discipline it applies to dbt's own coverage:

- **Snapshot coverage** — column-precise vs table-only vs unresolved card counts. A table-only
  reach is reported *as reach* but flagged not-column-precise.
- **Snapshot staleness** — the snapshot's `generated_at` age. A **stale** snapshot (older than the
  threshold) is **still used in full** — the reach is gated on exactly as if fresh — but flagged
  **degraded, not authoritative**, so a block driven by an old snapshot reads as such. It is never
  discarded and never silently trusted-as-fresh. (An **absent** snapshot is the only case that
  falls back to dbt-only reach.) Either way, dashboard reach is never fabricated.

The impact report renders a line such as *"Metabase reach: 3 dashboards (2 table-grain); snapshot
18h old"* so that a fail-closed block driven by a stale or coarse snapshot reads as such, rather
than as a fabricated certainty.
