# The Explorer with change context

The interactive explorer (`--explore`) has always shown column lineage and the *"what breaks if
I change this column?"* impact panel. It now also surfaces the decision-engine signals — the
semantic categorization, the policy verdict, and cross-boundary Metabase reach — when you give it
a **change context** to work from.

## Turning on change context

In plain `--explore` mode (no diff) the explorer looks exactly as it always has. To light up the
new surfaces, point it at a changeset the same way the `impact` command does. These options apply
**only** with `--explore`:

```bash
# Two-manifest diff — the reliable source
parrant --explore \
  --base-manifest base/manifest.json --base-catalog base/catalog.json \
  --policy policy.yml \
  --metabase metabase_lineage.json
```

| Option | What it adds |
|---|---|
| `--base-manifest` | Base (target-branch) manifest for a two-manifest diff — enables the change context. |
| `--base-catalog` | Base catalog paired with `--base-manifest` (defaults to a `catalog.json` next to it). |
| `--git-base <ref>` | Alternative: derive the changeset from a git diff against a ref, when no base manifest is available. |
| `--policy <path>` | Evaluate the policy so the explorer can show the verdict. |
| `--metabase <path>` | Load a `metabase_lineage.json` snapshot so the explorer surfaces dashboard reach. Consumed **offline**. |

When a change context loads, the server confirms it on startup — e.g.
`Change context loaded: 6 changed column(s), policy=block`. The explorer then shows exactly the
signals the CI gate would, computed from the same primitives.

## What you'll see

- **Policy panel.** A dedicated panel for the whole-change verdict, with a pinned banner:
  `block` reads as a **red stop**, `warn` as **amber**, `allow` as a neutral check. Each fired
  rule expands to show the subject change and the reach it matched, and the selective build/test
  sets are copy-able (as `dbt build --select …` chips).
- **Semantic badges.** Changed columns in the impact panel carry a semantic chip: **breaking**
  columns get the amber caution spark; **proven-equivalent** columns get a de-emphasized neutral
  check — the good news is deliberately quiet. A column whose semantic is indeterminate is drawn
  as breaking (fail-safe: the UI never renders "unknown" as "safe").
- **Cross-boundary cards.** In the affected-exposures list, Metabase dashboards render as a
  distinct card variant — tagged *via Metabase*, with a `View dashboard` link, a `meta.tier` tag,
  and a "table-level" caption when the reach is table-grain rather than column-precise.
- **Confidence pip.** A compact confidence dot sits next to the related-exposures tile on the
  relationship summary card: neutral + check when the analysis is complete, amber when it's
  partial — with a tooltip explaining the gap. The full confidence badge and coverage footer in
  the impact panel are unchanged.

!!! note "Colour discipline"
    Amber is rationed to a single caution axis — breaking / warn / partial-confidence all share
    it and never compete in one view. The one escalation above amber is a policy **`block`** (the
    red stop). This keeps the reading "amber once, plus a red stop when the gate actually blocks".

## Status

The panels above (policy panel, semantic chips, cross-boundary exposure cards, confidence pip)
are shipped. The **graph-canvas marks** — amber blast-path edges, a dedicated BI/Metabase
boundary band with dashboard nodes, and block/warn rings on the subject node — are a planned
polish layer: the API already emits the data (`semantic`/`breaking`/`boundary` on nodes, `source`
on exposures), and the panels already deliver the information at full fidelity.
