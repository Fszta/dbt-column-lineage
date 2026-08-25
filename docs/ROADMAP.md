# Roadmap

A living, high-level view of what has shipped and what is planned for `parrant`'s
metadata-agnostic **policy gate** and impact analysis. This file was created alongside the
`config` axis; it is intentionally short and curated, not exhaustive — the
[Changelog](changelog.md) is the authoritative per-release record (versions are cut by the
maintainer via `release.yml`, so this page does not pin release numbers).

## Shipped

### Policy predicate axes

The gate matches a leaf condition on one of these axes (see the
[policy gate guide](decision-engine/policy-gate.md)):

- **`change`** — facts about the changed column (kind / semantic / breaking / model / column).
- **`meta`** — any dbt `meta` key on the changed model or column (dotted paths).
- **`inferred_meta`** *(shipped)* — a meta key resolved by **folding UPSTREAM lineage**, so a
  classification declared once (e.g. `pii: true` on a staging column) is inherited by every
  downstream column that derives from it, with column-level declassification. Shipped in
  #119 and renamed `inferred` → `inferred_meta` in #120.
- **`config`** *(new — this release)* — each model's resolved dbt `node.config`
  (`grants.select`, `materialized`, `tags`, `enabled`, `schema`, …) by dotted key, mirroring
  `meta`. Set-operator missing paths resolve to the **empty set** (present, not unknown);
  scalar missing paths resolve to `UNKNOWN` (routed to `on_missing_meta`). This makes
  "sensitive data must not be granted to a role outside an allowlist" expressible directly via
  `config.grants.select not_subset_of [...]`, with no manifest-patch bridge — it composes with
  `inferred_meta.pii` for the offline PII-exposure rule (example `pii_grants_allowlist.yml`).
- **`reach`** — a quantified condition over the change's downstream reach (models / columns /
  exposures), including Metabase dashboards as `kind: exposure`.
- **`structural`** — booleans the pipeline computes (`provable_test_break`, `touches_exposure`,
  `reaches_anything`).

### Other shipped capabilities

- Fail-safe three-valued evaluation with independent `on_missing_meta` / `on_error` knobs and
  built-in `on_meaning_changed` / `on_indeterminate` semantic-severity defaults.
- `policy init` (manifest-introspecting, safe-by-construction scaffold) and `policy test`
  (offline backtest with a per-rule fail-safe-UNKNOWN trust column).
- In-code override pragmas (`-- lineage:allow-change` / `-- lineage:allow-break`).
- Cross-boundary Metabase reach (`metabase-extract` artifact + offline join).

## Next / under consideration

- **Per-commit base-manifest fidelity for `policy test`** — exercise the provable-break and
  semantic meaning-change block tiers during a backtest (today they need a per-commit
  before-state; see the fidelity note).
- **External-input release paths** — `block-until-acknowledged` (owner sign-off via a PR label)
  and `block-until-proven` (a `/data-diff` result). Both would require the gate to consume a new
  external input, cutting against the offline / zero-credential guarantee — noted as possible
  future work, deliberately unbuilt today.
- **Column-grained `config`** — `config` is model-grained today (dbt config is a model-level
  notion); a column-level surface would only follow if a real per-column config need appears.
