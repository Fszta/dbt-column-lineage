# Design system

The definitive design source of truth for `parrant`. Both surfaces — the
documentation site and the interactive explorer app — **must** follow this system so they
read as one product. Aesthetic: *engineering-grade editorial* (Linear / Stripe tier).

This is adopted, not a proposal. Implemented in the docs (`docs/stylesheets/custom.css`,
`docs/index.md`); the explorer app is being brought into line (see "App alignment" below).

---

## Tokens

| Role | Value |
|---|---|
| Display + body type | **DM Sans** — bold with tight `-0.03em` tracking for display |
| Mono / engineering voice | **JetBrains Mono** — eyebrows, labels, code, SQL |
| Serif | **none.** Fraunces is banned — it reads as the over-used "AI serif" look |
| Accent | a single, rationed **electric indigo** `#5E6AD2` (dark: `#8A94EC`) |
| Neutrals | cool slate ramp |
| Spark | warm **amber** `#f59e0b`, used once per view (e.g. severity / a broken edge) |
| Gradients | **none** — no gradient text, no gradient buttons |

## Principles

- One typeface family carries display and body; mono is the second voice.
- Color is rationed: indigo on interactive/active state and active graph edges only; amber
  once. Everything else is slate on paper/ink.
- Flat surfaces, hairline `--border` dividers, whisper shadows.
- Whitespace is a material. Generous section spacing; a set type scale, held to.
- The product is the hero — frame the real lineage graph, don't decorate around it.
- Both light and dark are first-class (toggle; the app flips via `[data-theme="dark"]`).

## Lineage graphs must be realistic dbt

Any illustrative DAG models real dbt shape: source → staging **1:1**, fan-in at the **mart**
layer, fan-out to exposures + downstream marts. Never multiple sources into one staging model.

---

## App alignment — required work

Explorer CSS: `parrant/lineage/display/html/static/css/`. `base.css` already
shares most tokens (indigo, slate, amber, DM Sans, JetBrains Mono). These are the gaps.

### Fonts (do first)

1. `base.css` line 1 `@import`: remove the `Fraunces:...` segment; keep DM Sans + JetBrains
   Mono. Add `--font-display: 'DM Sans', ...` (its own token). Alias `--font-serif` to it
   during migration, then delete `--font-serif`.
2. Repoint serif headings to `--font-display`, weight `700`, `letter-spacing: -0.03em`:
   - `impact.css:125` — `.impact-question` (the "what breaks if…" impact heading)
   - `graph.css:392` — `.graph-empty-state h3`
3. Replace hardcoded `'Monaco','Menlo','Ubuntu Mono', monospace` with `var(--font-mono)`:
   - `impact.css` lines `474`, `621`, `1185`, `1305` (SQL / expression chips)

**Acceptance:** `grep -rn "Fraunces\|font-serif\|'Monaco'" static/css/` returns nothing
outside token definitions.

### Editorial treatment

- [ ] Display headings: DM Sans bold, tight tracking, larger scale (impact title, empty
      states, section heads).
- [ ] Eyebrows: mono, uppercase, `0.16em` tracking, muted — above section titles.
- [ ] Accent discipline: indigo rationed to interactive/active + active edges; amber once.
- [ ] Surfaces: flat cards, hairline dividers, no gradients.
- [ ] Spacing: adopt the docs' section rhythm / type scale.
- [ ] Code/SQL blocks: match the docs terminal chrome (dark, traffic-lights, JetBrains
      Mono, indigo `$`, amber selectors).
- [ ] Dark-mode parity.

### Out of scope

- D3 graph layout / interaction logic — visual tokens only.
- The docs site (already done).

---

## References

- Docs implementation to mirror: `docs/stylesheets/custom.css`, `docs/index.md`.
- The docs redesign PR establishes this system: **#71** (held unmerged until the app
  alignment above is ready, so both surfaces land coherent).
