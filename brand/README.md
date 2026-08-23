# parrant — brand assets

Staged brand assets for the **parrant** rebrand (formerly `dbt-col-lineage`).
Direction **"The Verdict"**. Drop these into the tree during the rename migration.

## The mark

A lineage edge enters, meets the **decision node** (the indigo vertex), and is deflected
into an upstroke: the bend is the **parry**, the resulting check is the **warrant**. Three
graph nodes keep it in the product's node-and-edge lineage language, not a generic checkmark.

On-spec with `DESIGN.md`: Lucide-weight stroke (`stroke-width: 2`, round caps/joins),
single rationed indigo, no gradients, no serif. **Amber is intentionally absent** — it stays
reserved for severity in the UI.

## Files → destinations

| File | Purpose | Destination in the rename |
|---|---|---|
| `logo.svg` | Primary logo, **mono `currentColor`** — adapts to any nav/header in both themes | replace `docs/assets/logo.svg` |
| `favicon.svg` | Browser tab, **flat indigo `#5E6AD2`** | replace `docs/assets/favicon.svg` |
| `logo-accent.svg` | Logo with theme-aware indigo vertex (`#5E6AD2` light / `#8A94EC` dark) — for the docs hero / README on a plain ground | optional, e.g. `docs/assets/logo-accent.svg` |
| `avatar.svg` | 512×512 rounded indigo tile, white mark — GitHub org/repo avatar + OG/social image | export to PNG for GitHub (upload in Settings) |
| `wordmark.svg` | Horizontal lockup (mark + `parrant` in DM Sans) — docs hero, README header | optional, e.g. `docs/assets/wordmark.svg` |

`logo.svg` and `favicon.svg` are already the paths referenced by `mkdocs.yml`:

```yaml
theme:
  logo: assets/logo.svg
  favicon: assets/favicon.svg
```

No `mkdocs.yml` change needed if you keep those filenames — just overwrite the files.

## Tokens (for reference)

| Role | Light | Dark |
|---|---|---|
| Accent (indigo) | `#5E6AD2` | `#8A94EC` |
| Ink (slate) | `#1B1D28` | `#E9EAF0` |
| Amber (severity — **not** used in the mark) | `#F59E0B` | `#F59E0B` |

## Notes for the migration agent

- These are **staged** (`brand/`), not yet wired — they were produced on
  `feat/gate-ergonomics-stack` ahead of the rename. Move/copy them to their destinations
  on the rename branch; the `brand/` dir can be deleted afterward or kept as source.
- `wordmark.svg` uses live `<text>` in DM Sans; it renders correctly on the docs site
  (which loads DM Sans) but **convert the text to outlines** if you need a standalone,
  font-independent asset.
- For the GitHub avatar, rasterize `avatar.svg` to a 512×512 PNG (GitHub doesn't accept SVG
  avatars). Same file works as a 512×512 OG/social image or as a base for a 1200×630 card.
- Geometry is identical across all files (`M4 12.5 L9.6 18.2 L20 5.2`, vertex at
  `9.6,18.2`) so they stay pixel-consistent.

Preview of all directions considered:
https://claude.ai/code/artifact/ba3ced28-f0f1-471a-a2ae-ff7d99ebc299
