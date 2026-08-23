---
name: check-explorer
description: Launch the parrant interactive HTML explorer against the bundled test project and verify the UI actually works end-to-end — API endpoints return well-formed data, the D3 graph renders, and the column-lineage + Analyze Impact flows respond. Use when the user asks to "check/verify/QA the explorer", "did my change break the UI?", "does the graph still render?", or after editing anything under parrant/lineage/display/ (explore.py, templates/, static/js/, static/css/). This is a dev/contributor QA skill — distinct from the product `explore-lineage` skill, which only *opens* the explorer for an end user and does no verification.
---

# Check explorer (dev QA)

Verify the interactive explorer (FastAPI + D3) after changes to the display layer.
Two layers of checks: fast **API smoke-tests** (reliable, always run) and a **browser
pass** (visual + interaction, when a headless browser is available). The explorer is
self-contained — `scripts/render_start.py` builds artifacts from the bundled
`tests/resources/dbt_test_project`, so no external dbt project is needed.

## 1. Launch (background, self-contained)

Pick a non-default port to avoid clashing with a real explorer the user may be running.

```bash
PORT=8899 poetry run python scripts/render_start.py > /tmp/check-explorer.log 2>&1 &
```

Then **poll for readiness** — do not assume it's up (artifact build takes a few seconds):

```bash
for i in $(seq 1 30); do
  curl -sf -o /dev/null http://127.0.0.1:8899/api/graph && { echo "READY"; break; }
  sleep 1
done
```

If it never becomes ready, `tail /tmp/check-explorer.log` — a failed artifact build or a
port collision is the usual cause.

## 2. API smoke-tests (always run these)

The explorer exposes these routes (see `parrant/lineage/display/html/explore.py`).
Assert each returns `200` and well-formed JSON:

| Endpoint | What to assert |
|---|---|
| `GET /` | `200`, HTML |
| `GET /api/graph` | keys `nodes`, `edges`, `main_node`, `column_info`, `impact_summary`; `nodes`/`edges` non-empty |
| `GET /api/coverage` | keys `models_in_manifest`, `models_in_catalog`, `parsed_ok`, `parse_failed` (the coverage/confidence banner) |
| `GET /api/models` | non-empty list/tree |
| `GET /api/lineage/{model}/{column}` | resolves for a known model.column |
| `GET /api/impact-analysis/{model}/{column}` | returns an impact object (includes `confidence`) |

A regression in the parser/lineage layer usually shows here first (empty `nodes`, a
non-200, or `parse_failed > 0` in coverage) — cheaper to catch than via the browser.

## 3. Browser pass (visual + interaction)

Drive the UI with the built-in **`browse`** / **`verify`** skills — don't hand-roll a
browser. Load `http://127.0.0.1:8899/` and verify against these real DOM anchors
(from `templates/graph.html`):

1. **Sidebar renders** — `#model-tree-container` populated; `#graphEmptyState` shown before selection.
2. **Column lineage flow** — pick a model in the tree → open `#columnSelectTrigger` /
   choose from `#columnSelect` → click `#loadLineage` → the `#graph` SVG paints nodes/edges
   (no empty state, no console errors).
3. **Analyze Impact flow** — trigger `#loadImpactAnalysisFromCard`; `#impactAnalysisPanel`
   opens and `#impactAnalysisContent` fills with review-required vs pass-through columns,
   affected models, and exposures.
4. **Controls** — `#zoomIn` / `#zoomOut` / `#resetView` / `#relayout` respond.
5. Capture a **screenshot** of the rendered graph as the artifact of record, and report any
   browser console errors.

## 4. Teardown (important)

`poetry run` spawns a **child uvicorn** that outlives the parent PID, so kill by port:

```bash
pkill -9 -f "render_start.py"; lsof -ti tcp:8899 | xargs -r kill -9
```

Confirm the port is free (`curl` fails) before finishing.

## Output

- **PASS/FAIL per layer** — API smoke-tests and each browser check, with the failing
  endpoint/selector and the relevant log or console line on failure.
- The graph screenshot.
- If green: state the explorer renders and the lineage + Analyze Impact flows work against
  the bundled test project. If red: point at the layer (API = parser/lineage regression;
  browser-only = template/JS/CSS regression) to localize the fix.
