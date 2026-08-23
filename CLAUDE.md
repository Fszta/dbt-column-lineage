# CLAUDE.md

Guidance for working **on** this repository (the `parrant` Python codebase).

## What this project is

`parrant` is a CLI for **column-level lineage and change-impact analysis**
of dbt projects. It answers "what breaks if I change this column?" It **only reads
dbt artifacts** (`manifest.json`, `catalog.json`) — it never runs dbt. SQL is parsed
with `sqlglot`. Output is human text, DOT/graphviz, an interactive HTML explorer, or
machine-readable JSON/Markdown (designed to feed AI agents).

## Architecture — the pipeline

Data flows in one direction; keep new code in the layer that owns the concern.

```
artifacts/  → parse & load dbt manifest + catalog, adapter mapping, model registry
parser/     → sqlglot-based SQL parsing (sql_parser.py) + helpers
lineage/    → core logic: service.py (lineage traversal), changeset.py (diff/impact),
              ci.py (CI gating), display/ (text, dot, json, markdown, html explorer)
models/     → pydantic schema (models/schema.py) — the shared data types
cli/        → main.py, Click entrypoint that wires everything together
```

Entrypoint: `parrant` → `parrant.cli.main:main`.
Selector grammar: `+model.col` = upstream, `model.col+` = downstream, `model.col` = both.
Change kinds ranked by blast radius: `removed` > `type_changed` > `logic_changed` > `added`.

## Commands (Poetry scripts — always via `poetry run`)

| Task | Command |
|---|---|
| All tests | `poetry run test` |
| Unit / integration / e2e only | `poetry run test-unit` · `test-integration` · `test-e2e` |
| Format (black + ruff) | `poetry run format` |
| Type check (mypy) | `poetry run type-check` |
| Run the CLI | `poetry run parrant --select model.col+ --format json` |

Test tiers: **unit** (`tests/unit`, per-module, no artifacts needed) · **integration**
(`tests/integration`, cross-module) · **e2e** (`tests/e2e`, drives the CLI end-to-end).
Test fixtures / a sample dbt project live in `tests/resources/dbt_test_project`.

## Conventions — must follow

- **Line length 100** for both black and ruff.
- **Python 3.10 floor** (`requires-python >=3.10`, mypy pinned to 3.10). Do **not** use
  3.11+ syntax. 3.10 features (`X | Y` unions, `match`) are allowed, but existing code
  favors `typing.Optional`/`Union` — match the surrounding style rather than mixing both.
- **Type everything.** mypy runs over `parrant` and `tests` with
  `show_error_codes`, strict-ish settings; new code must pass `poetry run type-check`.
- **Conventional Commits**, enforced by commitlint (`.commitlintrc.json`) as a
  commit-msg hook. This gates `release.yml` / CHANGELOG — non-conforming messages break
  releases. Use `feat:`, `fix:`, `chore:`, `docs:`, etc.
- Data types are **pydantic** models in `models/schema.py` — extend those rather than
  passing loose dicts.
- **Design system is `DESIGN.md`** (definitive). Any UI work on the docs site *or* the
  explorer app must follow it: DM Sans + JetBrains Mono, single indigo accent, cool slate,
  no serif, no gradients. Both surfaces must stay visually one product.
- Run `pre-commit` (or `poetry run format && poetry run type-check && poetry run test`)
  before pushing — CI (`.github/workflows/test.yml`) runs the same checks.

## `skills/` is a PRODUCT, not dev tooling

`skills/dbt-lineage/` is a **Claude Code plugin shipped to end users** of the CLI
(analysts/engineers running impact analysis). It is not tooling for developing this
repo. When editing those `SKILL.md` files, match the existing frontmatter
(`name`, `description`) and section structure (Inputs → Steps → Output), and keep them
in sync with the CLI surface they wrap. Contributor tooling lives in `.claude/` and this file.

## Contributor tooling (`.claude/`)

- `/check` (command) — pre-push gate: format + type-check + tests.
- `/new-skill` (command) — scaffold a new *product* skill matching the house style.
- `check-explorer` (skill) — launch the interactive explorer against the bundled test
  project and QA the UI (API smoke-tests + browser pass). Auto-triggers after edits under
  `parrant/lineage/display/`. This is dev QA, not the product `explore-lineage`.
