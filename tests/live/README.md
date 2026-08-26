# Live-Metabase end-to-end tier (`tests/live/`)

An **opt-in**, out-of-gate test tier that boots a **real Metabase**, seeds content through
the REST API, runs `parrant metabase-extract` against it, and asserts a
**non-zero-coverage** extract.

## Why it exists

Metabase's API *serves cards back in the running version's serialization*: legacy MBQL on
older versions, **pMBQL / MBQL-5 on v0.57+**. That drift silently breaks card resolution
(coverage drops to zero) unless the resolver keeps up. This tier boots a **version matrix**
of real Metabases so that regression turns CI red automatically — something a mocked unit
test can't catch, because the mock can't drift.

## Why it's opt-in / out of the gate

The per-push gate (`.github/workflows/test.yml`) runs only the directory tiers
`tests/unit`, `tests/integration`, `tests/e2e` via `scripts/run_tests.py`. `tests/live/`
is **deliberately not** one of those, and the test module additionally **skips** unless
`RUN_METABASE_LIVE=1`. It's slow (Metabase takes 30-90s to boot), needs Docker, and depends
on a moving external surface — so it runs **nightly + on demand** via
`.github/workflows/metabase-live.yml`, never on push/PR.

The tier never manages the container from inside pytest. The **caller** (CI or you) owns the
Metabase lifecycle and points the tests at it with `METABASE_URL`. If `METABASE_URL` is
unset, the tests skip.

## Run it locally

```bash
# 1. Boot a Metabase (any tag; :latest is pMBQL).
docker run -d -p 3000:3000 --name parrant-metabase metabase/metabase:latest

# 2. Wait until healthy (Metabase takes 30-90s to boot).
until curl -sf http://localhost:3000/api/health | grep -q '"status":"ok"'; do sleep 3; done

# 3. Run the tier. The fixtures run first-boot /api/setup to create the admin, then seed.
RUN_METABASE_LIVE=1 METABASE_URL=http://localhost:3000 poetry run pytest tests/live -v
```

Against an **already-set-up** instance you can skip the setup step by providing credentials:

```bash
RUN_METABASE_LIVE=1 \
METABASE_URL=http://localhost:3000 \
METABASE_API_KEY='mb_...' \
poetry run pytest tests/live -v
# or: METABASE_USERNAME=you@example.com METABASE_PASSWORD='...'
```

### Environment variables

| Variable | Required | Purpose |
|---|---|---|
| `RUN_METABASE_LIVE` | yes (`1`) | Master opt-in; without it the module skips. |
| `METABASE_URL` | yes | Base URL of the running Metabase (e.g. `http://localhost:3000`). |
| `METABASE_API_KEY` | no | Preferred auth if the instance is already set up. |
| `METABASE_USERNAME` / `METABASE_PASSWORD` | no | Session auth; also the admin created on a fresh container (defaults `admin@example.com` / `Parrant_Live_123!`). |

## What it seeds & asserts

Seeds against the built-in **Sample Database** (no warehouse / DuckDB driver needed):

- a **native** card whose SQL references the dbt-style relation
  `test.main.stg_transactions` (resolved from SQL via sqlglot — the table need not exist);
- an **MBQL** card on the real `ORDERS` table using real Table/Field ids;
- a **dashboard** showing both.

Then runs `parrant metabase-extract` and asserts: exit code 0; the artifact parses
(schema-validated); `coverage.cards_total >= seeded cards`; **non-zero coverage**
(`cards_resolved_column + cards_resolved_table_only > 0`); the native card resolved
column-precise to `test.main.stg_transactions` with `transaction_id` + `account_id`; and the
seeded dashboard is present.
