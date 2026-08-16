---
description: Pre-push gate — run format, type-check, and tests; report and fix failures.
---

Run the full local quality gate for this repo, in order, and stop to report if a step fails.

1. **Format** — `poetry run format` (black + ruff). If it reformats files, note which.
2. **Type check** — `poetry run type-check` (mypy over `dbt_column_lineage` and `tests`).
3. **Tests** — `poetry run test` (all tiers). If you only touched one layer and want a
   faster loop, you may run the matching tier instead (`test-unit` / `test-integration` /
   `test-e2e`) — but run the full suite before declaring the gate green.

Rules:
- Run the steps sequentially. If a step fails, **stop and report the failure** with the
  relevant output rather than pushing past it.
- For type or test failures, diagnose and fix the root cause, then re-run from step 1.
- Respect repo conventions when fixing: line-length 100, Python 3.9 syntax only
  (no `X | Y` unions / `match`), pydantic models in `models/schema.py`.
- This mirrors `.pre-commit-config.yaml` and `.github/workflows/test.yml`, so a green
  `/check` means CI should pass. End with a one-line PASS/FAIL summary per step.
