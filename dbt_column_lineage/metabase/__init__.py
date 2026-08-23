"""Metabase cross-boundary lineage.

Two halves, split so the offline gate never touches credentials:

* **extract** (this package, credentialed) — :mod:`client` talks to the Metabase API,
  :mod:`resolvers` turn cards into warehouse ``schema.table.column`` edges, and
  :mod:`extract` writes the ``metabase_lineage.json`` snapshot artifact.
* **consume** (offline, zero-credential) — :mod:`artifact` loads that snapshot, :mod:`join`
  maps its warehouse relations back to dbt models, and :mod:`reach` inverts it into the
  ``(model, column) -> card -> dashboard`` reach index. The gate / reach path imports ONLY
  these three (never :mod:`client`), so credentials are structurally confined to the extract
  step — enforced by ``tests/unit/metabase/test_offline_guardrail.py``.

Implemented here: (schema + IO), (client), (resolvers), (extract + CLI),
 (dbt relation join + reach index). The policy wiring lives in ``lineage/policy.py``
(MetaIndex dashboard-meta fallback) + ``lineage/service.py`` (reach append) + ``cli/main.py``.
"""
