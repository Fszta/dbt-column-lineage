"""Opt-in, live-Metabase end-to-end tests.

This tier is intentionally NOT one of the directory-based tiers driven by
``scripts/run_tests.py`` (unit / integration / e2e), so it never runs in the per-push
gate (``.github/workflows/test.yml``). It boots a real Metabase, seeds content via the
REST API, runs ``parrant metabase-extract`` against it, and asserts a non-zero-coverage
extract — guarding against Metabase API-serialization drift (e.g. the pMBQL / MBQL-5
change in v0.57+). See ``tests/live/README.md``.
"""
