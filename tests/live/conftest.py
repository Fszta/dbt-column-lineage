"""Fixtures for the opt-in live-Metabase tier.

The container lifecycle is owned by CI / a local caller, NOT by pytest — we never
``docker run`` from here. A test runs only when ``METABASE_URL`` points at a reachable
instance; otherwise every fixture skips. See ``tests/live/README.md``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.live import seed

# Local-dev defaults for the admin created by first-boot ``/api/setup`` (used only when no
# API key is provided). CI creating a fresh container relies on these to bootstrap the admin.
DEFAULT_USERNAME = "admin@example.com"
DEFAULT_PASSWORD = "Parrant_Live_123!"

# The bundled dbt manifest supplies the SQL dialect (duckdb) for the native resolver.
MANIFEST_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "resources"
    / "dbt_test_project"
    / "target"
    / "manifest.json"
)


@pytest.fixture(scope="session")
def metabase_url() -> str:
    """The base URL of a running Metabase, or SKIP if ``METABASE_URL`` is unset."""
    url = os.environ.get("METABASE_URL")
    if not url:
        pytest.skip("METABASE_URL not set — provide a running Metabase to run the live tier.")
    return url.rstrip("/")


@pytest.fixture(scope="session")
def metabase_auth(metabase_url: str) -> seed.Auth:
    """Authenticate against the running instance (API key if provided, else session/setup)."""
    seed.wait_for_health(metabase_url)
    return seed.authenticate(
        metabase_url,
        api_key=os.environ.get("METABASE_API_KEY"),
        username=os.environ.get("METABASE_USERNAME", DEFAULT_USERNAME),
        password=os.environ.get("METABASE_PASSWORD", DEFAULT_PASSWORD),
    )


@pytest.fixture(scope="session")
def seeded(metabase_auth: seed.Auth) -> seed.SeededContent:
    """Seed the representative corpus once for the session and return the created ids."""
    return seed.seed_all(metabase_auth)


@pytest.fixture(scope="session")
def manifest_path() -> str:
    if not MANIFEST_PATH.exists():
        pytest.skip(f"dbt manifest not found at {MANIFEST_PATH}")
    return str(MANIFEST_PATH)
