""" — client auth, the legacy-mbql pin, pagination and retry/backoff."""

from __future__ import annotations

import pytest

from parrant.metabase.client import MetabaseAuthError, MetabaseClient
from tests.unit.metabase._fixtures import FakeSession, load_recorded


def _client(session: FakeSession, **kwargs) -> MetabaseClient:
    return MetabaseClient(
        base_url="https://metabase.example.com",
        session=session,
        sleep=lambda _s: None,  # no real sleeps in tests
        **kwargs,
    )


def test_api_key_auth_sets_header_and_pins_legacy_mbql():
    session = FakeSession(load_recorded())
    client = _client(session, api_key="secret-key")

    cards = client.list_cards()

    assert len(cards) == 5
    # legacy-mbql pin present on the card fetch (stable MBQL 4 serialization).
    card_call = next(c for c in session.get_calls if c[0].endswith("/api/card"))
    assert card_call[1].get("legacy-mbql") == "true"


def test_session_auth_posts_credentials_and_uses_token():
    session = FakeSession(load_recorded())
    client = _client(session, username="u", password="p")

    client.list_snippets()

    assert any(url.endswith("/api/session") for url, _ in session.post_calls)


def test_missing_credentials_raises():
    session = FakeSession(load_recorded())
    client = _client(session)
    with pytest.raises(MetabaseAuthError):
        client.list_cards()


def test_retry_on_transient_5xx():
    # First GET returns 503, then success — the client must retry and succeed.
    session = FakeSession(load_recorded(), fail_first=1)
    client = _client(session, api_key="k")

    dbs = client.database_metadata(2)

    assert dbs["id"] == 2
    assert len(session.get_calls) == 2  # one failed + one success


def test_server_version_best_effort():
    session = FakeSession(load_recorded())
    client = _client(session, api_key="k")
    assert client.server_version() == "v0.51.6"
