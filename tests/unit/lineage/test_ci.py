"""Unit tests for the CI integration: severity gate, marker, PR-context
resolution, and the find-or-update sticky-comment flow (with a fake session)."""

import json

import pytest

from dbt_column_lineage.lineage.ci import (
    COMMENT_MARKER,
    FailOn,
    GitHubContext,
    gate_exit_code,
    highest_tripped_level,
    post_sticky_comment,
    resolve_context,
    resolve_pr_number,
    with_marker,
    write_github_outputs,
)


class TestGateExitCode:
    @pytest.mark.parametrize(
        "summary,fail_on,expected",
        [
            # 'none' never blocks, even with critical impact + exposures.
            ({"critical_count": 5, "affected_exposures": 3}, FailOn.NONE, 0),
            # exposures gate
            ({"affected_exposures": 0}, FailOn.EXPOSURES, 0),
            ({"affected_exposures": 2}, FailOn.EXPOSURES, 1),
            # critical gate keys off derived-logic count, not exposures
            ({"critical_count": 0, "affected_exposures": 9}, FailOn.CRITICAL, 0),
            ({"critical_count": 1}, FailOn.CRITICAL, 1),
            # any gate trips on any downstream touch
            ({"affected_models": 0, "affected_columns": 0, "affected_exposures": 0}, FailOn.ANY, 0),
            ({"affected_columns": 4}, FailOn.ANY, 1),
        ],
    )
    def test_gate(self, summary, fail_on, expected):
        assert gate_exit_code(summary, fail_on) == expected

    def test_empty_summary_never_blocks(self):
        for policy in FailOn:
            assert gate_exit_code({}, policy) == 0


class TestHighestTrippedLevel:
    @pytest.mark.parametrize(
        "summary,expected",
        [
            ({}, "none"),
            ({"affected_models": 0, "affected_columns": 0, "affected_exposures": 0}, "none"),
            # exposures is the most severe band and wins over critical/any
            ({"affected_exposures": 2, "critical_count": 5, "affected_columns": 9}, "exposures"),
            # no exposures, but derived logic recomputed downstream
            ({"critical_count": 3, "affected_columns": 7}, "critical"),
            # only pass-through references touched
            ({"affected_columns": 4}, "any"),
            ({"affected_models": 1}, "any"),
        ],
    )
    def test_level(self, summary, expected):
        assert highest_tripped_level(summary) == expected


class TestWriteGithubOutputs:
    def test_writes_expected_keys(self, tmp_path, monkeypatch):
        out_file = tmp_path / "gh_output"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
        report = {
            "summary": {
                "affected_models": 3,
                "affected_columns": 8,
                "affected_exposures": 2,
                "critical_count": 1,
            }
        }
        assert write_github_outputs(report) is True
        written = dict(
            line.split("=", 1) for line in out_file.read_text().splitlines() if "=" in line
        )
        assert written["affected_models"] == "3"
        assert written["affected_columns"] == "8"
        assert written["affected_exposures"] == "2"
        # exposures affected -> highest band
        assert written["tripped_level"] == "exposures"

    def test_appends_without_clobbering(self, tmp_path, monkeypatch):
        out_file = tmp_path / "gh_output"
        out_file.write_text("preexisting=1\n")
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
        write_github_outputs({"summary": {"affected_columns": 4}})
        content = out_file.read_text()
        assert "preexisting=1" in content
        assert "tripped_level=any" in content

    def test_noop_without_env(self, monkeypatch):
        monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
        assert write_github_outputs({"summary": {"affected_models": 5}}) is False


def test_fail_on_blocks_property():
    assert FailOn.NONE.blocks is False
    assert FailOn.EXPOSURES.blocks is True
    assert FailOn.CRITICAL.blocks is True
    assert FailOn.ANY.blocks is True


def test_with_marker_is_idempotent():
    body = "hello"
    once = with_marker(body)
    assert once.startswith(COMMENT_MARKER)
    assert with_marker(once) == once  # already marked -> unchanged


class TestResolvePrNumber:
    def test_explicit_wins(self):
        assert resolve_pr_number(42) == 42

    def test_from_event_pull_request(self, tmp_path, monkeypatch):
        event = tmp_path / "event.json"
        event.write_text(json.dumps({"pull_request": {"number": 7}}))
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
        assert resolve_pr_number() == 7

    def test_from_event_top_level_number(self, tmp_path, monkeypatch):
        event = tmp_path / "event.json"
        event.write_text(json.dumps({"number": 11}))
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
        assert resolve_pr_number() == 11

    def test_missing_event_returns_none(self, monkeypatch):
        monkeypatch.delenv("GITHUB_EVENT_PATH", raising=False)
        assert resolve_pr_number() is None


class TestResolveContext:
    def test_none_when_incomplete(self, monkeypatch):
        for var in ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
            monkeypatch.delenv(var, raising=False)
        assert resolve_context() is None

    def test_full_context_from_args(self, monkeypatch):
        monkeypatch.delenv("GITHUB_API_URL", raising=False)
        ctx = resolve_context(token="t", repo="o/r", pr_number=3)
        assert ctx == GitHubContext(repo="o/r", pr_number=3, token="t")

    def test_env_fallbacks(self, tmp_path, monkeypatch):
        event = tmp_path / "event.json"
        event.write_text(json.dumps({"pull_request": {"number": 5}}))
        monkeypatch.setenv("GITHUB_TOKEN", "envtok")
        monkeypatch.setenv("GITHUB_REPOSITORY", "acme/dbt")
        monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))
        monkeypatch.setenv("GITHUB_API_URL", "https://ghe.example.com/api/v3/")
        ctx = resolve_context()
        assert ctx is not None
        assert ctx.token == "envtok"
        assert ctx.repo == "acme/dbt"
        assert ctx.pr_number == 5
        # trailing slash trimmed
        assert ctx.api_url == "https://ghe.example.com/api/v3"


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeSession:
    """Records calls and returns queued GET pages; POST/PATCH are no-ops."""

    def __init__(self, get_pages):
        self._get_pages = list(get_pages)
        self.posted = []
        self.patched = []

    def get(self, url, headers=None, params=None, timeout=None):
        page = self._get_pages.pop(0) if self._get_pages else []
        return _FakeResponse(page)

    def post(self, url, headers=None, json=None, timeout=None):
        self.posted.append((url, json))
        return _FakeResponse({"id": 999})

    def patch(self, url, headers=None, json=None, timeout=None):
        self.patched.append((url, json))
        return _FakeResponse({"id": 999})


class TestPostStickyComment:
    ctx = GitHubContext(repo="o/r", pr_number=8, token="tok")

    def test_creates_when_no_existing_comment(self):
        session = _FakeSession(get_pages=[[]])
        outcome = post_sticky_comment(self.ctx, "body", session=session)
        assert outcome == "created"
        assert len(session.posted) == 1
        url, payload = session.posted[0]
        assert url.endswith("/repos/o/r/issues/8/comments")
        assert COMMENT_MARKER in payload["body"]
        assert not session.patched

    def test_updates_when_marker_found(self):
        existing = [{"id": 123, "body": f"{COMMENT_MARKER}\nold report"}]
        session = _FakeSession(get_pages=[existing])
        outcome = post_sticky_comment(self.ctx, "new body", session=session)
        assert outcome == "updated"
        assert len(session.patched) == 1
        url, payload = session.patched[0]
        assert url.endswith("/repos/o/r/issues/comments/123")
        assert "new body" in payload["body"]
        assert not session.posted

    def test_paginates_to_find_marker(self):
        first_page = [{"id": i, "body": "unrelated"} for i in range(100)]
        second_page = [{"id": 500, "body": COMMENT_MARKER}]
        session = _FakeSession(get_pages=[first_page, second_page])
        outcome = post_sticky_comment(self.ctx, "body", session=session)
        assert outcome == "updated"
        assert session.patched[0][0].endswith("/issues/comments/500")
