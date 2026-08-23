"""Unit tests for the policy-engine CI wiring.

Covers the additive ``FailOn.POLICY`` gate, its ``gate_exit_code`` semantics, the additive
GitHub outputs, and (critically) that adding the policy gate leaves every pre-existing
``--fail-on`` value byte-for-byte unchanged (backward compatible).
"""

import pytest

from dbt_column_lineage.lineage.ci import FailOn, gate_exit_code, write_github_outputs
from dbt_column_lineage.models.schema import GateDecision, PolicyVerdict


def _verdict(decision: GateDecision, **kw) -> PolicyVerdict:
    return PolicyVerdict(decision=decision, **kw)


class TestPolicyGate:
    def test_block_verdict_fails(self):
        v = _verdict(GateDecision.BLOCK)
        assert gate_exit_code({}, FailOn.POLICY, v) == 1

    def test_warn_verdict_passes(self):
        v = _verdict(GateDecision.WARN)
        assert gate_exit_code({}, FailOn.POLICY, v) == 0

    def test_allow_verdict_passes(self):
        v = _verdict(GateDecision.ALLOW)
        assert gate_exit_code({}, FailOn.POLICY, v) == 0

    def test_no_verdict_passes(self):
        # --fail-on policy with no resolvable policy -> no verdict -> never fires.
        assert gate_exit_code({"critical_count": 9}, FailOn.POLICY, None) == 0

    def test_policy_is_a_member_and_blocks_flag(self):
        assert FailOn("policy") is FailOn.POLICY
        assert FailOn.POLICY.blocks is True


class TestBackwardCompatibility:
    """A block verdict passed alongside a legacy gate must NOT change that gate's outcome."""

    @pytest.mark.parametrize(
        "summary,fail_on,expected",
        [
            ({"critical_count": 5, "affected_exposures": 3}, FailOn.NONE, 0),
            ({"affected_exposures": 2}, FailOn.EXPOSURES, 1),
            ({"affected_exposures": 0}, FailOn.EXPOSURES, 0),
            ({"critical_count": 1}, FailOn.CRITICAL, 1),
            ({"provable_break_count": 2}, FailOn.TESTS, 1),
            ({"affected_columns": 4}, FailOn.ANY, 1),
        ],
    )
    def test_legacy_gate_ignores_policy_verdict(self, summary, fail_on, expected):
        blocking = _verdict(GateDecision.BLOCK)
        # Same result whether or not a (blocking) policy verdict is present.
        assert gate_exit_code(summary, fail_on) == expected
        assert gate_exit_code(summary, fail_on, blocking) == expected

    def test_empty_summary_never_blocks_across_all_gates(self):
        for policy in FailOn:
            assert gate_exit_code({}, policy) == 0


class TestGithubOutputs:
    def test_policy_outputs_written_when_present(self, tmp_path, monkeypatch):
        out_file = tmp_path / "gh_output"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
        report = {
            "summary": {"affected_models": 1},
            "verdict": "safe",
            "policy_verdict": {
                "decision": "block",
                "build_set": ["a", "b"],
                "test_set": ["c"],
            },
        }
        assert write_github_outputs(report) is True
        text = out_file.read_text()
        assert "policy_decision=block" in text
        assert "build_set_size=2" in text
        assert "test_set_size=1" in text
        # Legacy keys still present and unchanged.
        assert "affected_models=1" in text
        assert "verdict=safe" in text

    def test_no_policy_key_leaves_legacy_outputs_intact(self, tmp_path, monkeypatch):
        out_file = tmp_path / "gh_output"
        monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))
        report = {"summary": {"affected_models": 2}, "verdict": "review"}
        assert write_github_outputs(report) is True
        text = out_file.read_text()
        assert "policy_decision" not in text
        assert "build_set_size" not in text
        assert "affected_models=2" in text
