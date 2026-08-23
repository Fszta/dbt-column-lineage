"""Tests for policy config loading/parsing.

A policy is pure consumer config: it must parse the documented shape, coerce a single action
to a list, reject an unknown major version and a malformed predicate loudly (never silently
allow), and resolve to ``None`` when no policy is configured (legacy-gate fallback).
"""

import os

import pytest

from parrant.lineage.policy import PolicyConfigError, load_policy, parse_policy
from parrant.models.schema import (
    ActionKind,
    GateDecision,
    MissingMetaPolicy,
    Operator,
)

_POLICY_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "resources", "policies")


def _fixture(name):
    return os.path.join(_POLICY_DIR, name)


def test_load_pii_allowlist_fixture_parses_as_config():
    policy = load_policy(_fixture("pii_allowlist.yml"))
    assert policy is not None
    assert policy.version == 1
    (rule,) = policy.rules
    assert rule.id == "pii-outside-allowlist"
    # all[0] = subject meta pii is_true; all[1] = reach model where readable_by not_subset_of.
    assert rule.predicate.all_[0].meta.key == "pii"
    reach = rule.predicate.all_[1].reach
    assert reach.kind.value == "model"
    assert reach.where.meta.op is Operator.NOT_SUBSET_OF
    assert [a.type for a in rule.action] == [ActionKind.BLOCK, ActionKind.NOTIFY]


def test_load_breaking_reaches_critical_fixture_parses():
    policy = load_policy(_fixture("breaking_reaches_critical.yml"))
    (rule,) = policy.rules
    assert rule.predicate.all_[0].change.field == "breaking"
    build = rule.action[1]
    assert build.type is ActionKind.ADD_TO_BUILD_SET
    assert build.include == "reached"
    assert [m.value for m in build.mechanism] == ["derived_recompute", "rowset_filter"]


def test_single_action_mapping_is_coerced_to_list():
    policy = parse_policy(
        {
            "version": 1,
            "rules": [
                {
                    "id": "r",
                    "predicate": {"structural": {"fact": "reaches_anything"}},
                    "action": {"type": "warn"},
                }
            ],
        }
    )
    assert policy.rules[0].action[0].type is ActionKind.WARN


def test_defaults_default_to_fail_closed():
    policy = parse_policy({"version": 1, "rules": []})
    assert policy.defaults.on_missing_meta is MissingMetaPolicy.FAIL_CLOSED
    assert policy.defaults.on_error is MissingMetaPolicy.FAIL_CLOSED
    # on_error is an INDEPENDENT knob: it can be set separately from
    # on_missing_meta. (That the engine HONORS the split is proven behaviourally by
    # test_on_error_honored_independently_of_on_missing_meta in test_policy_engine.py.)
    mixed = parse_policy(
        {
            "version": 1,
            "defaults": {"on_missing_meta": "fail_open", "on_error": "fail_closed"},
            "rules": [],
        }
    )
    assert mixed.defaults.on_missing_meta is MissingMetaPolicy.FAIL_OPEN
    assert mixed.defaults.on_error is MissingMetaPolicy.FAIL_CLOSED


def test_rule_on_error_override_parses():
    policy = parse_policy(
        {
            "version": 1,
            "rules": [
                {
                    "id": "r",
                    "predicate": {"structural": {"fact": "reaches_anything"}},
                    "action": [{"type": "block"}],
                    "on_error": "fail_open",
                }
            ],
        }
    )
    assert policy.rules[0].on_error is MissingMetaPolicy.FAIL_OPEN
    assert policy.rules[0].on_missing_meta is None  # independent, unset -> falls back to default


def test_reach_min_count_zero_is_rejected():
    """min_count 0 is a vacuously-true reach; ge=1 must reject it loudly at load."""
    with pytest.raises(PolicyConfigError):
        parse_policy(
            {
                "version": 1,
                "rules": [
                    {
                        "id": "r",
                        "predicate": {
                            "reach": {
                                "kind": "model",
                                "min_count": 0,
                                "where": {"meta": {"key": "critical", "op": "is_true"}},
                            }
                        },
                        "action": [{"type": "block"}],
                    }
                ],
            }
        )


def test_unknown_version_is_rejected_loudly():
    with pytest.raises(PolicyConfigError):
        parse_policy({"version": 2, "rules": []})


def test_missing_version_is_rejected():
    with pytest.raises(PolicyConfigError):
        parse_policy({"rules": []})


def test_malformed_predicate_two_axes_is_rejected():
    with pytest.raises(PolicyConfigError):
        parse_policy(
            {
                "version": 1,
                "rules": [
                    {
                        "id": "bad",
                        "predicate": {
                            "meta": {"key": "a", "op": "eq", "value": 1},
                            "change": {"field": "kind", "op": "eq", "value": "added"},
                        },
                        "action": [{"type": "block"}],
                    }
                ],
            }
        )


def test_empty_document_is_rejected():
    with pytest.raises(PolicyConfigError):
        parse_policy(None)


def test_no_policy_configured_returns_none(tmp_path, monkeypatch):
    # No explicit path and no repo-default file in cwd -> None (legacy fallback).
    monkeypatch.chdir(tmp_path)
    assert load_policy(None) is None


def test_explicit_missing_path_raises():
    with pytest.raises(PolicyConfigError):
        load_policy("/no/such/policy.yml")


def test_repo_default_file_is_discovered(tmp_path, monkeypatch):
    (tmp_path / "dbt-col-lineage.policy.yml").write_text(
        "version: 1\nrules:\n  - id: r\n    predicate: {structural: {fact: reaches_anything}}\n"
        "    action: [{type: warn}]\n"
    )
    monkeypatch.chdir(tmp_path)
    policy = load_policy(None)
    assert policy is not None and policy.rules[0].id == "r"


def test_semantic_severity_knobs_parse():
    policy = load_policy(_fixture("semantic_severity_defaults.yml"))
    assert policy is not None
    assert policy.defaults.on_meaning_changed is GateDecision.BLOCK
    assert policy.defaults.on_indeterminate is GateDecision.WARN


def test_semantic_severity_knobs_default_to_none():
    policy = parse_policy({"version": 1, "rules": []})
    assert policy.defaults.on_meaning_changed is None
    assert policy.defaults.on_indeterminate is None


def test_invalid_semantic_severity_value_is_rejected():
    with pytest.raises(PolicyConfigError):
        parse_policy({"version": 1, "defaults": {"on_meaning_changed": "review"}, "rules": []})
