"""Unit tests for the ``policy init`` scaffold): the read-only scan, the safe-by-construction
YAML emitter, and the overwrite/--force/--stdout orchestration.

The bundled test project has been introspected (VERIFIED): 18 models, 3 exposures, 20
column-targeted generic tests, and ZERO dbt meta (neither model- nor column-level), so the
golden output enables both structural rules and emits NO meta templates. The meta-template code
path is exercised separately with a synthetic scan.
"""

from pathlib import Path

import pytest
import yaml

from dbt_column_lineage.lineage.policy import parse_policy
from dbt_column_lineage.lineage.policy_init import (
    _flatten_meta_keys,
    emit_policy_yaml,
    run_policy_init,
    scan_project,
)
from dbt_column_lineage.lineage.service import LineageService
from dbt_column_lineage.models.schema import MetaKeyCoverage, PolicyInitScan

_TARGET = Path(__file__).parent.parent.parent / "resources" / "dbt_test_project" / "target"


@pytest.fixture(scope="module")
def bundled_scan():
    service = LineageService(_TARGET / "catalog.json", _TARGET / "manifest.json")
    return scan_project(service.registry)


# --- scan --------------------------------------------------------------------


def test_scan_histograms_on_bundled_project(bundled_scan):
    assert bundled_scan.total_models == 18
    assert bundled_scan.exposure_count == 3
    assert bundled_scan.column_test_count > 0  # 20 at time of writing
    assert bundled_scan.tests_present is True
    assert bundled_scan.exposures_present is True
    # The bundled project declares no dbt meta at all.
    assert bundled_scan.model_meta_keys == []
    assert bundled_scan.column_meta_keys == []


def test_flatten_meta_keys_dotted_nesting():
    keys = _flatten_meta_keys({"critical": True, "governance": {"tier": "gold", "owner": "x"}})
    assert set(keys) == {"critical", "governance.tier", "governance.owner"}


def test_flatten_meta_keys_empty():
    assert _flatten_meta_keys({}) == []


# --- MetaKeyCoverage.pct -----------------------------------------------------


def test_pct_guards_zero_total():
    assert MetaKeyCoverage(key="k", n_present=0, total=0).pct == 0


def test_pct_rounds():
    assert MetaKeyCoverage(key="critical", n_present=95, total=1163).pct == 8


# --- emitter: bundled (both structural rules enabled) ------------------------


def test_emit_enables_both_structural_rules_for_bundled(bundled_scan):
    text = emit_policy_yaml(bundled_scan)
    # provable-break-block ENABLED (uncommented list item), not commented.
    assert "\n  - id: provable-break-block" in text
    assert "\n  - id: exposure-guard" in text
    assert "NOT emitted" not in text  # neither structural rule was withheld
    # Round-trips through the real parser into exactly the two enabled rules.
    policy = parse_policy(yaml.safe_load(text))
    assert [r.id for r in policy.rules] == ["provable-break-block", "exposure-guard"]


def test_emit_never_contains_fail_open(bundled_scan):
    assert "fail_open" not in emit_policy_yaml(bundled_scan)
    assert "on_missing_meta: fail_closed" in emit_policy_yaml(bundled_scan)


def test_emit_points_at_policy_test(bundled_scan):
    assert "policy test --last 20" in emit_policy_yaml(bundled_scan)


# --- emitter: provable-break-block commented when no tests -------------------


def test_provable_break_block_commented_when_no_tests():
    scan = PolicyInitScan(total_models=5, column_test_count=0, exposure_count=1)
    text = emit_policy_yaml(scan)
    # The block rule must NOT be an armed list item...
    assert "\n  - id: provable-break-block" not in text
    # ...it is present only as a commented, withheld template with the honest reason.
    assert "provable-break-block — NOT emitted" in text
    assert "no column-targeted generic tests" in text
    # exposure-guard is still armed (exposures present).
    assert "\n  - id: exposure-guard" in text
    parse_policy(yaml.safe_load(text))  # must still be valid


# --- emitter: meta templates (synthetic scan) --------------------------------


def test_meta_template_commented_with_coverage_and_presence_op():
    scan = PolicyInitScan(
        total_models=1163,
        total_columns=1,
        column_test_count=10,
        exposure_count=0,
        model_meta_keys=[MetaKeyCoverage(key="critical", n_present=95, total=1163)],
    )
    text = emit_policy_yaml(scan)
    # Exact coverage string from the scan (not a placeholder).
    assert "meta.critical present on 95/1163 models (8%)" in text
    # A presence operator (is_true), never a value comparison, and always commented (never armed).
    assert "op: is_true" in text
    assert "\n  - id: reach-critical-model" not in text  # not armed
    assert "  # - id: reach-critical-model" in text  # commented template
    # Still valid + never fail_open.
    assert "fail_open" not in text
    parse_policy(yaml.safe_load(text))


def test_meta_template_dotted_key_slug():
    scan = PolicyInitScan(
        total_models=100,
        column_test_count=1,
        exposure_count=1,
        model_meta_keys=[MetaKeyCoverage(key="governance.tier", n_present=17, total=100)],
    )
    text = emit_policy_yaml(scan)
    assert "meta.governance.tier present on 17/100 models (17%)" in text
    # The dotted key survives verbatim in the where.meta.key (matches the engine's dotted lookup).
    assert "key: governance.tier, op: is_true" in text
    # The rule id is a sanitized slug (dots -> dashes).
    assert "reach-governance-tier-model" in text


def test_meta_template_cap_notes_omitted_keys():
    rows = [MetaKeyCoverage(key=f"k{i:02d}", n_present=20 - i, total=100) for i in range(20)]
    scan = PolicyInitScan(
        total_models=100, column_test_count=1, exposure_count=1, model_meta_keys=rows
    )
    text = emit_policy_yaml(scan)
    assert "more model-meta key(s) not shown" in text


# --- emitter: honest empty (no tests, no exposures, no meta) -----------------


def test_emit_honest_empty_when_no_signal():
    scan = PolicyInitScan(total_models=3, total_columns=9)
    text = emit_policy_yaml(scan)
    assert "rules: []" in text
    assert "Nothing could be safely auto-enabled" in text
    assert "fail_open" not in text
    # An honest-empty policy must still parse and carry zero rules (does not rage-block).
    policy = parse_policy(yaml.safe_load(text))
    assert policy.rules == []


# --- orchestration: run_policy_init (write / --force / --stdout) -------------


def test_run_policy_init_writes_file(tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    text = run_policy_init(
        manifest=str(_TARGET / "manifest.json"),
        catalog=str(_TARGET / "catalog.json"),
        adapter=None,
        output=str(out),
        force=False,
        stdout=False,
    )
    assert out.exists()
    assert out.read_text() == text
    assert "version: 1" in text


def test_run_policy_init_refuses_overwrite_without_force(tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    out.write_text("version: 1\nrules: []\n")
    with pytest.raises(FileExistsError):
        run_policy_init(
            manifest=str(_TARGET / "manifest.json"),
            catalog=str(_TARGET / "catalog.json"),
            adapter=None,
            output=str(out),
            force=False,
            stdout=False,
        )
    # The pre-existing file is untouched.
    assert out.read_text() == "version: 1\nrules: []\n"


def test_run_policy_init_force_overwrites(tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    out.write_text("stale\n")
    text = run_policy_init(
        manifest=str(_TARGET / "manifest.json"),
        catalog=str(_TARGET / "catalog.json"),
        adapter=None,
        output=str(out),
        force=True,
        stdout=False,
    )
    assert out.read_text() == text
    assert "provable-break-block" in out.read_text()


def test_run_policy_init_stdout_never_writes(tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    text = run_policy_init(
        manifest=str(_TARGET / "manifest.json"),
        catalog=str(_TARGET / "catalog.json"),
        adapter=None,
        output=str(out),
        force=False,
        stdout=True,
    )
    assert not out.exists()  # --stdout never touches disk
    assert "version: 1" in text
