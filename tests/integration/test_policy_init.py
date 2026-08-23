"""Integration / anti-footgun regression for ``policy init``.

Generates the scaffold policy for the bundled dbt project, parses it through the REAL policy
loader, and evaluates it over a no-op changeset — locking in the property the whole feature
exists to guarantee: the generated policy does NOT rage-block on day one (it yields ALLOW when
nothing changed). Uses the ``dbt_artifacts`` fixture + ``LineageService`` like
``test_backtest_policy.py``.
"""

from pathlib import Path

import pytest
import yaml

from parrant.lineage.policy import evaluate_policy, parse_policy
from parrant.lineage.policy_init import emit_policy_yaml, run_policy_init, scan_project
from parrant.lineage.service import LineageService
from parrant.models.schema import GateDecision


@pytest.fixture
def head_service(dbt_artifacts):
    return LineageService(
        Path(dbt_artifacts["catalog_path"]),
        Path(dbt_artifacts["manifest_path"]),
    )


@pytest.fixture
def bundled_scan(head_service):
    return scan_project(head_service.registry)


def test_generated_policy_parses_and_allows_noop(head_service):
    scan = scan_project(head_service.registry)
    text = emit_policy_yaml(scan)

    # 1. The generated YAML must round-trip through the real parser without raising.
    policy = parse_policy(yaml.safe_load(text))
    assert [r.id for r in policy.rules] == ["provable-break-block", "exposure-guard"]

    # 2. On a no-op changeset (nothing changed, no impact, no provable breaks) the gate ALLOWs.
    #    This is the anti-footgun regression: the scaffold never blocks with an empty changeset.
    verdict = evaluate_policy(
        changes=[],
        changeset_impact={},
        registry=head_service.registry,
        policy=policy,
        breaks=[],
    )
    assert verdict.decision is GateDecision.ALLOW
    assert verdict.blocks() is False


def test_generated_policy_defaults_are_fail_closed_never_open(head_service):
    scan = scan_project(head_service.registry)
    policy = parse_policy(yaml.safe_load(emit_policy_yaml(scan)))
    # The scaffold ships the closed posture; it must never author an open-when-unsure default.
    assert policy.defaults.on_missing_meta.value == "fail_closed"
    assert policy.defaults.on_error.value == "fail_closed"


# --- scan over the real bundled project (needs built artifacts) --------------


def test_scan_histograms_on_bundled_project(bundled_scan):
    assert bundled_scan.total_models == 18
    assert bundled_scan.exposure_count == 3
    assert bundled_scan.column_test_count > 0  # 20 at time of writing
    assert bundled_scan.tests_present is True
    assert bundled_scan.exposures_present is True
    # The bundled project declares no dbt meta at all.
    assert bundled_scan.model_meta_keys == []
    assert bundled_scan.column_meta_keys == []


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


# --- run_policy_init orchestration: write / --force / --stdout ---------------


def test_run_policy_init_writes_file(dbt_artifacts, tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    text = run_policy_init(
        manifest=str(dbt_artifacts["manifest_path"]),
        catalog=str(dbt_artifacts["catalog_path"]),
        adapter=None,
        output=str(out),
        force=False,
        stdout=False,
    )
    assert out.exists()
    assert out.read_text() == text
    assert "version: 1" in text


def test_run_policy_init_refuses_overwrite_without_force(dbt_artifacts, tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    out.write_text("version: 1\nrules: []\n")
    with pytest.raises(FileExistsError):
        run_policy_init(
            manifest=str(dbt_artifacts["manifest_path"]),
            catalog=str(dbt_artifacts["catalog_path"]),
            adapter=None,
            output=str(out),
            force=False,
            stdout=False,
        )
    # The pre-existing file is untouched.
    assert out.read_text() == "version: 1\nrules: []\n"


def test_run_policy_init_force_overwrites(dbt_artifacts, tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    out.write_text("stale\n")
    text = run_policy_init(
        manifest=str(dbt_artifacts["manifest_path"]),
        catalog=str(dbt_artifacts["catalog_path"]),
        adapter=None,
        output=str(out),
        force=True,
        stdout=False,
    )
    assert out.read_text() == text
    assert "provable-break-block" in out.read_text()


def test_run_policy_init_stdout_never_writes(dbt_artifacts, tmp_path):
    out = tmp_path / "dbt-col-lineage.policy.yml"
    text = run_policy_init(
        manifest=str(dbt_artifacts["manifest_path"]),
        catalog=str(dbt_artifacts["catalog_path"]),
        adapter=None,
        output=str(out),
        force=False,
        stdout=True,
    )
    assert not out.exists()  # --stdout never touches disk
    assert "version: 1" in text
