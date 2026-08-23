"""Integration tests for override pragmas driven through the ``impact`` CLI end-to-end.

Uses the real dbt test-project artifacts. For each scenario we synthesize a HEAD (or base)
that carries a ``-- lineage:allow-*`` pragma in a model's compiled SQL, then assert the
override moves the verdict / the CI gate / the report — and that ``--no-overrides`` reproduces
the raw gate.
"""

import copy
import json

import pytest
from click.testing import CliRunner

from dbt_column_lineage.cli.main import impact


def _load(path):
    with open(path) as f:
        return json.load(f)


def _model_uid(manifest, name):
    for uid, node in manifest["nodes"].items():
        if node.get("name") == name and node.get("resource_type") == "model":
            return uid
    raise AssertionError(f"{name} not in manifest")


def _catalog_uid(catalog, name):
    for uid in catalog["nodes"]:
        if uid.split(".")[-1] == name:
            return uid
    raise AssertionError(f"{name} not in catalog")


@pytest.fixture
def allow_break_head(dbt_artifacts, tmp_path):
    """HEAD that drops the tested ``stg_transactions.transaction_id`` column (orphaning its
    not_null/unique tests) and carries an ``allow-break`` pragma acknowledging it."""
    manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    catalog = copy.deepcopy(_load(dbt_artifacts["catalog_path"]))

    cat_uid = _catalog_uid(catalog, "stg_transactions")
    catalog["nodes"][cat_uid]["columns"].pop("transaction_id", None)

    man_uid = _model_uid(manifest, "stg_transactions")
    original = manifest["nodes"][man_uid]["compiled_code"]
    manifest["nodes"][man_uid]["compiled_code"] = (
        '-- lineage:allow-break column=transaction_id reason="test yml updated in follow-up PR"\n'
        + original
    )

    head_manifest = tmp_path / "manifest.json"
    head_catalog = tmp_path / "catalog.json"
    head_manifest.write_text(json.dumps(manifest))
    head_catalog.write_text(json.dumps(catalog))
    return {"manifest": str(head_manifest), "catalog": str(head_catalog)}


def _run(args):
    # mix_stderr=False so stderr (loud override warnings) doesn't corrupt the JSON on stdout.
    return CliRunner(mix_stderr=False).invoke(impact, args)


def test_allow_break_demotes_provable_break_and_clears_tests_gate(dbt_artifacts, allow_break_head):
    base_args = [
        "--manifest",
        allow_break_head["manifest"],
        "--catalog",
        allow_break_head["catalog"],
        "--base-manifest",
        str(dbt_artifacts["manifest_path"]),
        "--base-catalog",
        str(dbt_artifacts["catalog_path"]),
    ]

    # With the override honored: no BLOCKING break remains, verdict floors at review, and the
    # honored override is logged.
    result = _run([*base_args, "--format", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["verdict"] == "review"
    assert payload["provable_breaks"] == []
    assert payload["summary"]["provable_break_count"] == 0
    applied = payload["overrides"]
    assert any(
        o["verb"] == "allow-break"
        and o["column"] == "transaction_id"
        and o["downgraded_from"] == "block"
        and o["downgraded_to"] == "review"
        for o in applied
    ), applied

    # THE HEADLINE: the --fail-on tests gate must NOT fire once the break is acknowledged.
    # (--ci is required for the gate to apply an exit code; no PR context => comment skipped.)
    gated = _run([*base_args, "--ci", "--fail-on", "tests"])
    assert gated.exit_code == 0, gated.stderr

    # --no-overrides reproduces the RAW gate: the break is armed again and the gate fails.
    raw = _run([*base_args, "--format", "json", "--no-overrides"])
    raw_payload = json.loads(raw.output)
    assert raw_payload["verdict"] == "block"
    assert raw_payload["provable_breaks"]
    assert raw_payload["overrides"] == []
    raw_gated = _run([*base_args, "--ci", "--fail-on", "tests", "--no-overrides"])
    assert raw_gated.exit_code == 1


@pytest.fixture
def allow_change_head_and_base(dbt_artifacts, tmp_path):
    """A logic change on ``stg_accounts.account_id`` (base derives it with ``abs(...)``), where
    HEAD carries an ``allow-change`` pragma acknowledging the recompute."""
    base_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    head_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))

    b_uid = _model_uid(base_manifest, "stg_accounts")
    base_manifest["nodes"][b_uid]["compiled_code"] = base_manifest["nodes"][b_uid][
        "compiled_code"
    ].replace("cast(id as integer) as account_id", "abs(cast(id as integer)) as account_id")

    h_uid = _model_uid(head_manifest, "stg_accounts")
    head_manifest["nodes"][h_uid]["compiled_code"] = head_manifest["nodes"][h_uid][
        "compiled_code"
    ].replace(
        "cast(id as integer) as account_id",
        '-- lineage:allow-change column=account_id reason="intended recompute; downstream updated"\n'
        "  cast(id as integer) as account_id",
    )

    base_m = tmp_path / "base_manifest.json"
    head_m = tmp_path / "head_manifest.json"
    base_m.write_text(json.dumps(base_manifest))
    head_m.write_text(json.dumps(head_manifest))
    return {
        "base_manifest": str(base_m),
        "head_manifest": str(head_m),
        "catalog": str(dbt_artifacts["catalog_path"]),
    }


def test_allow_change_logs_override_and_no_overrides_restores_review(
    allow_change_head_and_base,
):
    cfg = allow_change_head_and_base
    base_args = [
        "--manifest",
        cfg["head_manifest"],
        "--catalog",
        cfg["catalog"],
        "--base-manifest",
        cfg["base_manifest"],
        "--base-catalog",
        cfg["catalog"],
    ]

    payload = json.loads(_run([*base_args, "--format", "json"]).output)
    # The account_id change carries the override in by_change, and it is logged as applied.
    account = [
        c
        for c in payload["by_change"]
        if c["model"] == "stg_accounts" and c["column"] == "account_id"
    ]
    assert account and account[0].get("override", {}).get("verb") == "allow-change"
    assert any(o["column"] == "account_id" for o in payload["overrides"])

    # --no-overrides: the pragma is not seen, so the recompute reads as REVIEW again and the
    # overrides block is empty (the raw-gate audit path).
    raw = json.loads(_run([*base_args, "--format", "json", "--no-overrides"]).output)
    raw_account = [
        c for c in raw["by_change"] if c["model"] == "stg_accounts" and c["column"] == "account_id"
    ]
    assert raw_account and "override" not in raw_account[0]
    assert raw["overrides"] == []
    assert raw["verdict"] == "review"


def test_malformed_pragma_is_warned_and_ruling_unchanged(dbt_artifacts, tmp_path):
    """A reasonless pragma must be DROPPED (a loud warning) and never alter the ruling."""
    head_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    base_manifest = copy.deepcopy(_load(dbt_artifacts["manifest_path"]))
    b_uid = _model_uid(base_manifest, "stg_accounts")
    base_manifest["nodes"][b_uid]["compiled_code"] = base_manifest["nodes"][b_uid][
        "compiled_code"
    ].replace("cast(id as integer) as account_id", "abs(cast(id as integer)) as account_id")
    h_uid = _model_uid(head_manifest, "stg_accounts")
    head_manifest["nodes"][h_uid]["compiled_code"] = head_manifest["nodes"][h_uid][
        "compiled_code"
    ].replace(
        "cast(id as integer) as account_id",
        "-- lineage:allow-change column=account_id\n  cast(id as integer) as account_id",
    )
    base_m = tmp_path / "b.json"
    head_m = tmp_path / "h.json"
    base_m.write_text(json.dumps(base_manifest))
    head_m.write_text(json.dumps(head_manifest))

    result = _run(
        [
            "--manifest",
            str(head_m),
            "--catalog",
            str(dbt_artifacts["catalog_path"]),
            "--base-manifest",
            str(base_m),
            "--base-catalog",
            str(dbt_artifacts["catalog_path"]),
            "--format",
            "json",
        ]
    )
    payload = json.loads(result.output)
    assert payload["overrides"] == []  # reasonless pragma honored nothing
    assert payload["override_warnings"], "a reasonless pragma must produce a loud warning"
    # Ruling unchanged from the raw recompute (still surfaced for review).
    assert payload["verdict"] == "review"
