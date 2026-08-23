"""Tests for the ``by_change`` reach-name reshape (Policy engine P1b).

``get_changeset_impact``'s per-change ``by_change`` entries historically carried only a
``summary`` of COUNTS. A later reach predicate needs the reached NAMES (+ the mechanism the
change propagates by) so it can attribute *which* change tripped a rule. These tests pin
that the resolved entries now carry ``reached_models`` / ``reached_exposures`` /
``reached_columns`` with names and mechanism labels — a pure re-shape of what
``get_column_impact`` already computes (no new traversal).
"""

import json

from parrant.lineage.changeset import ChangesetBuilder
from parrant.lineage.service import LineageService


def _catalog_node(name, columns):
    return {
        "unique_id": f"model.p.{name}",
        "metadata": {"name": name, "schema": "s", "database": "d", "type": "BASE TABLE"},
        "columns": {c: {"name": c, "type": "NUMBER"} for c in columns},
    }


def _manifest_node(name, compiled, depends_on=None):
    return {
        "name": name,
        "unique_id": f"model.p.{name}",
        "resource_type": "model",
        "language": "sql",
        "schema": "s",
        "database": "d",
        "compiled_code": compiled,
        "depends_on": {"nodes": [f"model.p.{d}" for d in (depends_on or [])]},
    }


# orders.amount changes its logic between base and head (a pure logic edit).
_ORDERS_BASE = "select dim.id as id, dim.amount as amount from d.s.dim as dim"
_ORDERS_HEAD = "select dim.id as id, dim.amount * 2 as amount from d.s.dim as dim"

# derived_model recomputes over orders.amount -> derived_recompute reach.
_DERIVED = "select o.id as id, sum(o.amount) as total from d.s.orders as o group by 1"
# passthrough_model just carries orders.amount forward -> passthrough reach.
_PASSTHROUGH = "select o.id as id, o.amount as amount from d.s.orders as o"


def _nodes(orders_sql):
    return {
        "model.p.dim": _manifest_node("dim", "select 1 as id, 2 as amount"),
        "model.p.orders": _manifest_node("orders", orders_sql, depends_on=["dim"]),
        "model.p.derived_model": _manifest_node("derived_model", _DERIVED, depends_on=["orders"]),
        "model.p.passthrough_model": _manifest_node(
            "passthrough_model", _PASSTHROUGH, depends_on=["orders"]
        ),
    }


def _catalog():
    return {
        "model.p.dim": _catalog_node("dim", ["id", "amount"]),
        "model.p.orders": _catalog_node("orders", ["id", "amount"]),
        "model.p.derived_model": _catalog_node("derived_model", ["id", "total"]),
        "model.p.passthrough_model": _catalog_node("passthrough_model", ["id", "amount"]),
    }


def _write(tmp_path, tag, orders_sql, with_exposure=False):
    catalog_path = tmp_path / f"{tag}_catalog.json"
    manifest_path = tmp_path / f"{tag}_manifest.json"
    manifest = {"metadata": {"adapter_type": "snowflake"}, "nodes": _nodes(orders_sql)}
    if with_exposure:
        manifest["exposures"] = {
            "exposure.p.revenue_dashboard": {
                "name": "revenue_dashboard",
                "type": "dashboard",
                "depends_on": {"nodes": ["model.p.derived_model"]},
                "meta": {"audience": "public"},
            }
        }
    catalog_path.write_text(
        json.dumps({"metadata": {"adapter_type": "snowflake"}, "nodes": _catalog()})
    )
    manifest_path.write_text(json.dumps(manifest))
    return str(catalog_path), str(manifest_path)


def _aggregate(tmp_path, with_exposure=False):
    bcat, bman = _write(tmp_path, "base", _ORDERS_BASE, with_exposure=with_exposure)
    hcat, hman = _write(tmp_path, "head", _ORDERS_HEAD, with_exposure=with_exposure)
    base = LineageService(bcat, bman, adapter="snowflake")
    head = LineageService(hcat, hman, adapter="snowflake")
    changes = ChangesetBuilder(base.registry, head.registry).build()
    return head.get_changeset_impact(changes, base_service=base), changes


def _amount_entry(agg):
    entries = [
        e
        for e in agg["by_change"]
        if e.get("model") == "orders" and e.get("column") == "amount" and e.get("resolved")
    ]
    assert entries, agg["by_change"]
    return entries[0]


def test_by_change_carries_reached_model_names_with_mechanism(tmp_path):
    agg, _changes = _aggregate(tmp_path)
    entry = _amount_entry(agg)

    assert "reached_models" in entry
    reached = {m["name"] for m in entry["reached_models"]}
    assert {"derived_model", "passthrough_model"} <= reached

    mech_by_model = {m["name"]: m["mechanism"] for m in entry["reached_models"]}
    # derived_model recomputes -> the derived mechanism label.
    assert mech_by_model["derived_model"] == "derived_recompute"
    # passthrough_model carries the value forward -> a pass-through mechanism.
    assert mech_by_model["passthrough_model"] in {"direct_passthrough", "renamed_passthrough"}


def test_by_change_carries_reached_column_names_with_mechanism(tmp_path):
    agg, _changes = _aggregate(tmp_path)
    entry = _amount_entry(agg)

    reached_cols = {(c["model"], c["column"]): c["mechanism"] for c in entry["reached_columns"]}
    assert reached_cols[("derived_model", "total")] == "derived_recompute"
    assert reached_cols[("passthrough_model", "amount")] in {
        "direct_passthrough",
        "renamed_passthrough",
    }


def test_by_change_carries_reached_exposure_names(tmp_path):
    agg, _changes = _aggregate(tmp_path, with_exposure=True)
    entry = _amount_entry(agg)

    names = {e["name"] for e in entry["reached_exposures"]}
    assert "revenue_dashboard" in names


def _standalone(tmp_path, tag, catalog_cols, orders_sql):
    """A tiny standalone project (orders only) for the removed-column case."""
    catalog_path = tmp_path / f"{tag}_catalog.json"
    manifest_path = tmp_path / f"{tag}_manifest.json"
    catalog_path.write_text(
        json.dumps(
            {
                "metadata": {"adapter_type": "snowflake"},
                "nodes": {"model.p.orders": _catalog_node("orders", catalog_cols)},
            }
        )
    )
    manifest_path.write_text(
        json.dumps(
            {
                "metadata": {"adapter_type": "snowflake"},
                "nodes": {"model.p.orders": _manifest_node("orders", orders_sql)},
            }
        )
    )
    return str(catalog_path), str(manifest_path)


def test_unresolved_change_has_no_reach_keys(tmp_path):
    """An unresolved change (no reach could be computed) is not given reach keys — the
    reshape only enriches resolved entries, staying honest about what couldn't be walked."""
    # base has orders.amount; head has it removed from BOTH catalog and SQL.
    bcat, bman = _standalone(tmp_path, "base2", ["id", "amount"], "select 1 as id, 2 as amount")
    hcat, hman = _standalone(tmp_path, "head2", ["id"], "select 1 as id")
    base = LineageService(bcat, bman, adapter="snowflake")
    head = LineageService(hcat, hman, adapter="snowflake")
    changes = ChangesetBuilder(base.registry, head.registry).build()

    # No base_service passed -> a removed column cannot be resolved against head.
    agg = head.get_changeset_impact(changes)
    unresolved = [e for e in agg["by_change"] if not e.get("resolved")]
    assert unresolved, agg["by_change"]
    for entry in unresolved:
        assert "reached_models" not in entry
        assert "reached_columns" not in entry
