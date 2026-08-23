""" explorer surfacing — change-context passthrough on the explorer API.

These exercise the additive wiring on ``LineageExplorer`` WITHOUT a live server or a
built dbt project: the enrichment is pure dict transformation over the stable data
contracts (``ColumnChange.semantic``, ``report["policy_verdict"]``, Metabase exposure
fields, ``Coverage`` + Metabase reach). Backward-compat is asserted explicitly: with no
change context every payload is returned untouched.
"""

from parrant.lineage.display.html.explore import LineageExplorer


def _report():
    """A changeset report shaped like the ``impact`` CLI assembles it."""
    return {
        "source": "two-manifest",
        "affected_exposures": [
            {
                "name": "metabase.dashboard.55",
                "type": "dashboard",
                "url": "https://mb/dashboard/55",
                "source": "metabase",
                "precision": "column",
                "via_cards": [128],
                "via_columns": [
                    {"model": "dim_accounts", "column": "balance", "card_id": 128, "role": "field"}
                ],
                "meta": {"tier": "executive"},
            },
            {"name": "exposure.dbt.finance", "type": "dashboard", "source": "dbt"},
        ],
        "by_change": [
            {
                "model": "dim_accounts",
                "column": "balance",
                "kind": "logic_changed",
                "semantic": "meaning_changed",
                "reached_exposures": [
                    {
                        "name": "metabase.dashboard.55",
                        "precision": "column",
                        "via_columns": [
                            {
                                "model": "dim_accounts",
                                "column": "balance",
                                "card_id": 128,
                                "role": "field",
                            }
                        ],
                    },
                    {"name": "exposure.dbt.finance"},
                ],
            },
            {
                "model": "dim_accounts",
                "column": "label",
                "kind": "logic_changed",
                "semantic": "equivalent",
                "reached_exposures": [],
            },
        ],
        "policy_verdict": {
            "decision": "block",
            "hits": [
                {
                    "rule_id": "pii-outside-allowlist",
                    "decision": "block",
                    "change_model": "dim_accounts",
                    "change_column": "balance",
                    "matched_reach": ["metabase.dashboard.55"],
                    "actions": ["block", "notify"],
                }
            ],
            "build_set": ["dim_accounts"],
            "test_set": ["fact_revenue"],
            "notifications": [],
            "evaluated_rules": 4,
            "fired_rules": 1,
        },
        "metabase": {
            "level": "partial",
            "stale": True,
            "dashboards_reached": 3,
            "cards_column_precise": 2,
            "cards_table_only": 1,
        },
    }


def test_is_breaking_fail_safe():
    e = LineageExplorer()
    # Additive column and proven-equivalent logic change are the only non-breaking cases.
    assert e._is_breaking("added", None) is False
    assert e._is_breaking("logic_changed", "equivalent") is False
    # Everything else — incl. unknown/indeterminate — is breaking (never render unknown as safe).
    assert e._is_breaking("logic_changed", "meaning_changed") is True
    assert e._is_breaking("logic_changed", "indeterminate") is True
    assert e._is_breaking("logic_changed", None) is True
    assert e._is_breaking("removed", None) is True
    assert e._is_breaking("type_changed", None) is True


def test_set_change_context_builds_indices():
    e = LineageExplorer()
    e.set_change_context(_report())

    assert e._policy_decision == "block"
    assert e._policy_verdict["decision"] == "block"
    assert e._metabase_coverage["level"] == "partial"

    assert e._change_by_column[("dim_accounts", "balance")] == {
        "semantic": "meaning_changed",
        "breaking": True,
    }
    assert e._change_by_column[("dim_accounts", "label")]["breaking"] is False

    # Only the metabase exposure is indexed for the cross-boundary merge.
    assert set(e._metabase_exposures) == {"metabase.dashboard.55"}
    # The breaking change reaches the dashboard (carrying the column chain, F4); the
    # equivalent one reaches nothing.
    assert e._metabase_reach_by_change[("dim_accounts", "balance")] == [
        {
            "name": "metabase.dashboard.55",
            "precision": "column",
            "via_columns": [
                {"model": "dim_accounts", "column": "balance", "card_id": 128, "role": "field"}
            ],
        }
    ]
    assert ("dim_accounts", "label") not in e._metabase_reach_by_change


def test_set_change_context_none_is_pure_explore():
    e = LineageExplorer()
    e.set_change_context(_report())
    e.set_change_context(None)  # a second call fully replaces / clears the prior context
    assert e._change_report is None
    assert e._policy_verdict is None
    assert e._policy_decision is None
    assert e._change_by_column == {}
    assert e._metabase_exposures == {}


def test_enrich_impact_no_context_is_passthrough():
    e = LineageExplorer()
    impact = {
        "affected_columns": [{"model": "m", "column": "c", "severity": "critical"}],
        "affected_exposures": [{"name": "x"}],
    }
    before = {
        "affected_columns": [dict(impact["affected_columns"][0])],
        "affected_exposures": [dict(impact["affected_exposures"][0])],
    }
    out = e._enrich_impact_with_change_context(impact, "m", "c")
    assert out["affected_columns"] == before["affected_columns"]
    assert out["affected_exposures"] == before["affected_exposures"]
    assert "policy_verdict" not in out
    assert "subject_semantic" not in out
    # Pure-explore mode: no changeset-membership flag at all (byte-for-byte today's payload).
    assert "subject_in_changeset" not in out


def test_enrich_impact_decorates_subject_and_columns():
    e = LineageExplorer()
    e.set_change_context(_report())
    impact = {
        "affected_columns": [
            {"model": "dim_accounts", "column": "label", "severity": "low_impact"},
            {"model": "fact_revenue", "column": "total", "severity": "critical"},
        ],
        "affected_exposures": [{"name": "exposure.dbt.finance", "type": "dashboard"}],
    }
    out = e._enrich_impact_with_change_context(impact, "dim_accounts", "balance")

    # Subject column (dim_accounts.balance) is a breaking logic change.
    assert out["subject_semantic"] == "meaning_changed"
    assert out["subject_breaking"] is True

    # An affected column that is itself a changed column gets its semantic mark.
    label_col = next(c for c in out["affected_columns"] if c["column"] == "label")
    assert label_col["semantic"] == "equivalent"
    assert label_col["breaking"] is False
    # A downstream column that is NOT in the changeset is left unmarked.
    total_col = next(c for c in out["affected_columns"] if c["column"] == "total")
    assert "semantic" not in total_col

    # The subject column IS part of the reviewed change, so the change-wide policy verdict
    # is attached and the column is flagged as in-changeset.
    assert out["subject_in_changeset"] is True
    assert out["policy_verdict"]["decision"] == "block"

    # The Metabase dashboard this change reaches is appended with its provenance fields.
    mb = next(x for x in out["affected_exposures"] if x.get("source") == "metabase")
    assert mb["name"] == "metabase.dashboard.55"
    assert mb["precision"] == "column"
    assert mb["via_cards"] == [128]
    assert mb["meta"]["tier"] == "executive"
    # F4: the per-change column chain (which field of the dashboard this change hits) rides
    # through onto the exposure entry, not just the dashboard name.
    assert mb["via_columns"] == [
        {"model": "dim_accounts", "column": "balance", "card_id": 128, "role": "field"}
    ]


def test_enrich_impact_equivalent_subject_reaches_no_metabase():
    e = LineageExplorer()
    e.set_change_context(_report())
    impact = {"affected_columns": [], "affected_exposures": []}
    out = e._enrich_impact_with_change_context(impact, "dim_accounts", "label")
    assert out["subject_breaking"] is False
    # The equivalent change reaches no dashboard → nothing appended.
    assert all(x.get("source") != "metabase" for x in out["affected_exposures"])


def test_enrich_impact_out_of_changeset_column_has_no_verdict():
    """Feedback F2: exploring a column that is NOT part of the reviewed change must NOT
    surface the whole-change policy verdict — picking a column is not making a change."""
    e = LineageExplorer()
    e.set_change_context(_report())
    impact = {"affected_columns": [], "affected_exposures": []}
    # stg_accounts.raw is not in the changeset (only dim_accounts.balance/label are).
    out = e._enrich_impact_with_change_context(impact, "stg_accounts", "raw")

    assert out["subject_in_changeset"] is False
    # The global block must not leak onto an arbitrarily-explored column.
    assert "policy_verdict" not in out
    # ...and it carries no subject semantic mark either (it is not a changed column).
    assert "subject_semantic" not in out


def test_impact_summary_policy_decision_is_change_scoped():
    """Feedback F2: the graph subject-ring feed (impact_summary.policy_decision) must only
    be threaded for a column that is part of the reviewed change, never for any explored one.

    Mirrors the guard in the ``/api/lineage`` endpoint: only set when the explored column is
    a changed column (``_change_mark`` truthy)."""
    e = LineageExplorer()
    e.set_change_context(_report())

    # In-changeset column → decision is threaded.
    assert e._policy_decision == "block"
    assert e._change_mark("dim_accounts", "balance") is not None
    # Out-of-changeset column → no mark, so the endpoint would not thread the decision.
    assert e._change_mark("stg_accounts", "raw") is None


def test_annotate_nodes_with_semantic_marks_nodes_and_blast_edge():
    e = LineageExplorer()
    e.set_change_context(_report())
    e.data.nodes = [
        {"id": "col_dim_accounts_balance", "type": "column", "model": "dim_accounts",
         "label": "balance"},
        {"id": "col_dim_accounts_label", "type": "column", "model": "dim_accounts",
         "label": "label"},
        {"id": "col_fact_revenue_total", "type": "column", "model": "fact_revenue",
         "label": "total"},
    ]
    e.data.edges = [
        {"source": "col_dim_accounts_balance", "target": "col_fact_revenue_total"},
        {"source": "col_dim_accounts_label", "target": "col_fact_revenue_total"},
    ]
    e._annotate_nodes_with_semantic()

    balance = next(n for n in e.data.nodes if n["id"] == "col_dim_accounts_balance")
    assert balance["semantic"] == "meaning_changed"
    assert balance["breaking"] is True
    label = next(n for n in e.data.nodes if n["id"] == "col_dim_accounts_label")
    assert label["breaking"] is False
    total = next(n for n in e.data.nodes if n["id"] == "col_fact_revenue_total")
    assert "breaking" not in total  # untouched — not a changed column

    # The one warm blast-path edge: leaves the breaking column only.
    breaking_edge = next(
        edge for edge in e.data.edges if edge["source"] == "col_dim_accounts_balance"
    )
    assert breaking_edge["breaking"] is True
    calm_edge = next(edge for edge in e.data.edges if edge["source"] == "col_dim_accounts_label")
    assert "breaking" not in calm_edge


def test_graph_node_and_edge_carry_new_optional_fields():
    """The GraphNode/GraphEdge additions default to None → today's payload when absent."""
    from parrant.lineage.display.html.explore import GraphEdge, GraphNode

    node = GraphNode(id="n", label="c", type="column", model="m").model_dump()
    assert node["semantic"] is None
    assert node["breaking"] is None
    assert node["boundary"] is None
    edge = GraphEdge(source="a", target="b").model_dump()
    assert edge["breaking"] is None
