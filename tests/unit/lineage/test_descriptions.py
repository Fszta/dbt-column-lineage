"""Unit tests for surfacing dbt-authored descriptions in the JSON report surface.

These use synthetic ``Column`` objects and hand-built impact dicts, so they exercise
the JSON display layer without needing regenerated dbt artifacts. Descriptions are
intentionally NOT rendered in the markdown PR comment (kept lean); they live in the
machine-readable JSON (agent context) and the interactive explorer.
"""

import json

from dbt_column_lineage.lineage.display.json import JsonDisplay
from dbt_column_lineage.models.schema import Column


def _impact_with_descriptions():
    return {
        "summary": {
            "affected_models": 1,
            "affected_columns": 1,
            "affected_exposures": 0,
            "critical_count": 1,
            "low_impact_count": 0,
            "filter_count": 0,
        },
        "affected_models": [
            {
                "name": "int_enriched",
                "resource_type": "model",
                "schema": "main",
                "database": "main",
                "description": "Enriched transactions with account info",
            }
        ],
        "affected_columns": [
            {
                "model": "int_enriched",
                "column": "amount_category",
                "transformation_type": "derived",
                "sql_expression": "case when amount > 100 then 'high' end",
                "severity": "critical",
                "data_type": "text",
                "description": "Bucketed amount label",
            }
        ],
        "affected_exposures": [],
        "confidence": None,
    }


def test_json_display_emits_selected_and_impact_descriptions(capsys):
    display = JsonDisplay()
    column = Column(
        name="account_id",
        model_name="stg_accounts",
        data_type="integer",
        description="Unique account identifier",
    )
    display.display_column_info(column)
    display.set_model_description("Staging model for account data")
    display.set_impact(_impact_with_descriptions())
    display.save()

    data = json.loads(capsys.readouterr().out)

    # selected column + its parent model
    assert data["description"] == "Unique account identifier"
    assert data["model_description"] == "Staging model for account data"
    # descriptions flow onto downstream/affected models and columns
    assert data["impact"]["affected_models"][0]["description"] == (
        "Enriched transactions with account info"
    )
    assert data["impact"]["affected_columns"][0]["description"] == "Bucketed amount label"


def test_json_model_description_may_be_null(capsys):
    display = JsonDisplay()
    column = Column(name="c", model_name="m", data_type=None, description=None)
    display.display_column_info(column)
    display.set_model_description(None)
    display.save()

    data = json.loads(capsys.readouterr().out)
    assert data["model_description"] is None
    assert data["description"] is None
