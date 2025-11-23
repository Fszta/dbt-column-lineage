import pytest
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.lineage.service import LineageService
from pathlib import Path


@pytest.fixture
def registry(dbt_artifacts):
    """Create and load a model registry with real data."""
    registry = ModelRegistry(
        str(dbt_artifacts["catalog_path"]), str(dbt_artifacts["manifest_path"])
    )
    registry.load()
    return registry


@pytest.fixture
def lineage_service(dbt_artifacts):
    """Create a LineageService instance."""
    return LineageService(
        catalog_path=Path(dbt_artifacts["catalog_path"]),
        manifest_path=Path(dbt_artifacts["manifest_path"]),
    )


def test_html_display_nodes(lineage_service, registry):
    """Test that the HTML display correctly processes lineage."""
    lineage_explorer = LineageExplorer(host="127.0.0.1", port=8000)
    lineage_explorer.set_lineage_service(lineage_service)

    start_model_name = "stg_transactions"
    start_column_name = "amount"
    model_obj = registry.get_model(start_model_name)
    column_obj = model_obj.columns.get(start_column_name)

    lineage_explorer._set_column_info(column_obj)
    expected_main_node_id = f"col_{start_model_name}_{start_column_name}"
    lineage_explorer.data.main_node = expected_main_node_id

    lineage_explorer._process_lineage_tree(start_model_name, start_column_name)

    data_dict = lineage_explorer.data.model_dump()

    assert data_dict["column_info"] is not None
    assert data_dict["column_info"]["name"] == start_column_name
    assert data_dict["column_info"]["model"] == start_model_name

    assert data_dict["main_node"] == expected_main_node_id

    main_node = next((n for n in data_dict["nodes"] if n["id"] == expected_main_node_id), None)
    assert main_node is not None, f"Main node {expected_main_node_id} not found in graph nodes"
    assert main_node["is_main"] is True, "Main node not marked as main"
    assert main_node["model"] == start_model_name
    assert main_node["label"] == start_column_name

    assert len(data_dict["nodes"]) > 0, "No nodes were generated in the graph"

    assert len(data_dict["edges"]) > 0, "No edges were generated, expected lineage"

    models_in_graph = {node["model"] for node in data_dict["nodes"]}
    assert (
        start_model_name in models_in_graph
    ), f"Starting model '{start_model_name}' not found in graph nodes"


def test_lineage_includes_impact_summary(lineage_service):
    """Test that the lineage endpoint includes impact_summary in the response."""
    lineage_explorer = LineageExplorer(host="127.0.0.1", port=8000)
    lineage_explorer.set_lineage_service(lineage_service)

    # Test with a column that has downstream dependencies
    model = "stg_transactions"
    column = "amount"

    lineage_explorer._process_lineage_tree(model, column)

    # Get impact summary - this is what the endpoint does
    impact_data = lineage_service.get_column_impact(model, column)
    if impact_data and "summary" in impact_data:
        lineage_explorer.data.impact_summary = impact_data["summary"]
    else:
        lineage_explorer.data.impact_summary = None

    data = lineage_explorer.data.model_dump(exclude_none=False)

    assert "impact_summary" in data, "Lineage response should include impact_summary field"

    if data["impact_summary"] is not None:
        summary = data["impact_summary"]
        assert "critical_count" in summary, "Impact summary should include critical_count"
        assert "low_impact_count" in summary, "Impact summary should include low_impact_count"
        assert "affected_models" in summary, "Impact summary should include affected_models"
        assert "affected_exposures" in summary, "Impact summary should include affected_exposures"

        assert isinstance(summary["critical_count"], int)
        assert isinstance(summary["low_impact_count"], int)
        assert isinstance(summary["affected_models"], int)
        assert isinstance(summary["affected_exposures"], int)

        # Verify the summary has reasonable values for a column with downstream dependencies
        assert summary["affected_models"] >= 0
        assert summary["affected_exposures"] >= 0
        assert summary["critical_count"] >= 0
        assert summary["low_impact_count"] >= 0


def test_lineage_without_impact_summary(lineage_service):
    """Test that lineage data is present even when impact_summary is None."""
    lineage_explorer = LineageExplorer(host="127.0.0.1", port=8000)
    lineage_explorer.set_lineage_service(lineage_service)

    model = "stg_transactions"
    column = "amount"

    lineage_explorer._process_lineage_tree(model, column)

    # Explicitly set impact_summary to None to test the case where it's not available
    lineage_explorer.data.impact_summary = None

    data = lineage_explorer.data.model_dump(exclude_none=False)

    # Verify that even if impact_summary is None, the lineage data is still present
    assert "impact_summary" in data
    assert data["impact_summary"] is None
    assert "nodes" in data, "Lineage data should still be present"
    assert "edges" in data, "Lineage edges should still be present"
    assert len(data["nodes"]) > 0, "Should have nodes even when impact_summary is None"
    assert len(data["edges"]) > 0, "Should have edges even when impact_summary is None"
