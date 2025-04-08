import pytest
from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.lineage.service import LineageService
from pathlib import Path

@pytest.fixture
def registry(dbt_artifacts):
    """Create and load a model registry with real data."""
    registry = ModelRegistry(
        str(dbt_artifacts["catalog_path"]),
        str(dbt_artifacts["manifest_path"])
    )
    registry.load()
    return registry

@pytest.fixture
def lineage_service(dbt_artifacts):
    """Create a LineageService instance."""
    return LineageService(
        catalog_path=Path(dbt_artifacts["catalog_path"]),
        manifest_path=Path(dbt_artifacts["manifest_path"])
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
    assert start_model_name in models_in_graph, f"Starting model '{start_model_name}' not found in graph nodes"