import pytest
from dbt_column_lineage.lineage.display.html.display import HTMLDisplay
from dbt_column_lineage.artifacts.registry import ModelRegistry

@pytest.fixture
def registry(dbt_artifacts):
    """Create and load a model registry with real data."""
    registry = ModelRegistry(
        str(dbt_artifacts["catalog_path"]),
        str(dbt_artifacts["manifest_path"])
    )
    registry.load()
    return registry

def test_html_display_nodes(registry):
    """Test that the HTML display correctly renders column nodes."""
    html_display = HTMLDisplay(host="127.0.0.1", port=8000)
    
    models = registry.get_models()
    model = models["stg_transactions"]
    column = model.columns["amount"]
    
    html_display.display_column_info(column)
    
    downstream_models = {}
    if "int_transactions_enriched" in models:
        downstream_models["int_transactions_enriched"] = models["int_transactions_enriched"].columns
    if "int_monthly_account_metrics" in models:
        downstream_models["int_monthly_account_metrics"] = models["int_monthly_account_metrics"].columns
    
    html_display.display_downstream(downstream_models)
    data_dict = html_display.data.model_dump()
    
    # Verify column info for the main column
    assert data_dict["column_info"]["name"] == "amount"
    assert data_dict["column_info"]["model"] == "stg_transactions"
    
    # Verify all expected models are in the graph
    models_in_graph = {node["model"] for node in data_dict["nodes"]}
    assert "stg_transactions" in models_in_graph
    assert "int_transactions_enriched" in models_in_graph
    assert "int_monthly_account_metrics" in models_in_graph
    
    # Verify main column is present and marked as main
    main_column_id = f"col_stg_transactions_amount" 
    main_node = next((n for n in data_dict["nodes"] if n["id"] == main_column_id), None)
    assert main_node is not None, "Main column not found in graph"
    assert main_node["is_main"] is True, "Main column not marked as main"
    
    # Verify key downstream columns are present
    key_columns = [
        "int_transactions_enriched.amount",
        "int_transactions_enriched.amount_category",
        "int_monthly_account_metrics.total_amount",
        "int_monthly_account_metrics.avg_amount"
    ]
    
    columns_in_graph = {f"{node['model']}.{node['label']}" for node in data_dict["nodes"]}
    
    for column in key_columns:
        assert column in columns_in_graph, f"Expected column {column} not found in graph"