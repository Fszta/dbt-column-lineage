import pytest
from dbt_column_lineage.artifacts.registry import ModelRegistry

@pytest.fixture
def registry(dbt_artifacts):
    """Create and load a registry instance."""
    registry = ModelRegistry(
        str(dbt_artifacts["catalog_path"]),
        str(dbt_artifacts["manifest_path"])
    )
    registry.load()
    return registry

def test_complete_lineage_chain(registry):
    """Test complete lineage chain from marts to staging through intermediate models."""
    models = registry.get_models()
    
    lineage_tests = [
        ("stg_accounts", "account_id", "raw_accounts.id"),
        ("stg_countries", "country_code", "raw_countries.code"),

        
        ("int_monthly_account_metrics", "account_id", "int_transactions_enriched.account_id"),
        ("int_monthly_account_metrics", "account_holder", "int_transactions_enriched.account_holder"),
        ("int_transactions_enriched", "transaction_id", "stg_transactions.transaction_id"),
        ("int_transactions_enriched", "account_holder", "stg_accounts.account_holder"),
        
        ("accounts_tiering", "account_id", "int_monthly_account_metrics.account_id"),
        ("accounts_tiering", "total_executed_amount", "int_monthly_account_metrics.executed_amount"),
        ("accounts_tiering", "country_code", "stg_countries.country_code"),
    ]
    
    for model_name, column_name, expected_source in lineage_tests:
        model = models[model_name]
        column = model.columns[column_name]
        assert column.lineage, f"No lineage found for {model_name}.{column_name}"

        assert model.language == "sql"

        sources = set()
        for lineage in column.lineage:
            sources.update(lineage.source_columns)
        
        assert expected_source in sources or any(expected_source in src for src in sources), \
            f"{model_name}.{column_name} should trace to {expected_source}, got {sources}"

def test_model_dependencies(registry):
    """Test model dependency chains."""
    models = registry.get_models()
    
    upstream_tests = [
        ("stg_transactions", ["raw_transactions"]),
        ("stg_accounts", ["raw_accounts"]),
        ("stg_countries", ["raw_countries"]),

        
        ("int_monthly_account_metrics", ["int_transactions_enriched"]),
        ("int_transactions_enriched", ["stg_transactions", "stg_accounts"]),
        
        ("accounts_tiering", [
            "int_monthly_account_metrics",
            "stg_countries"
        ]),
        
    ]
    
    for model_name, expected_upstream in upstream_tests:
        model = models[model_name]
        for upstream in expected_upstream:
            assert upstream in model.upstream, f"{model_name} should depend on {upstream}"

def test_downstream_dependencies(registry):
    """Test downstream dependency chains."""
    models = registry.get_models()
    
    downstream_tests = [
        ("stg_accounts", ["int_transactions_enriched"]),
        ("stg_transactions", ["int_transactions_enriched"]),
        
        ("int_transactions_enriched", ["int_monthly_account_metrics"]),
        ("int_monthly_account_metrics", ["accounts_tiering"]),
        
        ("accounts_tiering", []),
    ]
    
    for model_name, expected_downstream in downstream_tests:
        model = models[model_name]
        for downstream in expected_downstream:
            assert downstream in model.downstream, \
                f"{model_name} should have {downstream} as downstream"

def test_derived_columns_lineage(registry):
    """Test lineage for derived/calculated columns."""
    models = registry.get_models()
    
    derived_column_tests = [
        ("int_monthly_account_metrics", "transaction_count", "derived"),
        ("int_monthly_account_metrics", "total_amount", "derived"),
        ("int_monthly_account_metrics", "avg_amount", "derived"),
        
        ("accounts_tiering", "account_tier", "derived"),
        ("int_transactions_enriched", "amount_category", "derived"),
    ]
    
    for model_name, column_name, expected_type in derived_column_tests:
        model = models[model_name]
        column = model.columns[column_name]
        assert column.lineage, f"No lineage found for {model_name}.{column_name}"
        
        assert any(l.transformation_type == expected_type for l in column.lineage), \
            f"{model_name}.{column_name} should have {expected_type} transformation"

def test_select_star_lineage(registry):
    """Test that select * properly maintains column lineage through multiple layers."""
    models = registry.get_models()
    
    # Test int_transactions_enriched inherits all columns from stg_transactions
    stg_model = models["stg_transactions"]
    int_model = models["int_transactions_enriched"]
    
    for col_name, _ in stg_model.columns.items():
        assert col_name in int_model.columns, f"Column {col_name} from stg_transactions should exist in int_transactions_enriched"
        int_column = int_model.columns[col_name]
        
        assert int_column.lineage, f"Column {col_name} should have lineage information"
        source_columns = {src for lineage in int_column.lineage for src in lineage.source_columns}
        expected_source = f"stg_transactions.{col_name}"
        assert expected_source in source_columns, f"Column should trace back to {expected_source}, got {source_columns}"

def test_compiled_sql_from_test_project(registry):
    """Test getting compiled SQL from test project."""
    sql = registry.get_compiled_sql("transactions")
    assert "select" in sql.lower()
    assert "from" in sql.lower()
    assert "join" in sql.lower()

def test_source_lineage_integration(registry):
    """Test source to model lineage integration."""
    models = registry.get_models()
    
    source_to_staging_tests = [
        ("stg_accounts", "account_id", "raw_accounts.id"),
        ("stg_accounts", "account_holder", "raw_accounts.holder"),
        ("stg_transactions", "transaction_id", "raw_transactions.id"),
        ("stg_transactions", "amount", "raw_transactions.amount"),
        ("stg_countries", "country_code", "raw_countries.code"),
        ("stg_countries", "country_name", "raw_countries.name"),
    ]
    
    for model_name, column_name, expected_source in source_to_staging_tests:
        model = models[model_name]
        column = model.columns[column_name]
        assert column.lineage, f"No lineage found for {model_name}.{column_name}"
        
        sources = set()
        for lineage in column.lineage:
            sources.update(lineage.source_columns)
        
        assert expected_source in sources, \
            f"{model_name}.{column_name} should trace to {expected_source}, got {sources}"

def test_source_model_types(registry):
    """Test that models and sources have correct resource types."""
    models = registry.get_models()
    
    # TODO: Add tests for seeds and tests when supported
    resource_type_tests = [
        ("raw_accounts", "source"),
        ("raw_transactions", "source"),
        ("raw_countries", "source"),
        ("stg_accounts", "model"),
        ("int_transactions_enriched", "model"),
        ("accounts_tiering", "model"),
    ]
    
    for node_name, expected_type in resource_type_tests:
        node = models[node_name]
        assert node.resource_type == expected_type, \
            f"{node_name} should be a {expected_type}, got {node.resource_type}"

def test_source_dependencies(registry):
    """Test source dependency relationships."""
    models = registry.get_models()
    
    source_dependency_tests = [
        ("stg_accounts", {"raw_accounts"}),
        ("stg_transactions", {"raw_transactions"}),
        ("stg_countries", {"raw_countries"}),
        ("int_transactions_enriched", {"stg_transactions", "stg_accounts"}),
    ]
    
    for model_name, expected_upstream in source_dependency_tests:
        model = models[model_name]
        for source in expected_upstream:
            assert source in model.upstream, \
                f"{model_name} should have {source} as upstream dependency"

def test_source_downstream_dependencies(registry):
    """Test source downstream dependency relationships."""
    models = registry.get_models()
    
    source_downstream_tests = [
        ("raw_accounts", {"stg_accounts"}),
        ("raw_transactions", {"stg_transactions"}),
        ("raw_countries", {"stg_countries"}),
    ]
    
    for source_name, expected_downstream in source_downstream_tests:
        source = models[source_name]
        for downstream_model in expected_downstream:
            assert downstream_model in source.downstream, \
                f"{source_name} should have {downstream_model} as downstream dependency"

def test_model_resource_paths(registry):
    """Test that models have the correct resource path."""
    models = registry.get_models()
    
    path_tests = [
        ("stg_accounts", "models/staging/stg_accounts.sql"),
        ("int_transactions_enriched", "models/intermediate/int_transactions_enriched.sql"),
        ("accounts_tiering", "models/marts/accounts_tiering.sql"),
    ]
    
    for model_name, expected_path in path_tests:
        model = models[model_name]
        assert model.resource_path == expected_path, \
            f"{model_name} should have resource path {expected_path}, got {model.resource_path}"

def test_exposures_loaded(registry):
    """Test that exposures are loaded correctly in the registry."""
    exposures = registry.get_exposures()
    
    assert len(exposures) == 3, f"Expected 3 exposures, got {len(exposures)}"
    
    expected_exposures = {
        "transactions_dashboard",
        "account_tiering_report",
        "api_transactions_endpoint"
    }
    
    actual_exposures = set(exposures.keys())
    assert actual_exposures == expected_exposures, \
        f"Expected exposures {expected_exposures}, got {actual_exposures}"
    
    dashboard = exposures["transactions_dashboard"]
    assert dashboard.type == "dashboard"
    assert dashboard.url == "https://example.com/dashboards/transactions"
    assert "transactions" in dashboard.depends_on_models
    
    api = exposures["api_transactions_endpoint"]
    assert api.type == "application"
    assert api.url == "https://api.example.com/v1/transactions"
    assert "transactions" in api.depends_on_models
    
    report = exposures["account_tiering_report"]
    assert report.type == "dashboard"
    assert "accounts_tiering" in report.depends_on_models

def test_exposures_as_downstream_dependencies(registry):
    """Test that exposures are added as downstream dependencies of models."""
    models = registry.get_models()
    
    transactions_model = models.get("transactions")
    if transactions_model:
        assert "transactions_dashboard" in transactions_model.downstream, \
            "transactions model should have transactions_dashboard as downstream"
        assert "api_transactions_endpoint" in transactions_model.downstream, \
            "transactions model should have api_transactions_endpoint as downstream"
    
    accounts_tiering_model = models["accounts_tiering"]
    assert "account_tiering_report" in accounts_tiering_model.downstream, \
        "accounts_tiering model should have account_tiering_report as downstream"

def test_impact_analysis(registry, dbt_artifacts):
    """Test impact analysis for a column - what would break if the column is modified."""
    from dbt_column_lineage.lineage.service import LineageService
    from pathlib import Path
    
    service = LineageService(
        Path(dbt_artifacts["catalog_path"]),
        Path(dbt_artifacts["manifest_path"])
    )
    
    # Test impact analysis for stg_transactions.amount
    # This column is used in multiple downstream models
    impact = service.get_column_impact("stg_transactions", "amount")
    
    assert "summary" in impact
    assert "affected_models" in impact
    assert "affected_columns" in impact
    assert "affected_exposures" in impact
    
    summary = impact["summary"]
    assert "affected_models" in summary
    assert "affected_columns" in summary
    assert "affected_exposures" in summary
    assert "critical_count" in summary
    assert "low_impact_count" in summary
    
    # stg_transactions.amount should affect downstream models
    # It's used in int_transactions_enriched and potentially in transactions
    assert summary["affected_models"] > 0, "stg_transactions.amount should affect at least one downstream model"
    assert summary["affected_columns"] > 0, "stg_transactions.amount should affect at least one downstream column"
    
    for model in impact["affected_models"]:
        assert "name" in model
        assert "resource_type" in model
        assert "schema" in model
        assert "database" in model
    
    for col in impact["affected_columns"]:
        assert "model" in col
        assert "column" in col
        assert "transformation_type" in col
        assert "severity" in col
        assert col["severity"] in ["critical", "low_impact"]
        assert col["transformation_type"] in ["direct", "renamed", "derived"]
    
    # Test impact analysis for a column with no downstream dependencies
    # accounts_tiering is a mart model, so its columns might not have downstream dependencies
    impact_no_deps = service.get_column_impact("accounts_tiering", "account_id")
    
    # Should still return valid structure even if no dependencies
    assert "summary" in impact_no_deps
    assert "affected_models" in impact_no_deps
    assert "affected_columns" in impact_no_deps
    
    # Test impact analysis for a derived column
    # int_monthly_account_metrics.total_amount is derived from stg_transactions.amount
    impact_derived = service.get_column_impact("int_monthly_account_metrics", "total_amount")
    
    assert "summary" in impact_derived
    # total_amount might be used in accounts_tiering
    assert isinstance(impact_derived["summary"]["affected_models"], int)
    assert isinstance(impact_derived["summary"]["affected_columns"], int)
    
    # Verify that critical vs low_impact classification is correct
    # Derived/transformed columns should be critical (transformation logic might break)
    # Direct/renamed columns should be low_impact (just pass-through, change propagates)
    for col in impact["affected_columns"]:
        if col["transformation_type"] == "derived":
            assert col["severity"] == "critical", \
                f"Column {col['model']}.{col['column']} with transformation_type {col['transformation_type']} should be critical (transformation logic might break)"
        elif col["transformation_type"] in ["direct", "renamed"]:
            assert col["severity"] == "low_impact", \
                f"Column {col['model']}.{col['column']} with transformation_type {col['transformation_type']} should be low_impact (just pass-through)"
