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
        
        # TODO Check this assertion
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