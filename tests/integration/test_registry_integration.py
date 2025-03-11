import pytest
from dbt_column_lineage.registry.registry import ModelRegistry

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
    """Test complete lineage chain from marts to staging to source."""
    models = registry.get_models()
    
    lineage_tests = [
        ("transactions", "account_id", "stg_accounts.account_id"),
        ("transactions", "country_code", "stg_countries.country_code"),
        ("transactions", "transaction_id", "stg_transactions.transaction_id"),
        ("stg_accounts", "account_id", "raw_accounts.id"),
        ("stg_countries", "country_code", "raw_countries.code"),
    ]
    
    for model_name, column_name, expected_source in lineage_tests:
        model = models[model_name]
        column = model.columns[column_name]
        assert column.lineage
        
        sources = set()
        for lineage in column.lineage:
            sources.update(lineage.source_columns)
        
        assert expected_source in sources or any(expected_source in src for src in sources), \
            f"{model_name}.{column_name} should trace to {expected_source}, got {sources}"
    
    upstream_tests = [
        ("transactions", ["stg_accounts", "stg_countries", "stg_transactions"]),
        ("accounts_tiering", ["stg_transactions", "stg_accounts"]),
        ("stg_transactions", ["raw_transactions"]),
        ("stg_accounts", ["raw_accounts"]),
        ("stg_countries", ["raw_countries"]),
    ]
    
    for model_name, expected_upstream in upstream_tests:
        model = models[model_name]
        for upstream in expected_upstream:
            print(model.name)
            print(model.upstream)
            assert upstream in model.upstream, f"{model_name} should depend on {upstream}"
    
    downstream_tests = [
        ("transactions", []),
        ("accounts_tiering", []),
        ("stg_accounts", ["transactions", "accounts_tiering"]),
        ("stg_transactions", ["transactions", "accounts_tiering"]),
        ("stg_countries", ["transactions", "accounts_tiering"]),
    ]
    
    for model_name, expected_downstream in downstream_tests:
        model = models[model_name]
        for downstream in expected_downstream:
            assert downstream in model.downstream, f"{model_name} should have {downstream} as downstream"

        assert len(model.downstream) == len(expected_downstream), \
            f"{model_name} has unexpected downstream models: {model.downstream - set(expected_downstream)}"

def test_compiled_sql_from_test_project(registry):
    """Test getting compiled SQL from test project."""
    sql = registry.get_compiled_sql("transactions")
    assert "select" in sql.lower()
    assert "from" in sql.lower()
    assert "join" in sql.lower() 