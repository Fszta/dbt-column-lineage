import json
import pytest
from pathlib import Path
from dbt_column_lineage.artifacts.catalog import CatalogReader

@pytest.fixture
def sample_catalog(tmp_path):
    """Create a sample catalog file for testing."""
    catalog_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "unique_id": "model.jaffle_shop.customers",
                "name": "customers",
                "schema": "jaffle_shop",
                "database": "raw",
                "metadata": {"owner": "analytics"},
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "Primary key",
                        "data_type": "integer",
                        "model_name": "customers"
                    },
                    "name": {
                        "name": "name",
                        "description": "Customer name",
                        "data_type": "varchar",
                        "model_name": "customers"
                    }
                }
            },
            "model.jaffle_shop.orders": {
                "unique_id": "model.jaffle_shop.orders",
                "name": "orders",
                "schema": "jaffle_shop",
                "database": "raw",
                "columns": {
                    "id": {"name": "id", "data_type": "integer", "model_name": "orders"},
                    "amount": {"name": "amount", "data_type": "decimal", "model_name": "orders"}
                }
            }
        }
    }
    
    catalog_path = tmp_path / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog_data, f)
    
    return catalog_path

def test_catalog_reader_basics():
    """Test CatalogReader init."""
    reader = CatalogReader("some/path")
    assert reader.catalog_path == Path("some/path")
    assert reader.catalog == {}
    
    with pytest.raises(FileNotFoundError):
        CatalogReader("nonexistent/path").load()

def test_catalog_loading_and_parsing(sample_catalog):
    """Test loading and parsing catalog file."""
    reader = CatalogReader(sample_catalog)
    reader.load()
    
    assert "nodes" in reader.catalog
    assert len(reader.catalog["nodes"]) == 2
    
    models = reader.get_models_nodes()
    assert len(models) == 2
    assert set(models.keys()) == {"customers", "orders"}
    
    for model_name, expected in {
        "customers": {
            "schema": "jaffle_shop", 
            "database": "raw",
            "columns": {"id", "name"},
            "column_types": {"id": "integer", "name": "varchar"}
        },
        "orders": {
            "schema": "jaffle_shop", 
            "database": "raw",
            "columns": {"id", "amount"},
            "column_types": {"id": "integer", "amount": "decimal"}
        }
    }.items():
        model = models[model_name]
        assert model.schema_name == expected["schema"]
        assert model.database == expected["database"]
        assert set(model.columns.keys()) == expected["columns"]
        for col, dtype in expected["column_types"].items():
            assert model.columns[col].data_type == dtype

def test_empty_catalog():
    """Test handling of empty catalog."""
    reader = CatalogReader("some/path")
    reader.catalog = {"nodes": {}}
    assert reader.get_models_nodes() == {}

def test_column_properties():
    """Test column properties."""
    reader = CatalogReader("some/path")
    reader.catalog = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "schema_name": "jaffle_shop",
                "database": "raw",
                "columns": {
                    "id": {"name": "id", "model_name": "customers", "data_type": "integer"}
                }
            }
        }
    }
    
    models = reader.get_models_nodes()
    column = models["customers"].columns["id"]
    assert column.full_name == "customers.id" 