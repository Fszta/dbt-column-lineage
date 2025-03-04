import json
import pytest
from pathlib import Path
from src.artifacts.catalog import CatalogReader

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
                "metadata": {
                    "owner": "analytics",
                    "updated_at": "2025-02-28"
                },
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
                "metadata": {
                    "owner": "analytics",
                    "updated_at": "2024-01-01"
                },
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "Order ID",
                        "data_type": "integer",
                        "model_name": "orders"
                    },
                    "amount": {
                        "name": "amount",
                        "description": "Order amount",
                        "data_type": "decimal",
                        "model_name": "orders"
                    }
                }
            }
        }
    }
    
    catalog_path = tmp_path / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog_data, f)
    
    return catalog_path

def test_catalog_reader_initialization():
    """Test CatalogReader initialization."""
    reader = CatalogReader("some/path")
    assert reader.catalog_path == Path("some/path")
    assert reader.catalog == {}

def test_catalog_reader_load(sample_catalog):
    """Test loading catalog file."""
    reader = CatalogReader(sample_catalog)
    reader.load()
    
    assert "nodes" in reader.catalog
    assert len(reader.catalog["nodes"]) == 2

def test_catalog_reader_load_nonexistent_file():
    """Test loading non-existent catalog file."""
    reader = CatalogReader("nonexistent/path")
    with pytest.raises(FileNotFoundError):
        reader.load()

def test_get_models_nodes(sample_catalog):
    """Test getting models from catalog."""
    reader = CatalogReader(sample_catalog)
    reader.load()
    
    models = reader.get_models_nodes()
    
    assert len(models) == 2
    assert "customers" in models
    assert "orders" in models
    
    # Test first model
    customers = models["customers"]
    assert customers.schema_name == "jaffle_shop"
    assert customers.database == "raw"
    assert len(customers.columns) == 2
    assert customers.columns["id"].data_type == "integer"
    assert customers.columns["name"].description == "Customer name"
    
    # Test second model
    orders = models["orders"]
    assert orders.schema_name == "jaffle_shop"
    assert len(orders.columns) == 2
    assert orders.columns["amount"].data_type == "decimal"

def test_get_models_nodes_empty_catalog():
    """Test getting models from empty catalog."""
    reader = CatalogReader("some/path")
    reader.catalog = {"nodes": {}}
    
    models = reader.get_models_nodes()
    assert models == {}

def test_model_column_properties():
    """Test model column properties and relationships."""
    reader = CatalogReader("some/path")
    reader.catalog = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "schema_name": "jaffle_shop",
                "database": "raw",
                "columns": {
                    "id": {
                        "name": "id",
                        "model_name": "customers",
                        "data_type": "integer"
                    }
                }
            }
        }
    }
    
    models = reader.get_models_nodes()
    model = models["customers"]
    
    # Test column full_name property
    column = model.columns["id"]
    assert column.full_name == "customers.id" 