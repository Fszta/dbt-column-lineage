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

@pytest.fixture
def sample_catalog_with_sources(tmp_path):
    """Create a sample catalog file that includes sources for testing."""
    catalog_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "unique_id": "model.jaffle_shop.customers",
                "name": "customers",
                "schema": "jaffle_shop",
                "database": "raw",
                "resource_type": "model",
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "Primary key",
                        "data_type": "integer",
                        "model_name": "customers"
                    }
                }
            }
        },
        "sources": {
            "source.jaffle_shop.raw_customers": {
                "unique_id": "source.jaffle_shop.raw_customers",
                "name": "raw_customers",
                "source_name": "raw",
                "schema": "jaffle_shop",
                "database": "raw",
                "resource_type": "source",
                "metadata": {
                    "name": "raw_customers_table",  # This is the identifier
                    "type": "source"
                },
                "columns": {
                    "id": {
                        "name": "id",
                        "data_type": "integer",
                        "model_name": "raw_customers"
                    },
                    "name": {
                        "name": "name",
                        "data_type": "varchar",
                        "model_name": "raw_customers"
                    }
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

def test_source_processing(sample_catalog_with_sources):
    """Test processing of sources from catalog."""
    reader = CatalogReader(sample_catalog_with_sources)
    reader.load()
    
    models = reader.get_models_nodes()
    
    # Check that both model and source are loaded
    assert len(models) == 2
    assert "customers" in models
    assert "raw_customers_table" in models  # Should use identifier
    
    # Check model properties
    model = models["customers"]
    assert model.resource_type == "model"
    assert model.source_identifier is None
    
    # Check source properties
    source = models["raw_customers_table"]
    assert source.resource_type == "source"
    assert source.source_identifier == "raw_customers_table"
    assert source.name == "raw_customers"
    assert "id" in source.columns
    assert "name" in source.columns
    
    # Check source column properties
    source_column = source.columns["id"]
    assert source_column.model_name == "raw_customers_table"
    assert source_column.data_type == "integer"
    assert source_column.full_name == "raw_customers_table.id"

def test_source_without_identifier(tmp_path):
    """Test handling of sources without explicit identifiers."""
    catalog_data = {
        "sources": {
            "source.jaffle_shop.raw_orders": {
                "unique_id": "source.jaffle_shop.raw_orders",
                "name": "raw_orders",
                "source_name": "raw",
                "schema": "jaffle_shop",
                "database": "raw",
                "resource_type": "source",
                "metadata": {
                    "type": "source"
                },
                "columns": {
                    "id": {"name": "id", "data_type": "integer"}
                }
            }
        }
    }
    
    catalog_path = tmp_path / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog_data, f)
    
    reader = CatalogReader(catalog_path)
    reader.load()
    models = reader.get_models_nodes()
    
    # Should fall back to source name when identifier is not present
    assert "raw_orders" in models
    source = models["raw_orders"]
    assert source.resource_type == "source"
    assert source.source_identifier is None
    assert source.name == "raw_orders" 