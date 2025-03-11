import json
import pytest
from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.artifacts.exceptions import ModelNotFoundError, RegistryNotLoadedError

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
                "columns": {
                    "id": {
                        "name": "id",
                        "description": "Primary key",
                        "data_type": "integer",
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
                    "customer_id": {"name": "customer_id", "data_type": "integer", "model_name": "orders"}
                }
            },
            "model.jaffle_shop.order_items": {
                "unique_id": "model.jaffle_shop.order_items",
                "name": "order_items",
                "schema": "jaffle_shop",
                "database": "raw",
                "columns": {
                    "order_id": {"name": "order_id", "data_type": "integer", "model_name": "order_items"},
                    "quantity": {"name": "quantity", "data_type": "integer", "model_name": "order_items"}
                }
            }
        }
    }
    
    catalog_path = tmp_path / "catalog.json"
    with open(catalog_path, "w") as f:
        json.dump(catalog_data, f)
    
    return catalog_path

@pytest.fixture
def sample_manifest(tmp_path):
    """Create a sample manifest file for testing."""
    manifest_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "resource_type": "model",
                "depends_on": {"nodes": []}
            },
            "model.jaffle_shop.orders": {
                "name": "orders",
                "resource_type": "model",
                "depends_on": {
                    "nodes": ["model.jaffle_shop.customers"]
                }
            },
            "model.jaffle_shop.order_items": {
                "name": "order_items",
                "resource_type": "model",
                "depends_on": {
                    "nodes": ["model.jaffle_shop.orders"]
                }
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

def test_registry_basics(sample_catalog, sample_manifest):
    """Test registry initialization, loading, and error handling."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    assert registry.catalog_reader is not None
    assert registry.manifest_reader is not None
    
    with pytest.raises(RegistryNotLoadedError):
        registry.get_models()
    
    registry.load()
    
    model = registry.get_model("customers")
    assert model is not None
    
    with pytest.raises(ModelNotFoundError):
        registry.get_model("foobar_model")

def test_model_dependencies(sample_catalog, sample_manifest):
    """Test model dependency relationships."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    registry.load()
    
    models = registry.get_models()
    
    dependency_tests = {
        "customers": {
            "upstream": set(),
            "downstream": {"orders"}
        },
        "orders": {
            "upstream": {"customers"},
            "downstream": {"order_items"}
        },
        "order_items": {
            "upstream": {"orders"},
            "downstream": set()
        }
    }
    
    for model_name, expected in dependency_tests.items():
        model = models[model_name]
        assert model.upstream == expected["upstream"], f"Expected {model_name}.upstream to be {expected['upstream']}, got {model.upstream}"
        assert model.downstream == expected["downstream"], f"Expected {model_name}.downstream to be {expected['downstream']}, got {model.downstream}"
