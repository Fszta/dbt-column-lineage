import json
import pytest
from pathlib import Path
from src.registry.registry import ModelRegistry
from src.registry.exceptions import ModelNotFoundError, RegistryNotLoadedError

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
                    "id": {
                        "name": "id",
                        "description": "Order ID",
                        "data_type": "integer",
                        "model_name": "orders"
                    },
                    "customer_id": {
                        "name": "customer_id",
                        "description": "Customer FK",
                        "data_type": "integer",
                        "model_name": "orders"
                    }
                }
            },
            "model.jaffle_shop.order_items": {
                "unique_id": "model.jaffle_shop.order_items",
                "name": "order_items",
                "schema": "jaffle_shop",
                "database": "raw",
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "Order FK",
                        "data_type": "integer",
                        "model_name": "order_items"
                    },
                    "quantity": {
                        "name": "quantity",
                        "data_type": "integer",
                        "model_name": "order_items"
                    }
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
                "depends_on": {
                    "nodes": []
                }
            },
            "model.jaffle_shop.orders": {
                "name": "orders",
                "depends_on": {
                    "nodes": [
                        {"name": "model.jaffle_shop", "alias": "customers"}
                    ]
                }
            },
            "model.jaffle_shop.order_items": {
                "name": "order_items",
                "depends_on": {
                    "nodes": [
                        {"name": "model.jaffle_shop", "alias": "orders"}
                    ]
                }
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

def test_registry_initialization(sample_catalog, sample_manifest):
    """Test ModelRegistry initialization."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    assert registry.catalog_reader is not None
    assert registry.manifest_reader is not None

def test_registry_load(sample_catalog, sample_manifest):
    """Test loading both catalog and manifest."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    registry.load()

def test_get_single_existing_model(sample_catalog, sample_manifest):
    """Test getting a model from the registry."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    registry.load()
    model = registry.get_model("customers")
    assert model is not None


def test_get_nonexistent_model(sample_catalog, sample_manifest):
    """Test getting a nonexistent model from the registry."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    registry.load()
    with pytest.raises(ModelNotFoundError):
        registry.get_model("foobar_model")

def test_get_models_with_dependencies(sample_catalog, sample_manifest):
    """Test getting models enriched with dependencies."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    registry.load()
    
    models = registry.get_models()
    
    # Test upstream dependencies
    assert not models["customers"].upstream  # No upstream deps
    assert "customers" in models["orders"].upstream  # Orders depends on customers
    assert "orders" in models["order_items"].upstream  # Order items depends on orders
    
    # Test downstream dependencies
    assert "orders" in models["customers"].downstream  # Customers is used by orders
    assert "order_items" in models["orders"].downstream  # Orders is used by order items
    assert not models["order_items"].downstream  # No downstream deps

def test_registry_not_loaded(sample_catalog, sample_manifest):
    """Test accessing models before registry is loaded."""
    registry = ModelRegistry(sample_catalog, sample_manifest)
    with pytest.raises(RegistryNotLoadedError):
        registry.get_models()
