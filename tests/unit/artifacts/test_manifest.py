import json
import pytest
from pathlib import Path
from dbt_column_lineage.artifacts.manifest import ManifestReader

@pytest.fixture
def sample_manifest(tmp_path):
    """Create a sample manifest file for testing."""
    manifest_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "resource_type": "model",
                "depends_on": {
                    "nodes": [
                        {"name": "stg_customers", "alias": "stg_customers"},
                        {"name": "stg_orders", "alias": "stg_orders"}
                    ]
                }
            },
            "model.jaffle_shop.stg_customers": {
                "name": "stg_customers",
                "resource_type": "model",
                "depends_on": {"nodes": []}
            },
            "model.jaffle_shop.stg_orders": {
                "name": "stg_orders",
                "resource_type": "model",
                "depends_on": {"nodes": []}
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

@pytest.fixture
def real_manifest(tmp_path):
    """Create a fixture with real manifest data."""
    manifest_data = {
        "nodes": {
            "model.jaffle_shop.orders": {
                "name": "orders",
                "resource_type": "model",
                "depends_on": {
                    "nodes": [
                        {"name": "raw_orders", "alias": "raw_orders"}
                    ]
                },
                "compiled_sql": "SELECT id, user_id, order_date, status, amount FROM raw_orders",
                "columns": {
                    "id": {"name": "id", "description": "Primary key"},
                    "user_id": {"name": "user_id", "description": "Foreign key to users"},
                    "amount": {"name": "amount", "description": "Order amount"}
                }
            },
            "model.jaffle_shop.customers": {
                "name": "customers",
                "resource_type": "model",
                "depends_on": {
                    "nodes": [
                        {"name": "raw_customers", "alias": "raw_customers"},
                        {"name": "orders", "alias": "orders"}
                    ]
                },
                "compiled_sql": """
                    SELECT 
                        c.id,
                        c.name,
                        c.email,
                        COUNT(o.id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM raw_customers c
                    LEFT JOIN orders o ON o.user_id = c.id
                    GROUP BY c.id, c.name, c.email
                """,
                "columns": {
                    "id": {"name": "id", "description": "Customer ID"},
                    "name": {"name": "name", "description": "Customer name"},
                    "order_count": {"name": "order_count", "description": "Number of orders"},
                    "total_amount": {"name": "total_amount", "description": "Total spent"}
                }
            },
            "model.jaffle_shop.customer_orders": {
                "name": "customer_orders",
                "resource_type": "model",
                "depends_on": {
                    "nodes": [
                        {"name": "customers", "alias": "customers"},
                        {"name": "orders", "alias": "orders"}
                    ]
                },
                "compiled_sql": """
                    WITH customer_data AS (
                        SELECT * FROM customers
                    ),
                    order_data AS (
                        SELECT * FROM orders
                    )
                    SELECT 
                        c.id as customer_id,
                        c.name,
                        COUNT(o.id) as order_count,
                        SUM(o.amount) as total_amount
                    FROM customer_data c
                    LEFT JOIN order_data o ON o.user_id = c.id
                    GROUP BY c.id, c.name
                """,
                "columns": {
                    "customer_id": {"name": "customer_id", "description": "Customer ID"},
                    "name": {"name": "name", "description": "Customer name"},
                    "order_count": {"name": "order_count", "description": "Number of orders"},
                    "total_amount": {"name": "total_amount", "description": "Total spent"}
                }
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

@pytest.fixture
def sample_manifest_with_sources_identifier(tmp_path):
    """Create a sample manifest file that includes sources with identifier."""
    manifest_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "resource_type": "model",
                "depends_on": {
                    "nodes": ["source.jaffle_shop.raw_customers"]
                }
            }
        },
        "sources": {
            "source.jaffle_shop.raw_customers": {
                "name": "raw_customers",
                "source_name": "raw",
                "resource_type": "source",
                "identifier": "raw_customers_table"
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

def test_manifest_reader_basics():
    """Test ManifestReader initialization and error handling."""
    reader = ManifestReader("some/path")
    assert reader.manifest_path == Path("some/path")
    assert reader.manifest == {}
    
    with pytest.raises(FileNotFoundError):
        ManifestReader("nonexistent/path").load()

def test_manifest_loading_and_dependencies(sample_manifest):
    """Test loading manifest and extracting dependencies."""
    reader = ManifestReader(sample_manifest)
    reader.load()
    
    assert "nodes" in reader.manifest
    assert len(reader.manifest["nodes"]) == 3
    
    dependencies = reader.get_model_dependencies()
    
    assert "model.jaffle_shop.customers" in dependencies
    assert dependencies["model.jaffle_shop.customers"] == {
        "stg_customers.stg_customers",
        "stg_orders.stg_orders"
    }
    
    assert "model.jaffle_shop.stg_customers" in dependencies
    assert dependencies["model.jaffle_shop.stg_customers"] == set()

def test_empty_manifest():
    """Test handling of empty manifest."""
    reader = ManifestReader("some/path")
    reader.manifest = {"nodes": {}}
    assert reader.get_model_dependencies() == {}

def test_complex_dependencies(real_manifest):
    """Test dependencies with a more complex model structure."""
    reader = ManifestReader(real_manifest)
    reader.load()
    
    dependencies = reader.get_model_dependencies()
    
    dependency_tests = {
        "model.jaffle_shop.orders": {"raw_orders.raw_orders"},
        "model.jaffle_shop.customers": {"raw_customers.raw_customers", "orders.orders"},
        "model.jaffle_shop.customer_orders": {"customers.customers", "orders.orders"}
    }
    
    for model, expected_deps in dependency_tests.items():
        assert dependencies[model] == expected_deps 

def test_source_dependencies(sample_manifest_with_sources_identifier):
    """Test handling of source dependencies in manifest."""
    reader = ManifestReader(sample_manifest_with_sources_identifier)
    reader.load()
    
    upstream = reader.get_model_upstream()    
    assert "customers" in upstream
    assert "raw_customers_table" in upstream["customers"]
    
    downstream = reader.get_model_downstream()
    assert "raw_customers_table" in downstream
    assert "customers" in downstream["raw_customers_table"]

def test_source_dependencies_without_identifier(tmp_path):
    """Test handling of source dependencies when source has no identifier."""
    manifest_data = {
        "nodes": {
            "model.jaffle_shop.customers": {
                "name": "customers",
                "resource_type": "model",
                "depends_on": {
                    "nodes": ["source.jaffle_shop.raw_customers"]
                }
            }
        },
        "sources": {
            "source.jaffle_shop.raw_customers": {
                "name": "raw_customers",
                "source_name": "raw",
                "resource_type": "source"
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    reader = ManifestReader(manifest_path)
    reader.load()
    
    upstream = reader.get_model_upstream()
    
    # Should fall back to source name when identifier is not present
    assert "customers" in upstream
    assert "raw_customers" in upstream["customers"] 