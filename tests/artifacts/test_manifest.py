import json
import pytest
from pathlib import Path
from src.artifacts.manifest import ManifestReader

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
                "depends_on": {
                    "nodes": []
                }
            },
            "model.jaffle_shop.stg_orders": {
                "name": "stg_orders",
                "resource_type": "model",
                "depends_on": {
                    "nodes": []
                }
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
                    WITH monthly_orders AS (
                        SELECT 
                            user_id,
                            DATE_TRUNC('month', order_date) as order_month,
                            COUNT(*) as order_count,
                            SUM(amount) as monthly_amount
                        FROM orders
                        GROUP BY 1, 2
                    )
                    SELECT 
                        c.id,
                        c.name,
                        mo.order_month,
                        mo.order_count,
                        mo.monthly_amount
                    FROM customers c
                    JOIN monthly_orders mo ON mo.user_id = c.id
                """
            }
        }
    }
    
    manifest_path = tmp_path / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    
    return manifest_path

def test_manifest_reader_initialization():
    """Test ManifestReader initialization."""
    reader = ManifestReader("some/path")
    assert reader.manifest_path == Path("some/path")
    assert reader.manifest == {}

def test_manifest_reader_load(sample_manifest):
    """Test loading manifest file."""
    reader = ManifestReader(sample_manifest)
    reader.load()
    
    assert "nodes" in reader.manifest
    assert len(reader.manifest["nodes"]) == 3

def test_manifest_reader_load_nonexistent_file():
    """Test loading non-existent manifest file."""
    reader = ManifestReader("nonexistent/path")
    with pytest.raises(FileNotFoundError):
        reader.load()

def test_get_model_dependencies(sample_manifest):
    """Test getting model dependencies."""
    reader = ManifestReader(sample_manifest)
    reader.load()
    
    dependencies = reader.get_model_upstream()
    
    assert "model.jaffle_shop.customers" in dependencies
    assert dependencies["model.jaffle_shop.customers"] == {
        "stg_customers.stg_customers",
        "stg_orders.stg_orders"
    }
    
    assert "model.jaffle_shop.stg_customers" in dependencies
    assert dependencies["model.jaffle_shop.stg_customers"] == set()

def test_get_model_dependencies_empty_manifest():
    """Test getting model dependencies with empty manifest."""
    reader = ManifestReader("some/path")
    reader.manifest = {"nodes": {}}
    
    dependencies = reader.get_model_upstream()
    assert dependencies == {}

def test_complex_model_dependencies(real_manifest):
    """Test dependencies with a more complex model structure."""
    reader = ManifestReader(real_manifest)
    reader.load()
    
    dependencies = reader.get_model_upstream()
    
    # Test direct dependencies
    assert dependencies["model.jaffle_shop.orders"] == {"raw_orders.raw_orders"}
    assert dependencies["model.jaffle_shop.customers"] == {
        "raw_customers.raw_customers",
        "orders.orders"
    }
    assert dependencies["model.jaffle_shop.customer_orders"] == {
        "customers.customers",
        "orders.orders"
    }

def test_model_chain_dependencies(real_manifest):
    """Test chained model dependencies (models depending on other models)."""
    reader = ManifestReader(real_manifest)
    reader.load()
    
    dependencies = reader.get_model_upstream()
    
    # customer_orders depends on customers which depends on orders
    assert "orders.orders" in dependencies["model.jaffle_shop.customer_orders"]
    assert "customers.customers" in dependencies["model.jaffle_shop.customer_orders"]

def test_model_with_cte(real_manifest):
    """Test model with CTE structure."""
    reader = ManifestReader(real_manifest)
    reader.load()
    
    dependencies = reader.get_model_upstream()
    
    # customer_orders uses a CTE but should still show correct dependencies
    assert dependencies["model.jaffle_shop.customer_orders"] == {
        "customers.customers",
        "orders.orders"
    } 