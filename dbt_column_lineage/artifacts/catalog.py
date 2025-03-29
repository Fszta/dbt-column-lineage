from pathlib import Path
from typing import Dict, Any, Optional
import json
from dbt_column_lineage.models.schema import Model

class CatalogReader:
    def __init__(self, catalog_path: str):
        self.catalog_path = Path(catalog_path)
        self.catalog: Dict[str, Any] = {}

    def load(self) -> None:
        if not self.catalog_path.exists():
            raise FileNotFoundError(f"Catalog file not found: {self.catalog_path}")
        with open(self.catalog_path, "r") as f:
            self.catalog = json.load(f)

    def get_models_nodes(self) -> Dict[str, Model]:
        models = {}
        nodes = self.catalog.get("nodes", {})
        
        for node_id, model_data in nodes.items():
            processed_data = {
                "name": model_data.get("name") or node_id.split(".")[-1],
                "schema": model_data.get("schema") or "main",
                "database": model_data.get("database") or "main",
                "columns": {}
            }
            
            for col_name, col_data in model_data.get("columns", {}).items():
                normalized_col_name = col_name.lower()
                processed_data["columns"][normalized_col_name] = {
                    "name": normalized_col_name,
                    "model_name": processed_data["name"],
                    "description": col_data.get("description"),
                    "data_type": col_data.get("type") or col_data.get("data_type"),
                    "lineage": []
                }
                
            model = Model(**processed_data)
            models[model.name] = model
            
        return models
