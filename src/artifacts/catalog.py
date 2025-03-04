import json
from typing import Dict, Optional, List
from pathlib import Path
from src.models.schema import Column, Model

class CatalogReader:
    def __init__(self, catalog_path: Optional[str] = None):
        self.catalog_path = Path(catalog_path)
        self.catalog: Dict = {}

    def load(self):
        if not self.catalog_path.exists():
            raise FileNotFoundError(f"Catalog file not found: {self.catalog_path}")
        with open(self.catalog_path, "r") as f:
            self.catalog = json.load(f)

    def get_models_nodes(self) -> Dict[str, Model]:
        models = {}
        for model_data in self.catalog.get("nodes", {}).values():
            model = Model(**model_data)
            models[model.name] = model
        return models
