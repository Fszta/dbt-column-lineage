from src.artifacts.catalog import CatalogReader
from src.artifacts.manifest import ManifestReader
from src.models.schema import Model
from src.registry.exceptions import ModelNotFoundError, RegistryNotLoadedError
from typing import Dict
from src.registry.sql_parser import SQLColumnParser

class ModelRegistry:
    def __init__(self, catalog_path: str, manifest_path: str):
        self.catalog_reader = CatalogReader(catalog_path)
        self.manifest_reader = ManifestReader(manifest_path)
        self._models: Dict[str, Model] = {}
        self.sql_parser = SQLColumnParser()

    def load(self):
        self.catalog_reader.load()
        self.manifest_reader.load()

        self._models = self.catalog_reader.get_models_nodes()
        
        # Add upstream dependencies
        upstream_deps = self.manifest_reader.get_model_upstream()
        for model_name, model in self._models.items():
            model.upstream = upstream_deps.get(model_name, set())
            
        # Add downstream dependencies
        downstream_deps = self.manifest_reader.get_model_downstream()
        for model_name, model in self._models.items():
            model.downstream = downstream_deps.get(model_name, set())

        # Add column lineage
        for model_name, model in self._models.items():
            if model.compiled_sql:
                column_lineage = self.sql_parser.parse_column_lineage(model.compiled_sql)
                for col_name, lineage in column_lineage.items():
                    if col_name in model.columns:
                        model.columns[col_name].lineage = lineage

    def _check_loaded(self):
        """Verify registry is loaded before operations"""
        if not self._models:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")

    def get_models(self) -> Dict[str, Model]:
        self._check_loaded()
        return self._models

    def get_model(self, model_name: str) -> Model:
        self._check_loaded()
        model = self._models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")
        return model

    
    