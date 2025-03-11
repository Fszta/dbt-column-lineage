from typing import Dict, Optional
from dbt_column_lineage.artifacts.catalog import CatalogReader
from dbt_column_lineage.artifacts.manifest import ManifestReader
from dbt_column_lineage.models.schema import Model
from dbt_column_lineage.artifacts.exceptions import ModelNotFoundError, RegistryNotLoadedError
from dbt_column_lineage.parser.sql_parser import SQLColumnParser

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
        
        upstream_deps = self.manifest_reader.get_model_upstream()
        for model_name, model in self._models.items():
            model.upstream = upstream_deps.get(model_name, set())
            
        downstream_deps = self.manifest_reader.get_model_downstream()
        for model_name, model in self._models.items():
            model.downstream = downstream_deps.get(model_name, set())

        for model_name, model in self._models.items():
            compiled_sql = self.manifest_reader.get_compiled_sql(model_name)
            if compiled_sql:
                column_lineage = self.sql_parser.parse_column_lineage(compiled_sql)
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
    

    def _find_compiled_sql(self, model_name: str) -> Optional[str]:
        """Find compiled SQL for a model from manifest or target file."""
        self._check_loaded()
        model = self._models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")
        
        # Find in manifest (meaning node has been executed)
        manifest_sql = self.manifest_reader.get_compiled_sql(model_name)
        if manifest_sql:
            model.compiled_sql = manifest_sql
            return manifest_sql
        
        # If not in manifest, try to read from compiled target file
        compiled_path = self.manifest_reader.get_model_path(model_name)
        if compiled_path:
            try:
                with open(compiled_path, 'r') as f:
                    compiled_sql = f.read()
                model.compiled_sql = compiled_sql
                return compiled_sql
            except (FileNotFoundError, IOError):
                pass
                
        return None

    def get_compiled_sql(self, model_name: str) -> str:
        """Get compiled SQL for a model, trying manifest first then target file."""
        self._check_loaded()
        model = self._models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")
            
        if model.compiled_sql:
            return model.compiled_sql
            
        compiled_sql = self._find_compiled_sql(model_name)
        if compiled_sql:
            return compiled_sql
            
        raise ValueError(f"No compiled SQL found for model '{model_name}'")
    
