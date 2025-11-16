from typing import Dict, Optional
from dataclasses import dataclass
import logging

from dbt_column_lineage.artifacts.catalog import CatalogReader
from dbt_column_lineage.artifacts.manifest import ManifestReader
from dbt_column_lineage.models.schema import Model, SQLParseResult, ColumnLineage, Exposure
from dbt_column_lineage.artifacts.exceptions import (
    ModelNotFoundError,
    RegistryNotLoadedError,
    RegistryError
)
from dbt_column_lineage.artifacts.sql_parser import SQLColumnParser

logger = logging.getLogger(__name__) 


@dataclass
class RegistryState:
    """Immutable state of the registry."""
    models: Dict[str, Model]
    exposures: Dict[str, Exposure]
    is_loaded: bool = False

class ModelRegistry:
    def __init__(self, catalog_path: str, manifest_path: str, adapter_override: Optional[str] = None):
        self._catalog_reader = CatalogReader(catalog_path)
        self._manifest_reader = ManifestReader(manifest_path)
        self._state = RegistryState(models={}, exposures={}, is_loaded=False)
        self._sql_parser : Optional[SQLColumnParser] = None
        self._dialect : Optional[str] = None
        self._adapter_override: Optional[str] = adapter_override

    @property
    def is_loaded(self) -> bool:
        return self._state.is_loaded

    def _initialize_models(self) -> Dict[str, Model]:
        """Initialize base model information from catalog."""
        try:
            models = self._catalog_reader.get_models_nodes()
            if not models:
                raise RegistryError("No models found in catalog")
            return models
        except Exception as e:
            raise RegistryError(f"Failed to initialize models: {e}")

    def _apply_dependencies(self, models: Dict[str, Model]) -> None:
        """Apply upstream and downstream dependencies to models."""
        try:
            upstream_deps = self._manifest_reader.get_model_upstream()
            downstream_deps = self._manifest_reader.get_model_downstream()
            model_exposures = self._manifest_reader.get_model_exposures()

            for model_name, model in models.items():
                model.upstream = upstream_deps.get(model_name, set())
                model.downstream = downstream_deps.get(model_name, set())
                if model_name in model_exposures:
                    model.downstream.update(model_exposures[model_name])
                model.language = self._manifest_reader.get_model_language(model_name)
                model.resource_path = self._manifest_reader.get_model_resource_path(model_name)
        except Exception as e:
            raise RegistryError(f"Failed to apply dependencies: {e}")

    def _load_exposures(self) -> Dict[str, Exposure]:
        """Load exposures from manifest."""
        exposures = {}
        exposure_data = self._manifest_reader.get_exposures()
        exposure_deps = self._manifest_reader.get_exposure_dependencies()
        
        for exposure_id, exp_data in exposure_data.items():
            exposure_name = exp_data.get("name")
            if not exposure_name:
                continue
            
            depends_on_models = exposure_deps.get(exposure_name, set())
            
            exposure = Exposure(
                name=exposure_name,
                type=exp_data.get("type", "dashboard"),
                url=exp_data.get("url"),
                description=exp_data.get("description"),
                owner=exp_data.get("owner"),
                unique_id=exposure_id,
                depends_on_models=depends_on_models,
                resource_path=exp_data.get("original_file_path"),
                metadata=exp_data.get("meta", {})
            )
            exposures[exposure_name] = exposure
        
        return exposures

    def _process_lineage(self, models: Dict[str, Model]) -> None:
        """Process and apply column lineage to models."""
        logger = logging.getLogger(__name__)
        
        # First pass: Process explicit column references
        for model_name, model in models.items():
            if model.language != "sql":
                continue

            sql = self._manifest_reader.get_compiled_sql(model_name)
            if not sql:
                continue

            try:
                parse_result = self._sql_parser.parse_column_lineage(sql)
                self._apply_column_lineage(model, parse_result)
            except Exception as e:
                logger.warning(
                    f"Failed to process lineage for model {model_name}, skipping..."
                )
                continue

        # Second pass: Process star references
        try:
            self._process_star_references(models)
        except Exception as e:
            logger.error(f"Failed to process star references: {e}", exc_info=True)

    def _apply_column_lineage(self, model: Model, parse_result: SQLParseResult) -> None:
        """Apply parsed lineage to model columns."""
        for col_name, lineage in parse_result.column_lineage.items():
            if col_name in model.columns:
                model.columns[col_name].lineage = lineage

        if parse_result.star_sources:
            model.metadata = model.metadata or {}
            model.metadata['star_sources'] = list(parse_result.star_sources)

    def _process_star_references(self, models: Dict[str, Model]) -> None:
        """Process star references between models."""
        for model in models.values():
            if not model.metadata or 'star_sources' not in model.metadata:
                continue

            for source_name in model.metadata['star_sources']:
                if source_name not in models:
                    continue

                self._apply_star_columns(model, source_name, models[source_name])

    def _apply_star_columns(self, target: Model, source_name: str, source: Model) -> None:
        """Apply star columns from source to target model."""
        for col_name, source_col in source.columns.items():
            if col_name not in target.columns:
                continue

            target_col = target.columns[col_name]
            if not target_col.lineage:
                target_col.lineage = []

            star_lineage = ColumnLineage(
                source_columns={f"{source_name}.{col_name}"},
                transformation_type="direct"
            )

            if not any(existing.source_columns == star_lineage.source_columns 
                      for existing in target_col.lineage):
                target_col.lineage.append(star_lineage)

    def load(self) -> None:
        """Load and initialize the registry."""
        if self.is_loaded:
            raise RegistryError("Registry has already been loaded")

        try:
            self._catalog_reader.load()
            self._manifest_reader.load()
            
            # Ensure the dialect is set before initializing the parser
            self._dialect = self._adapter_override or self._manifest_reader.get_adapter()
            
            if self._adapter_override:
                logger.info(f"Using adapter override from CLI: {self._adapter_override}")
            elif self._dialect:
                logger.info(f"Detected dialect: {self._dialect}")
            else:
                logger.warning("No dialect detected, the sql parser will be less accurate")

            self._sql_parser = SQLColumnParser(dialect=self._dialect)
            
            models = self._initialize_models()
            self._apply_dependencies(models)
            self._process_lineage(models)
            exposures = self._load_exposures()
            self._state = RegistryState(models=models, exposures=exposures, is_loaded=True)
        except Exception as e:
            raise RegistryError(f"Failed to load registry: {e}")

    def get_models(self) -> Dict[str, Model]:
        """Get all models in the registry."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")
        return self._state.models

    def get_model(self, model_name: str) -> Model:
        """Get a specific model by name."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")
        
        model = self._state.models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found")
        return model

    def get_exposures(self) -> Dict[str, Exposure]:
        """Get all exposures in the registry."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing exposures")
        return self._state.exposures

    def get_exposure(self, exposure_name: str) -> Exposure:
        """Get a specific exposure by name."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing exposures")
        
        exposure = self._state.exposures.get(exposure_name)
        if exposure is None:
            raise ValueError(f"Exposure '{exposure_name}' not found")
        return exposure

    def _check_loaded(self) -> None:
        """Verify registry is loaded before operations"""
        if not self._state.models:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")

    def _find_compiled_sql(self, model_name: str) -> Optional[str]:
        """Find compiled SQL for a model from manifest or target file."""
        self._check_loaded()
        model = self._state.models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")
        
        # Find in manifest (meaning node has been executed)
        manifest_sql = self._manifest_reader.get_compiled_sql(model_name)
        if manifest_sql:
            model.compiled_sql = manifest_sql
            return manifest_sql
        
        # If not in manifest, try to read from compiled target file
        compiled_path = self._manifest_reader.get_model_path(model_name)
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
        model = self._state.models.get(model_name)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")
            
        if model.compiled_sql:
            return model.compiled_sql
            
        compiled_sql = self._find_compiled_sql(model_name)
        if compiled_sql:
            return compiled_sql
            
        raise ValueError(f"No compiled SQL found for model '{model_name}'")
    
