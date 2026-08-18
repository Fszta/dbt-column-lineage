from typing import Dict, List, Optional
from dataclasses import dataclass, field
import logging

from dbt_column_lineage.artifacts.catalog import CatalogReader
from dbt_column_lineage.artifacts.manifest import ManifestReader
from dbt_column_lineage.models.schema import (
    Model,
    Column,
    SQLParseResult,
    ColumnLineage,
    Exposure,
    Coverage,
)
from dbt_column_lineage.artifacts.exceptions import (
    ModelNotFoundError,
    RegistryNotLoadedError,
    RegistryError,
)
from dbt_column_lineage.parser import SQLColumnParser

logger = logging.getLogger(__name__)


# Resource types that can carry column lineage; coverage is measured against these.
_MODEL_LIKE_RESOURCE_TYPES = frozenset({"model", "snapshot", "seed"})

# Cap on the failed/skipped name lists surfaced in Coverage.
_COVERAGE_NAME_CAP = 25


@dataclass
class ParseStats:
    """Column-lineage parse outcome tallies."""

    parsed_ok: int = 0
    parse_failed: int = 0
    skipped_no_sql: int = 0
    failed_model_names: List[str] = field(default_factory=list)
    skipped_model_names: List[str] = field(default_factory=list)


@dataclass
class RegistryState:
    """Immutable state of the registry."""

    models: Dict[str, Model]
    exposures: Dict[str, Exposure]
    is_loaded: bool = False


class ModelRegistry:
    def __init__(
        self,
        catalog_path: str,
        manifest_path: str,
        adapter_override: Optional[str] = None,
    ):
        self._catalog_reader = CatalogReader(catalog_path)
        self._manifest_reader = ManifestReader(manifest_path)
        self._state = RegistryState(models={}, exposures={}, is_loaded=False)
        self._sql_parser: Optional[SQLColumnParser] = None
        self._dialect: Optional[str] = None
        self._adapter_override: Optional[str] = adapter_override
        self._parse_stats: ParseStats = ParseStats()
        # Names of model-like nodes that have a real catalog entry (data types known).
        # A manifest node absent from this set is "catalog-missing": still analyzable via
        # its compiled SQL, but with unknown column types.
        self._catalog_backed_model_names: set = set()

    @property
    def is_loaded(self) -> bool:
        return self._state.is_loaded

    def _initialize_models(self) -> Dict[str, Model]:
        """Initialize the model universe from the *manifest*, enriched by the catalog.

        The manifest is the source of truth for which models exist: it lists every
        node dbt knows about, regardless of whether the relation has been built in
        the warehouse. The catalog is only an *enrichment* source that supplies real
        column names and data types for relations that were built and profiled.

        Seeding from the catalog alone (the previous behaviour) made every model
        absent from ``catalog.json`` invisible — a common case under a deferred /
        partial CI build (``dbt docs generate --defer`` after building only the
        ``state:modified+`` cone) and for non-table relations such as semantic views.
        Those models were then silently dropped from impact and, in the two-manifest
        diff, misreported as removed. Here we register every manifest model-like node;
        columns for catalog-missing nodes are recovered later from their compiled SQL
        (see :meth:`_apply_column_lineage`).
        """
        try:
            catalog_models = self._catalog_reader.get_models_nodes()
        except Exception as e:
            raise RegistryError(f"Failed to initialize models: {e}")

        models: Dict[str, Model] = {}
        catalog_backed: set = set()

        # 1) Seed the universe from manifest model-like nodes (model/snapshot/seed).
        for node_id, node in self._manifest_reader.manifest.get("nodes", {}).items():
            if node.get("resource_type") not in _MODEL_LIKE_RESOURCE_TYPES:
                continue
            name = (node.get("name") or node_id.split(".")[-1]).lower()
            if name in models:
                continue

            catalog_model = catalog_models.get(name)
            if (
                catalog_model is not None
                and catalog_model.resource_type in _MODEL_LIKE_RESOURCE_TYPES
            ):
                # Built & profiled: reuse the catalog Model (real column types).
                models[name] = catalog_model
                catalog_backed.add(name)
            else:
                # Present in the manifest but absent from the catalog. Register it now;
                # its output columns are derived from compiled SQL during lineage parsing.
                models[name] = Model(
                    name=name,
                    schema=node.get("schema") or "main",
                    database=node.get("database") or "main",
                    columns={},
                    resource_type=node.get("resource_type"),
                    unique_id=node_id,
                    metadata={"catalog_missing": True},
                )

        # 2) Add sources (catalog-only inputs, not manifest ``nodes``) so upstream
        #    references still resolve.
        for name, model in catalog_models.items():
            if model.resource_type == "source" and name not in models:
                models[name] = model

        self._catalog_backed_model_names = catalog_backed

        if not models:
            raise RegistryError("No models found in manifest or catalog")
        return models

    def _apply_dependencies(self, models: Dict[str, Model]) -> None:
        """Apply upstream and downstream dependencies to models."""
        try:
            upstream_deps = self._manifest_reader.get_model_upstream()
            downstream_deps = self._manifest_reader.get_model_downstream()
            model_exposures = self._manifest_reader.get_model_exposures()

            manifest_sources = self._manifest_reader.manifest.get("sources", {})
            for source_id, source_node in manifest_sources.items():
                source_name = source_node.get("source_name")
                source_identifier = (
                    source_node.get("identifier", "").lower()
                    if source_node.get("identifier")
                    else source_node.get("name", "").lower()
                )

                source_model = models.get(source_identifier)
                if source_model and source_model.resource_type == "source" and source_name:
                    source_model.source_name = source_name.lower()

            for model_name, model in models.items():
                model.upstream = upstream_deps.get(model_name, set())
                model.downstream = downstream_deps.get(model_name, set())
                if model_name in model_exposures:
                    model.downstream.update(model_exposures[model_name])
                model.language = self._manifest_reader.get_model_language(model_name)
                model.resource_path = self._manifest_reader.get_model_resource_path(model_name)

                node = self._manifest_reader._find_node(model_name)
                if node:
                    model.description = node.get("description")
                    model.tags = node.get("tags", [])
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
                metadata=exp_data.get("meta", {}),
            )
            exposures[exposure_name] = exposure

        return exposures

    def _process_lineage(self, models: Dict[str, Model]) -> None:
        """Process and apply column lineage to models."""
        logger = logging.getLogger(__name__)

        if self._sql_parser is None:
            raise RegistryError("SQL parser not initialized. Call load() first.")

        successful_parses = 0
        failed_parses = 0
        skipped_models = 0
        failed_model_names = []
        skipped_model_names = []

        # First pass: Process explicit column references
        for model_name, model in models.items():
            if model.language != "sql":
                continue

            sql = self._manifest_reader.get_compiled_sql(model_name)
            if not sql:
                skipped_models += 1
                skipped_model_names.append(model_name)
                continue

            try:
                parse_result = self._sql_parser.parse_column_lineage(sql)
                self._apply_column_lineage(model, parse_result)
                successful_parses += 1
            except Exception as e:
                failed_parses += 1
                failed_model_names.append(model_name)
                logger.warning(
                    f"Failed to process lineage for model {model_name}: {type(e).__name__}: {str(e)}"
                )
                continue

        self._parse_stats = ParseStats(
            parsed_ok=successful_parses,
            parse_failed=failed_parses,
            skipped_no_sql=skipped_models,
            failed_model_names=failed_model_names,
            skipped_model_names=skipped_model_names,
        )

        logger.info(
            f"SQL parsing summary: {successful_parses} successful, "
            f"{failed_parses} failed, {skipped_models} skipped (no SQL)"
        )

        if failed_model_names:
            logger.info(
                f"Failed models ({len(failed_model_names)}): {', '.join(failed_model_names)}"
            )

        # Second pass: Process star references
        try:
            self._process_star_references(models)
        except Exception as e:
            logger.error(f"Failed to process star references: {e}", exc_info=True)

    def _apply_column_lineage(self, model: Model, parse_result: SQLParseResult) -> None:
        """Apply parsed lineage to model columns.

        For a catalog-missing model (present in the manifest but absent from the
        catalog) we have no authoritative column list, so we materialise its output
        columns from the parsed final ``SELECT``. Data types are left ``None`` — the
        column is known and traversable, its type merely unknown. Catalog-backed
        models keep the catalog as the authority and only receive lineage on columns
        that already exist there.
        """
        catalog_missing = bool(model.metadata and model.metadata.get("catalog_missing"))
        for col_name, lineage in parse_result.column_lineage.items():
            if col_name not in model.columns:
                if not catalog_missing:
                    continue
                model.columns[col_name] = Column(
                    name=col_name,
                    model_name=model.name,
                    data_type=None,
                )
            model.columns[col_name].lineage = lineage

        if parse_result.star_sources:
            model.metadata = model.metadata or {}
            model.metadata["star_sources"] = list(parse_result.star_sources)

    def _process_star_references(self, models: Dict[str, Model]) -> None:
        """Process star references between models."""
        for model in models.values():
            if not model.metadata or "star_sources" not in model.metadata:
                continue

            for source_name in model.metadata["star_sources"]:
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
                transformation_type="direct",
            )

            if not any(
                existing.source_columns == star_lineage.source_columns
                for existing in target_col.lineage
            ):
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

        model = self._state.models.get(model_name.lower())
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

    def _count_manifest_models(self) -> int:
        """Count model-like nodes (model/snapshot/seed) declared in the manifest."""
        count = 0
        for node in self._manifest_reader.manifest.get("nodes", {}).values():
            if node.get("resource_type") in _MODEL_LIKE_RESOURCE_TYPES:
                count += 1
        return count

    def get_coverage(self) -> Coverage:
        """Report how completely the loaded artifacts cover the project."""
        if not self.is_loaded:
            raise RegistryNotLoadedError("Registry must be loaded before accessing coverage")

        models_in_manifest = self._count_manifest_models()
        # The universe is now manifest-seeded, so counting every model-like node in the
        # registry would always equal the manifest count. Coverage is about *catalog*
        # completeness, so count only the nodes actually backed by a catalog entry.
        models_in_catalog = len(self._catalog_backed_model_names)
        not_in_catalog_count = max(models_in_manifest - models_in_catalog, 0)

        stats = self._parse_stats
        complete = (
            not_in_catalog_count == 0 and stats.parse_failed == 0 and stats.skipped_no_sql == 0
        )

        return Coverage(
            models_in_manifest=models_in_manifest,
            models_in_catalog=models_in_catalog,
            parsed_ok=stats.parsed_ok,
            parse_failed=stats.parse_failed,
            skipped_no_sql=stats.skipped_no_sql,
            not_in_catalog_count=not_in_catalog_count,
            failed_models=sorted(stats.failed_model_names)[:_COVERAGE_NAME_CAP],
            skipped_models=sorted(stats.skipped_model_names)[:_COVERAGE_NAME_CAP],
            complete=complete,
        )

    def get_unparsed_models(self) -> set:
        """Names of catalog models whose SQL failed to parse or was missing."""
        return set(self._parse_stats.failed_model_names) | set(
            self._parse_stats.skipped_model_names
        )

    def get_parse_failed_models(self) -> set:
        """Names of models whose compiled SQL was present but failed to parse."""
        return set(self._parse_stats.failed_model_names)

    def is_catalog_backed(self, model_name: str) -> bool:
        """Whether a model has a real catalog entry (known column types)."""
        return model_name.lower() in self._catalog_backed_model_names

    def get_manifest_downstream(self) -> Dict[str, set]:
        """Manifest-level downstream child map, covering every model (not just catalog ones)."""
        return self._manifest_reader.get_model_downstream()

    def _check_loaded(self) -> None:
        """Verify registry is loaded before operations"""
        if not self._state.models:
            raise RegistryNotLoadedError("Registry must be loaded before accessing models")

    def _find_compiled_sql(self, model_name: str) -> Optional[str]:
        """Find compiled SQL for a model from manifest or target file."""
        self._check_loaded()
        model_name_lower = model_name.lower()
        model = self._state.models.get(model_name_lower)
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
                with open(compiled_path, "r") as f:
                    compiled_sql = f.read()
                model.compiled_sql = compiled_sql
                return compiled_sql
            except (FileNotFoundError, IOError):
                pass

        return None

    def get_compiled_sql(self, model_name: str) -> str:
        """Get compiled SQL for a model, trying manifest first then target file."""
        self._check_loaded()
        model_name_lower = model_name.lower()
        model = self._state.models.get(model_name_lower)
        if model is None:
            raise ModelNotFoundError(f"Model '{model_name}' not found in registry")

        if model.compiled_sql:
            return model.compiled_sql

        compiled_sql = self._find_compiled_sql(model_name)
        if compiled_sql:
            return compiled_sql

        raise ValueError(f"No compiled SQL found for model '{model_name}'")
