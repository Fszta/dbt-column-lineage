from pathlib import Path
from typing import Dict, Set, Optional, Any, Union
from dataclasses import dataclass, field
import logging

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.models.schema import ColumnLineage
from dbt_column_lineage.parser.sql_parser_utils import strip_sql_comments

logger = logging.getLogger(__name__)


@dataclass
class LineageSelector:
    model: str
    column: Optional[str]
    upstream: bool
    downstream: bool

    @classmethod
    def from_string(cls, selector: str) -> "LineageSelector":
        if not selector:
            raise ValueError("Selector cannot be empty")

        upstream = selector.startswith("+")
        downstream = selector.endswith("+")

        if not upstream and not downstream:
            upstream = downstream = True

        clean_selector = selector.strip("+")
        model_name, column_name = (
            clean_selector.split(".", 1) if "." in clean_selector else (clean_selector, None)
        )

        return cls(
            model=model_name,
            column=column_name,
            upstream=upstream,
            downstream=downstream,
        )


@dataclass
class LineageReferences:
    """Structured lineage references separating model mappings from special sets."""

    models: Dict[str, Dict[str, ColumnLineage]] = field(default_factory=dict)
    exposures: Set[str] = field(default_factory=set)
    sources: Set[str] = field(default_factory=set)
    direct_refs: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Convert to legacy dict format for backward compatibility."""
        result: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]] = {}
        result.update(self.models)
        if self.exposures:
            result["exposures"] = self.exposures
        if self.sources:
            result["sources"] = self.sources
        if self.direct_refs:
            result["direct_refs"] = self.direct_refs
        return result

    @classmethod
    def from_dict(
        cls, data: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]
    ) -> "LineageReferences":
        """Create from legacy dict format."""
        refs = cls()
        for key, value in data.items():
            if key == "exposures" and isinstance(value, set):
                refs.exposures = value
            elif key == "sources" and isinstance(value, set):
                refs.sources = value
            elif key == "direct_refs" and isinstance(value, set):
                refs.direct_refs = value
            elif isinstance(value, dict):
                refs.models[key] = value
        return refs


class LineageService:
    """Service for handling lineage operations."""

    def __init__(self, catalog_path: Path, manifest_path: Path, adapter: Optional[str] = None):
        self.registry = ModelRegistry(
            str(catalog_path), str(manifest_path), adapter_override=adapter
        )
        self.registry.load()

    def get_model_info(self, selector: LineageSelector) -> Dict[str, Any]:
        """Get model information based on selector."""
        model = self.registry.get_model(selector.model)
        return {
            "name": model.name,
            "schema": model.schema_name,
            "database": model.database,
            "columns": list(model.columns.keys()),
            "upstream": list(model.upstream) if selector.upstream else [],
            "downstream": list(model.downstream) if selector.downstream else [],
        }

    def get_column_info(self, selector: LineageSelector) -> Dict[str, Any]:
        """Get column information and lineage based on selector."""
        model = self.registry.get_model(selector.model)
        if not selector.column or selector.column not in model.columns:
            raise ValueError(f"Column '{selector.column}' not found in model '{selector.model}'")

        column = model.columns[selector.column]
        return {
            "name": column.name,
            "data_type": column.data_type,
            "description": column.description,
            "upstream": (
                self._get_upstream_lineage(selector.model, selector.column)
                if selector.upstream
                else {}
            ),
            "downstream": (
                self._get_downstream_lineage(selector.model, selector.column)
                if selector.downstream
                else {}
            ),
        }

    def _split_qualified_name(self, qualified_name: str) -> Optional[tuple[str, str]]:
        """Split a fully qualified name into model and column parts. Returns None if invalid."""
        if "." not in qualified_name:
            return None
        qualified_name = strip_sql_comments(qualified_name)
        parts = qualified_name.split(".")
        if len(parts) < 2:
            return None
        model_part = ".".join(parts[:-1])
        column_part = strip_sql_comments(parts[-1]).lower()
        return (model_part, column_part)

    def _process_source_reference(
        self,
        source: str,
        upstream_refs: LineageReferences,
    ) -> None:
        """Process a source reference and add it to upstream_refs."""
        upstream_refs.sources.add(source)

    def _merge_upstream_refs(
        self,
        target: LineageReferences,
        source_dict: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]],
    ) -> None:
        """Merge source refs dict into target LineageReferences."""
        for key, value in source_dict.items():
            if key == "sources" and isinstance(value, set):
                target.sources.update(value)
            elif key == "direct_refs" and isinstance(value, set):
                target.direct_refs.update(value)
            elif key == "exposures" and isinstance(value, set):
                target.exposures.update(value)
            elif isinstance(value, dict):
                if key not in target.models:
                    target.models[key] = {}
                target.models[key].update(value)

    def _process_model_reference(
        self,
        src_model: str,
        src_column: str,
        lineage: ColumnLineage,
        upstream_refs: LineageReferences,
        visited: Set[str],
    ) -> None:
        """Process a model reference and add it to upstream_refs."""
        try:
            model_obj = self.registry.get_model(src_model)

            if src_model not in upstream_refs.models:
                upstream_refs.models[src_model] = {}

            col_obj = model_obj.columns.get(src_column)
            if col_obj and col_obj.lineage:
                upstream_refs.models[src_model][src_column] = col_obj.lineage[0]
            else:
                upstream_refs.models[src_model][src_column] = lineage

            recursive_refs = self._get_upstream_lineage(src_model, src_column, visited)
            self._merge_upstream_refs(upstream_refs, recursive_refs)

        except Exception:
            self._process_source_reference(f"{src_model}.{src_column}", upstream_refs)

    def _get_upstream_lineage(
        self, model_name: str, column_name: str, visited: Optional[Set[str]] = None
    ) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Recursively get all upstream column references."""
        if visited is None:
            visited = set()

        column_name = strip_sql_comments(column_name).lower()
        current_ref = f"{model_name}.{column_name}"
        if current_ref in visited:
            return {}

        visited.add(current_ref)
        upstream_refs = LineageReferences()
        current_model = self.registry.get_model(model_name)

        try:
            if column_name not in current_model.columns:
                return upstream_refs.to_dict()

            column = current_model.columns[column_name]
            if not column.lineage:
                return upstream_refs.to_dict()

            sorted_lineage = sorted(
                column.lineage,
                key=lambda lineage: (
                    lineage.transformation_type,
                    sorted(lineage.source_columns)[0] if lineage.source_columns else "",
                ),
            )
            for lineage in sorted_lineage:
                for source in sorted(lineage.source_columns):
                    if "." not in source:
                        upstream_refs.direct_refs.add(source)
                        continue

                    split_result = self._split_qualified_name(strip_sql_comments(source))
                    if split_result is None:
                        continue
                    src_model, src_column = split_result
                    if src_model in current_model.upstream:
                        self._process_model_reference(
                            src_model, src_column, lineage, upstream_refs, visited
                        )

        except Exception as e:
            logger.warning(f"Failed to process lineage for {current_ref}: {str(e)}")

        return upstream_refs.to_dict()

    def _get_immediate_downstream_lineage(
        self, model_name: str, column_name: str
    ) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Get only immediate (non-recursive) downstream column references."""
        column_name = strip_sql_comments(column_name).lower()
        current_ref = f"{model_name}.{column_name}"
        downstream_refs = LineageReferences()
        current_model = self.registry.get_model(model_name)

        try:
            if column_name not in current_model.columns:
                return downstream_refs.to_dict()

            column_used_downstream = False
            downstream_models_using_column = []

            for other_name in sorted(current_model.downstream):
                try:
                    self.registry.get_exposure(other_name)
                    continue
                except (ValueError, KeyError):
                    pass

                # Some models in manifest dependencies may not be in catalog
                if other_name not in self.registry.get_models():
                    continue

                try:
                    other_model = self.registry.get_model(other_name)
                    for col_name, col in sorted(other_model.columns.items()):
                        if not col.lineage:
                            continue

                        for lineage in col.lineage:
                            if any(
                                src.lower() == current_ref.lower() for src in lineage.source_columns
                            ):
                                column_used_downstream = True
                                if other_name not in downstream_models_using_column:
                                    downstream_models_using_column.append(other_name)

                                if other_name not in downstream_refs.models:
                                    downstream_refs.models[other_name] = {}
                                downstream_refs.models[other_name][col_name] = lineage

                except Exception as e:
                    logger.warning(f"Failed to process downstream model {other_name}: {str(e)}")

            if column_used_downstream and downstream_models_using_column:
                models_using_column = set(downstream_models_using_column)
                models_using_column.add(model_name)
            else:
                models_using_column = {model_name}

            for other_name in sorted(current_model.downstream):
                try:
                    exposure = self.registry.get_exposure(other_name)
                    if any(
                        model in models_using_column for model in sorted(exposure.depends_on_models)
                    ):
                        downstream_refs.exposures.add(other_name)
                except (ValueError, KeyError):
                    pass

        except Exception as e:
            logger.warning(
                f"Failed to process immediate downstream lineage for {current_ref}: {str(e)}"
            )

        return downstream_refs.to_dict()

    def _get_downstream_lineage(
        self, model_name: str, column_name: str, visited: Optional[Set[str]] = None
    ) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Get downstream column references following the model DAG, including exposures.

        Uses breadth-first traversal without shared mutable state to ensure determinism.
        """
        column_name = strip_sql_comments(column_name).lower()
        start_ref = f"{model_name}.{column_name}"

        queue = [(model_name, column_name)]
        visited_set = visited.copy() if visited else set()
        visited_set.add(start_ref)

        downstream_refs = LineageReferences()
        models_using_column = {model_name}
        all_models_using_column = {model_name}

        while queue:
            current_level = []
            while queue:
                current_level.append(queue.pop(0))

            current_level.sort()

            next_level_nodes = []

            for current_model, current_col in current_level:
                current_ref = f"{current_model}.{current_col}"

                try:
                    current_model_obj = self.registry.get_model(current_model)
                    if current_col not in current_model_obj.columns:
                        continue

                    column_used_downstream = False
                    level_downstream_models = []

                    for other_name in sorted(current_model_obj.downstream):
                        try:
                            self.registry.get_exposure(other_name)
                            continue
                        except (ValueError, KeyError):
                            pass

                        # Some models in manifest dependencies may not be in catalog
                        if other_name not in self.registry.get_models():
                            continue

                        try:
                            other_model = self.registry.get_model(other_name)
                            for col_name, col in sorted(other_model.columns.items()):
                                if not col.lineage:
                                    continue

                                sorted_lineage = sorted(
                                    col.lineage,
                                    key=lambda lineage: (
                                        lineage.transformation_type,
                                        (
                                            sorted(lineage.source_columns)[0]
                                            if lineage.source_columns
                                            else ""
                                        ),
                                    ),
                                )
                                for lineage in sorted_lineage:
                                    if any(
                                        src.lower() == current_ref.lower()
                                        for src in sorted(lineage.source_columns)
                                    ):
                                        column_used_downstream = True
                                        if other_name not in level_downstream_models:
                                            level_downstream_models.append(other_name)

                                        if other_name not in downstream_refs.models:
                                            downstream_refs.models[other_name] = {}
                                        downstream_refs.models[other_name][col_name] = lineage

                                        # Collect for next level if not already visited
                                        next_ref = f"{other_name}.{col_name}"
                                        if next_ref not in visited_set:
                                            visited_set.add(next_ref)
                                            next_level_nodes.append((other_name, col_name))

                        except Exception as e:
                            logger.warning(
                                f"Failed to process downstream model {other_name}: {str(e)}"
                            )

                    if column_used_downstream:
                        models_using_column.update(level_downstream_models)
                        all_models_using_column.update(level_downstream_models)
                        all_models_using_column.add(current_model)
                        models_using_column = set(sorted(models_using_column))

                except Exception as e:
                    logger.warning(
                        f"Failed to process downstream lineage for {current_ref}: {str(e)}"
                    )

            next_level_nodes.sort()
            queue.extend(next_level_nodes)

        processed_exposures = set()
        for model_using_col in sorted(all_models_using_column):
            try:
                model_obj = self.registry.get_model(model_using_col)
                for other_name in sorted(model_obj.downstream):
                    if other_name in processed_exposures:
                        continue
                    try:
                        exposure = self.registry.get_exposure(other_name)
                        if any(
                            model in all_models_using_column
                            for model in sorted(exposure.depends_on_models)
                        ):
                            downstream_refs.exposures.add(other_name)
                            processed_exposures.add(other_name)
                    except (ValueError, KeyError):
                        pass
            except Exception:
                pass

        return downstream_refs.to_dict()

    def get_column_impact(self, model_name: str, column_name: str) -> Dict[str, Any]:
        """Get impact analysis for a column - what would break if this column is modified.

        Returns:
            Dict with:
            - summary: metrics (affected_models, affected_columns, affected_exposures, critical_count, potential_count)
            - affected_models: list of affected models with resource_type
            - affected_columns: list of affected columns with details
            - affected_exposures: list of affected exposures
        """
        try:
            model = self.registry.get_model(model_name)
            if column_name not in model.columns:
                raise ValueError(f"Column '{column_name}' not found in model '{model_name}'")

            downstream_refs = self._get_downstream_lineage(model_name, column_name)

            affected_models = {}
            affected_columns = []
            affected_exposures = []
            critical_count = 0
            potential_count = 0

            for downstream_model_name, columns in sorted(downstream_refs.items()):
                if downstream_model_name in (
                    "exposures",
                    "sources",
                    "direct_refs",
                ) or not isinstance(columns, dict):
                    continue

                try:
                    downstream_model = self.registry.get_model(downstream_model_name)

                    if downstream_model_name not in affected_models:
                        affected_models[downstream_model_name] = {
                            "name": downstream_model_name,
                            "resource_type": getattr(downstream_model, "resource_type", "model"),
                            "schema": downstream_model.schema_name,
                            "database": downstream_model.database,
                        }

                    for col_name, lineage in columns.items():
                        if not isinstance(lineage, ColumnLineage):
                            logger.warning(
                                f"Expected ColumnLineage for {downstream_model_name}.{col_name}, got {type(lineage)}"
                            )
                            continue

                        # Determine severity based on transformation type
                        # Critical = derived/transformed columns (transformation logic might break)
                        # Low impact = direct/renamed (just pass-through, change propagates)
                        is_critical = lineage.transformation_type == "derived"
                        if is_critical:
                            critical_count += 1
                        else:
                            potential_count += 1

                        col_obj = downstream_model.columns.get(col_name)
                        affected_columns.append(
                            {
                                "model": downstream_model_name,
                                "column": col_name,
                                "transformation_type": lineage.transformation_type,
                                "sql_expression": lineage.sql_expression,
                                "severity": "critical" if is_critical else "low_impact",
                                "data_type": col_obj.data_type if col_obj else None,
                            }
                        )

                except Exception as e:
                    logger.warning(
                        f"Failed to process model {downstream_model_name} in impact analysis: {e}",
                        exc_info=True,
                    )

            if "exposures" in downstream_refs and isinstance(downstream_refs["exposures"], set):
                for exposure_name in sorted(downstream_refs["exposures"]):
                    try:
                        exposure = self.registry.get_exposure(exposure_name)
                        affected_exposures.append(
                            {
                                "name": exposure.name,
                                "type": exposure.type,
                                "url": exposure.url,
                                "description": exposure.description,
                                "depends_on_models": list(exposure.depends_on_models),
                            }
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to process exposure {exposure_name} in impact analysis: {e}"
                        )

            return {
                "summary": {
                    "affected_models": len(affected_models),
                    "affected_columns": len(affected_columns),
                    "affected_exposures": len(affected_exposures),
                    "critical_count": critical_count,
                    "low_impact_count": potential_count,
                },
                "affected_models": list(affected_models.values()),
                "affected_columns": affected_columns,
                "affected_exposures": affected_exposures,
            }

        except Exception as e:
            logger.error(f"Error in impact analysis for {model_name}.{column_name}: {e}")
            raise
