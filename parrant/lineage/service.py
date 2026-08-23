from pathlib import Path
from typing import Dict, List, Literal, Set, Optional, Any, Tuple, Union, TYPE_CHECKING
from dataclasses import dataclass, field
import logging

from parrant.lineage.provider import LineageAndMetadataProvider
from parrant.lineage.sqlglot_provider import build_sqlglot_provider

if TYPE_CHECKING:
    from parrant.lineage.changeset import ColumnChange
    from parrant.metabase.reach import MetabaseReach
from parrant.models.schema import ColumnLineage, Coverage, ImpactConfidence
from parrant.parser.sql_parser_utils import strip_sql_comments

logger = logging.getLogger(__name__)

# Higher rank == more severe. Used to keep the worst severity when the same
# downstream node is reached by several changed columns.
_SEVERITY_RANK: Dict[str, int] = {"critical": 2, "low_impact": 1}

# Cap on the number of unanalyzable model names carried in the confidence block, so a
# huge coverage gap doesn't bloat the impact payload. Totals stay in the integer counts.
_IMPACT_CONFIDENCE_NAME_CAP = 100

# A downstream column's ``transformation_type`` → the plain-language *mechanism* by which
# the change reaches it. This is the machine-readable twin of the markdown's mechanism
# split (derived recompute / row-set filter / pass-through): it lets an agent or the
# Impact Report envelope reason over *how* impact propagates, not just how many nodes.
_MECHANISM_LABELS: Dict[str, str] = {
    "derived": "derived_recompute",
    "filter": "rowset_filter",
    "renamed": "renamed_passthrough",
    "direct": "direct_passthrough",
}


def _mechanism_label(transformation_type: Optional[str]) -> str:
    """Map a downstream column's ``transformation_type`` to its reach *mechanism* label.

    The single source of the recompute/filter/pass-through taxonomy predicates match on.
    An unrecognized (or missing) type is bucketed under its raw value / ``"unknown"`` so
    nothing is silently dropped.
    """
    raw = transformation_type or "unknown"
    return _MECHANISM_LABELS.get(raw, raw)


def _reached_from_impact(
    impact: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Re-shape a single change's impact into reached NAMES + mechanism (no new traversal).

    ``get_column_impact`` already computes the reached models/columns/exposures; it only
    reports *counts* per change. This lifts the identifiers back out so a downstream reach
    predicate can attribute what a specific change tripped:

      * ``reached_columns``   — one entry per affected downstream column, with mechanism.
      * ``reached_models``    — one entry per (model, mechanism) the change reaches; a model
                                reached by several mechanisms yields several entries so a
                                mechanism filter resolves cleanly. Models with no attributed
                                column carry ``mechanism = None``.
      * ``reached_exposures`` — one entry per reached exposure (name only).

    Pure re-shape of data already in ``impact``; deterministic ordering for stable reports.
    """
    reached_columns: List[Dict[str, Any]] = [
        {
            "model": column["model"],
            "column": column["column"],
            "mechanism": _mechanism_label(column.get("transformation_type")),
        }
        for column in impact.get("affected_columns", [])
    ]

    model_mechanisms: Dict[str, Set[str]] = {}
    for column in impact.get("affected_columns", []):
        model_mechanisms.setdefault(column["model"], set()).add(
            _mechanism_label(column.get("transformation_type"))
        )

    reached_models: List[Dict[str, Any]] = []
    for model in impact.get("affected_models", []):
        mechanisms = sorted(model_mechanisms.get(model["name"], set()))
        if mechanisms:
            reached_models.extend(
                {"name": model["name"], "mechanism": mechanism} for mechanism in mechanisms
            )
        else:
            reached_models.append({"name": model["name"], "mechanism": None})

    reached_exposures: List[Dict[str, Any]] = [
        {"name": exposure["name"]} for exposure in impact.get("affected_exposures", [])
    ]

    return reached_models, reached_exposures, reached_columns


def _mechanism_breakdown(affected_columns: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count affected downstream columns by the mechanism that propagates the change.

    Pure aggregation over the ``transformation_type`` each affected column already
    carries — no new traversal. An unrecognized type is bucketed under its raw value so
    nothing is silently dropped.
    """
    breakdown: Dict[str, int] = {}
    for column in affected_columns:
        raw = column.get("transformation_type") or "unknown"
        label = _MECHANISM_LABELS.get(raw, raw)
        breakdown[label] = breakdown.get(label, 0) + 1
    return breakdown


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
        # Depend on the LineageProvider seam, not the concrete registry: the factory builds
        # and loads the SQLGlot-backed provider today, and is the single place a future
        # Fusion/warehouse backend would swap in.
        self.registry: LineageAndMetadataProvider = build_sqlglot_provider(
            str(catalog_path), str(manifest_path), adapter_override=adapter
        )
        self._coverage: Coverage = self.registry.get_coverage()

    def get_coverage(self) -> Coverage:
        """Return coverage for the loaded artifacts."""
        return self._coverage

    def _dag_reachable_models(self, model_name: str) -> Set[str]:
        """Transitive downstream models of model_name in the manifest DAG."""
        downstream_map = self.registry.get_manifest_downstream()
        start = model_name.lower()
        reachable: Set[str] = set()
        queue = [start]
        while queue:
            current = queue.pop()
            for child in downstream_map.get(current, set()):
                if child != start and child not in reachable:
                    reachable.add(child)
                    queue.append(child)
        return reachable

    def _impact_confidence(self, reachable: Set[str], resolved_models: int) -> Dict[str, Any]:
        """Confidence block: "full" when every reachable model was analyzable, else "partial".

        The honest signal is the *coverage gap* — reachable downstream models we could
        not analyze at the column level. A model is analyzable when we have its output
        columns (from the catalog OR recovered from its compiled SQL); most reachable
        models are analyzable and simply don't reference the changed column, so the
        resolved-vs-reachable ratio is not the signal.

        A reachable model is *unanalyzable* only when we have no columns to inspect,
        split by reason:
        - ``parse_failed``: it had compiled SQL but the parser could not read it;
        - ``no_column_info``: neither a catalog entry nor parseable compiled SQL — e.g.
          a non-table relation such as a semantic view, a python model, or a relation
          dbt has not built/compiled. We deliberately do NOT claim these are "not built".
        """
        models = self.registry.get_models()
        parse_failed_names = self.registry.get_parse_failed_models()

        parse_failed: Set[str] = set()
        no_column_info: Set[str] = set()
        for name in reachable:
            model = models.get(name)
            if model is not None and model.columns:
                continue  # analyzable: we have columns to trace
            if name in parse_failed_names:
                parse_failed.add(name)
            else:
                no_column_info.add(name)

        unanalyzable_reachable = parse_failed | no_column_info
        level: Literal["full", "partial"] = "full" if not unanalyzable_reachable else "partial"
        cap = _IMPACT_CONFIDENCE_NAME_CAP
        return ImpactConfidence(
            reachable_models=len(reachable),
            resolved_models=resolved_models,
            unanalyzable_models=len(unanalyzable_reachable),
            no_column_info=len(no_column_info),
            parse_failed=len(parse_failed),
            no_column_info_models=sorted(no_column_info)[:cap],
            parse_failed_models=sorted(parse_failed)[:cap],
            level=level,
        ).model_dump()

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

                # Safety net: the universe is manifest-seeded, so every manifest node
                # (built or not, catalogued or not) resolves here; this only skips names
                # that are genuinely unknown to the registry.
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

                        # Safety net: the universe is manifest-seeded, so every manifest node
                        # (built or not, catalogued or not) resolves here; this only skips names
                        # that are genuinely unknown to the registry.
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
                            "description": downstream_model.description,
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
                                "description": col_obj.description if col_obj else None,
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
                                "owner": exposure.owner,
                                "depends_on_models": list(exposure.depends_on_models),
                            }
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to process exposure {exposure_name} in impact analysis: {e}"
                        )

            # Row-set (filter/join) dependents: models that reference a column ONLY in a
            # predicate (WHERE / JOIN ON / HAVING / QUALIFY, incl. a window ORDER BY). They
            # never project the value, so column-value lineage misses them — but changing the
            # column shifts which rows they keep (and which row a QUALIFY picks), and therefore
            # their aggregates. Surface them as a distinct 'filter' severity.
            #
            # Crucially this is checked for EVERY value-carrying column in the lineage, not just
            # the queried one: the change flows by value to each downstream column, and a model
            # that filters on any of those has its row-set shifted too. (Without this, a source
            # column renamed through staging and then used only in a downstream QUALIFY ... ORDER
            # BY — e.g. "pick the first account by created_at" — is silently dropped.)
            filter_count = 0
            value_affected = set(affected_models.keys())
            value_columns = {(model_name, column_name)} | {
                (c["model"], c["column"])
                for c in affected_columns
                if c.get("transformation_type") != "filter"
            }
            added_filter_models: set = set()
            for src_model, src_col in sorted(value_columns):
                src_key = f"{src_model}.{src_col}"
                for fm_name in sorted(self.registry.get_filter_dependents(src_key)):
                    if fm_name in value_affected or fm_name in added_filter_models:
                        continue
                    try:
                        fm = self.registry.get_model(fm_name)
                    except Exception:
                        continue
                    added_filter_models.add(fm_name)
                    affected_models.setdefault(
                        fm_name,
                        {
                            "name": fm_name,
                            "resource_type": getattr(fm, "resource_type", "model"),
                            "schema": fm.schema_name,
                            "database": fm.database,
                            "description": fm.description,
                        },
                    )
                    affected_columns.append(
                        {
                            "model": fm_name,
                            "column": "(row-set)",
                            "transformation_type": "filter",
                            # The predicate condition the (value-reached) column appears in.
                            "sql_expression": (fm.predicate_lineage or {}).get(src_key),
                            "severity": "filter",
                            "data_type": None,
                            "description": None,
                        }
                    )
                    filter_count += 1
                    for other_name in sorted(fm.downstream):
                        try:
                            exposure = self.registry.get_exposure(other_name)
                        except (ValueError, KeyError):
                            continue
                        if not any(e["name"] == exposure.name for e in affected_exposures):
                            affected_exposures.append(
                                {
                                    "name": exposure.name,
                                    "type": exposure.type,
                                    "url": exposure.url,
                                    "description": exposure.description,
                                    "owner": exposure.owner,
                                    "depends_on_models": list(exposure.depends_on_models),
                                }
                            )

            reachable = self._dag_reachable_models(model_name)
            confidence = self._impact_confidence(reachable, len(affected_models))

            return {
                "summary": {
                    "affected_models": len(affected_models),
                    "affected_columns": len(affected_columns),
                    "affected_exposures": len(affected_exposures),
                    "critical_count": critical_count,
                    "low_impact_count": potential_count,
                    "filter_count": filter_count,
                    "by_mechanism": _mechanism_breakdown(affected_columns),
                },
                "affected_models": list(affected_models.values()),
                "affected_columns": affected_columns,
                "affected_exposures": affected_exposures,
                "confidence": confidence,
            }

        except Exception as e:
            logger.error(f"Error in impact analysis for {model_name}.{column_name}: {e}")
            raise

    @staticmethod
    def _lookup_column_description(
        registry: Any, model_name: str, column_name: str
    ) -> Optional[str]:
        """The dbt-authored description of a column, or None if it can't be resolved.

        Guarded so a stub service without a real registry simply yields no description
        rather than erroring (mirrors the ``getattr(self, "registry", None)`` guard used
        for the confidence block).
        """
        if registry is None:
            return None
        try:
            model = registry.get_model(model_name)
        except Exception:
            return None
        column = model.columns.get(column_name)
        return column.description if column else None

    def get_changeset_impact(
        self,
        changes: List["ColumnChange"],
        base_service: Optional["LineageService"] = None,
        *,
        metabase: Optional["MetabaseReach"] = None,
    ) -> Dict[str, Any]:
        """Aggregate single-column impact across a changeset into one blast radius.

        ``self`` is the *head* service. Each change is fanned through
        :meth:`get_column_impact` and downstream nodes are deduplicated by
        ``(model, column)``, keeping the highest severity per node.

        Removed columns no longer exist in head, so their impact is computed
        against ``base_service`` (where the column and its downstream consumers
        still exist). If no base service is supplied, such changes are reported as
        unresolved rather than silently dropped.

        When a :class:`~parrant.metabase.reach.MetabaseReach` index is supplied
        (``metabase=``, from the offline ``metabase_lineage.json`` artifact), the Metabase
        dashboards that read any terminal node of a change are APPENDED onto that change's
        reach as EXPOSURE-kind objects — ONE unified reach model. ``metabase=None``
        (the default) leaves every existing behaviour byte-for-byte unchanged.

        Returns a dict with the same top-level keys as :meth:`get_column_impact`
        (``summary``, ``affected_models``, ``affected_columns``,
        ``affected_exposures``) plus a ``by_change`` breakdown.
        """
        # Deferred import: changeset depends on the registry, not the service, so
        # importing here keeps module load order simple and avoids any cycle.
        from parrant.lineage.changeset import ChangeKind

        affected_models: Dict[str, Dict[str, Any]] = {}
        affected_columns: Dict[Tuple[str, str], Dict[str, Any]] = {}
        affected_exposures: Dict[str, Dict[str, Any]] = {}
        by_change: List[Dict[str, Any]] = []
        unresolved = 0

        for change in changes:
            service = self
            if change.kind == ChangeKind.REMOVED and base_service is not None:
                service = base_service

            # The changed column's own dbt docs — "what X is" — so a reviewer sees the
            # meaning of what changed, not just its name. Sourced from whichever side
            # still has the column (base for a removed column, head otherwise).
            change_description = LineageService._lookup_column_description(
                getattr(service, "registry", None), change.model, change.column
            )

            try:
                impact = service.get_column_impact(change.model, change.column)
            except Exception as e:
                logger.info(
                    f"Could not resolve impact for {change.model}.{change.column} "
                    f"({change.kind.value}): {e}"
                )
                unresolved += 1
                by_change.append(
                    {**change.to_dict(), "resolved": False, "description": change_description}
                )
                continue

            for model in impact["affected_models"]:
                affected_models[model["name"]] = model

            for column in impact["affected_columns"]:
                key = (column["model"], column["column"])
                existing = affected_columns.get(key)
                if existing is None or _SEVERITY_RANK.get(
                    column["severity"], 0
                ) > _SEVERITY_RANK.get(existing["severity"], 0):
                    affected_columns[key] = column

            for exposure in impact["affected_exposures"]:
                affected_exposures[exposure["name"]] = exposure

            reached_models, reached_exposures, reached_columns = _reached_from_impact(impact)

            # Cross-boundary reach: the Metabase dashboards that read any terminal
            # node of THIS change — the changed column itself plus every downstream column /
            # model the dbt reach already resolved. Appended, never re-walked.
            if metabase is not None:
                columns_universe: Set[Tuple[str, str]] = {(change.model, change.column)}
                for affected in impact["affected_columns"]:
                    columns_universe.add((affected["model"], affected["column"]))
                models_universe: Set[str] = {change.model} | {
                    model["name"] for model in impact["affected_models"]
                }
                for entry in metabase.reached_dashboards(columns_universe, models_universe):
                    # Carry the column-precise chain (changed column -> card field ->
                    # dashboard) onto THIS change's reach so per-change attribution stays
                    # column-precise downstream, not just the dashboard name.
                    reached_exposures.append(
                        {
                            "name": entry["name"],
                            "via_columns": entry["via_columns"],
                            "precision": entry["precision"],
                        }
                    )
                    # A Metabase dashboard IS an exposure — surface it in the global blast
                    # radius so markdown/JSON render it and the summary count includes it.
                    # When several changes reach the SAME dashboard, union the column chain +
                    # cards so the global entry names every affected column, not just the last.
                    existing = affected_exposures.get(entry["name"])
                    if existing is None:
                        affected_exposures[entry["name"]] = entry
                    else:
                        seen = {
                            (v["model"], v["column"], v["card_id"])
                            for v in existing.get("via_columns", [])
                        }
                        for v in entry["via_columns"]:
                            vk = (v["model"], v["column"], v["card_id"])
                            if vk not in seen:
                                existing.setdefault("via_columns", []).append(v)
                                seen.add(vk)
                        existing["via_cards"] = sorted(
                            set(existing.get("via_cards", [])) | set(entry.get("via_cards", []))
                        )
                        if entry.get("precision") == "column":
                            existing["precision"] = "column"

            by_change.append(
                {
                    **change.to_dict(),
                    "resolved": True,
                    "summary": impact["summary"],
                    "description": change_description,
                    # Reached NAMES (+ mechanism) so a later reach predicate can attribute
                    # what this specific change tripped — the counts in ``summary`` cannot.
                    "reached_models": reached_models,
                    "reached_exposures": reached_exposures,
                    "reached_columns": reached_columns,
                }
            )

        deduped_columns = [affected_columns[key] for key in sorted(affected_columns)]
        critical_count = sum(1 for c in deduped_columns if c["severity"] == "critical")
        # Row-set (filter/join) dependents are a distinct band, mirroring the single-column
        # summary — without this key a filter-only changeset would read as SAFE in the JSON
        # verdict while the markdown banner (which counts filter columns) says REVIEW.
        filter_count = sum(1 for c in deduped_columns if c["severity"] == "filter")
        low_impact_count = len(deduped_columns) - critical_count - filter_count

        # Guarded so a stub service without a real registry omits confidence rather than erroring.
        confidence: Optional[Dict[str, Any]] = None
        if getattr(self, "registry", None) is not None:
            reachable: Set[str] = set()
            for change in changes:
                reachable |= self._dag_reachable_models(change.model)
            confidence = self._impact_confidence(reachable, len(affected_models))

        return {
            "summary": {
                "affected_models": len(affected_models),
                "affected_columns": len(deduped_columns),
                "affected_exposures": len(affected_exposures),
                "critical_count": critical_count,
                "low_impact_count": low_impact_count,
                "filter_count": filter_count,
                "unresolved_changes": unresolved,
                "by_mechanism": _mechanism_breakdown(deduped_columns),
            },
            "affected_models": [affected_models[name] for name in sorted(affected_models)],
            "affected_columns": deduped_columns,
            "affected_exposures": [affected_exposures[name] for name in sorted(affected_exposures)],
            "by_change": by_change,
            "confidence": confidence,
        }
