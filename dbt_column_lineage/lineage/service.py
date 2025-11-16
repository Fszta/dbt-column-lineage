from pathlib import Path
from typing import Dict, Set, Optional, Any, Union
from dataclasses import dataclass
import logging

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.models.schema import ColumnLineage

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
        
        upstream = selector.startswith('+')
        downstream = selector.endswith('+')
        
        if not upstream and not downstream:
            upstream = downstream = True
        
        clean_selector = selector.strip('+')
        model_name, column_name = clean_selector.split('.', 1) if '.' in clean_selector else (clean_selector, None)
        
        return cls(model=model_name, column=column_name, upstream=upstream, downstream=downstream)

class LineageService:
    """Service for handling lineage operations."""
    
    def __init__(self, catalog_path: Path, manifest_path: Path, adapter: Optional[str] = None):
        self.registry = ModelRegistry(str(catalog_path), str(manifest_path), adapter_override=adapter)
        self.registry.load()

    def get_model_info(self, selector: LineageSelector) -> Dict[str, Any]:
        """Get model information based on selector."""
        model = self.registry.get_model(selector.model)
        return {
            'name': model.name,
            'schema': model.schema_name,
            'database': model.database,
            'columns': list(model.columns.keys()),
            'upstream': list(model.upstream) if selector.upstream else [],
            'downstream': list(model.downstream) if selector.downstream else []
        }

    def get_column_info(self, selector: LineageSelector) -> Dict[str, Any]:
        """Get column information and lineage based on selector."""
        model = self.registry.get_model(selector.model)
        if not selector.column or selector.column not in model.columns:
            raise ValueError(f"Column '{selector.column}' not found in model '{selector.model}'")
            
        column = model.columns[selector.column]
        return {
            'name': column.name,
            'data_type': column.data_type,
            'description': column.description,
            'upstream': self._get_upstream_lineage(selector.model, selector.column) if selector.upstream else {},
            'downstream': self._get_downstream_lineage(selector.model, selector.column) if selector.downstream else {}
        }

    def _process_source_reference(self, source: str, upstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]) -> None:
        """Process a source reference and add it to upstream_refs."""
        if 'sources' not in upstream_refs:
            upstream_refs['sources'] = set()
        
        sources = upstream_refs['sources']
        if isinstance(sources, set):
            sources.add(source)

    def _process_model_reference(self, src_model: str, src_column: str, lineage: ColumnLineage, 
                               upstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]], 
                               visited: Set[str]) -> None:
        """Process a model reference and add it to upstream_refs."""
        try:
            self.registry.get_model(src_model)
            
            if src_model not in upstream_refs:
                upstream_refs[src_model] = {}
            
            model_refs = upstream_refs[src_model]
            if isinstance(model_refs, dict):
                model_refs[src_column] = lineage
            
            self._get_upstream_lineage(src_model, src_column, visited)
            
        except Exception:
            self._process_source_reference(f"{src_model}.{src_column}", upstream_refs)

    def _get_upstream_lineage(self, model_name: str, column_name: str, 
                            visited: Optional[Set[str]] = None) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Recursively get all upstream column references."""
        if visited is None:
            visited = set()
        
        current_ref = f"{model_name}.{column_name}"
        if current_ref in visited:
            return {}
        
        visited.add(current_ref)
        upstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]] = {}
        current_model = self.registry.get_model(model_name)
        
        try:
            column = current_model.columns[column_name]
            if not column.lineage:
                return upstream_refs
            
            for lineage in column.lineage:
                for source in lineage.source_columns:
                    if '.' not in source:
                        if 'direct_refs' not in upstream_refs:
                            upstream_refs['direct_refs'] = set()
                        direct_refs = upstream_refs['direct_refs']
                        if isinstance(direct_refs, set):
                            direct_refs.add(source)
                        continue
                    
                    src_model, src_column = source.split('.')
                    if src_model in current_model.upstream:
                        self._process_model_reference(src_model, src_column, lineage, upstream_refs, visited)
                    
        except Exception as e:
            logger.warning(f"Failed to process lineage for {current_ref}: {str(e)}")
            
        return upstream_refs

    def _get_downstream_lineage(self, model_name: str, column_name: str, 
                              visited: Optional[Set[str]] = None) -> Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]:
        """Get downstream column references following the model DAG, including exposures."""
        if visited is None:
            visited = set()
        
        # Normalize to match source_columns format (column names are lowercase in source_columns)
        current_ref = f"{model_name}.{column_name.lower()}"
        if current_ref in visited:
            return {}
        
        visited.add(current_ref)
        downstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]] = {}
        current_model = self.registry.get_model(model_name)
        
        try:
            column_used_downstream = False
            downstream_models_using_column = []
            
            for other_name in current_model.downstream:
                try:
                    self.registry.get_exposure(other_name)
                    continue
                except (ValueError, KeyError):
                    pass
                
                try:
                    other_model = self.registry.get_model(other_name)
                    for col_name, col in other_model.columns.items():
                        if not col.lineage:
                            continue
                            
                        for lineage in col.lineage:
                            # Check if current_ref (normalized) is in source_columns
                            # source_columns are normalized to lowercase for column names
                            if any(src.lower() == current_ref.lower() for src in lineage.source_columns):
                                column_used_downstream = True
                                if other_name not in downstream_models_using_column:
                                    downstream_models_using_column.append(other_name)
                                
                                if other_name not in downstream_refs:
                                    downstream_refs[other_name] = {}
                                model_refs = downstream_refs[other_name]
                                if isinstance(model_refs, dict):
                                    model_refs[col_name] = lineage
                                
                                # Recursively get downstream lineage (col_name is already the actual column name)
                                next_refs = self._get_downstream_lineage(other_name, col_name, visited)
                                for model, cols in next_refs.items():
                                    if model == 'exposures':
                                        if isinstance(cols, set):
                                            if 'exposures' not in downstream_refs:
                                                downstream_refs['exposures'] = set()
                                            downstream_refs['exposures'].update(cols)
                                        continue
                                        
                                    if model not in downstream_refs:
                                        downstream_refs[model] = {}
                                    model_refs = downstream_refs[model]
                                    if isinstance(cols, dict) and isinstance(model_refs, dict):
                                        model_refs.update(cols)
                                        if model not in downstream_models_using_column:
                                            downstream_models_using_column.append(model)
                                    elif isinstance(cols, set) and isinstance(model_refs, set):
                                        model_refs.update(cols)
                except Exception as e:
                    logger.warning(f"Failed to process downstream model {other_name}: {str(e)}")
            
            if column_used_downstream and downstream_models_using_column:
                models_using_column = set(downstream_models_using_column)
                models_using_column.add(model_name)
            else:
                models_using_column = {model_name}
            
            for other_name in current_model.downstream:
                try:
                    exposure = self.registry.get_exposure(other_name)
                    if any(model in models_using_column for model in exposure.depends_on_models):
                        if 'exposures' not in downstream_refs:
                            downstream_refs['exposures'] = set()
                        downstream_refs['exposures'].add(other_name)
                except (ValueError, KeyError):
                    pass
                            
        except Exception as e:
            logger.warning(f"Failed to process downstream lineage for {current_ref}: {str(e)}")
            
        return downstream_refs 

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
            
            for downstream_model_name, columns in downstream_refs.items():
                # Skip non-model entries (exposures, sources, direct_refs)
                if downstream_model_name in ('exposures', 'sources', 'direct_refs') or not isinstance(columns, dict):
                    continue
                
                try:
                    downstream_model = self.registry.get_model(downstream_model_name)
                    
                    if downstream_model_name not in affected_models:
                        affected_models[downstream_model_name] = {
                            'name': downstream_model_name,
                            'resource_type': getattr(downstream_model, 'resource_type', 'model'),
                            'schema': downstream_model.schema_name,
                            'database': downstream_model.database
                        }
                    
                    for col_name, lineage in columns.items():
                        if not isinstance(lineage, ColumnLineage):
                            logger.warning(f"Expected ColumnLineage for {downstream_model_name}.{col_name}, got {type(lineage)}")
                            continue
                            
                        # Determine severity based on transformation type
                        # Critical = derived/transformed columns (transformation logic might break)
                        # Low impact = direct/renamed (just pass-through, change propagates)
                        is_critical = lineage.transformation_type == 'derived'
                        if is_critical:
                            critical_count += 1
                        else:
                            potential_count += 1
                        
                        col_obj = downstream_model.columns.get(col_name)
                        affected_columns.append({
                            'model': downstream_model_name,
                            'column': col_name,
                            'transformation_type': lineage.transformation_type,
                            'sql_expression': lineage.sql_expression,
                            'severity': 'critical' if is_critical else 'low_impact',
                            'data_type': col_obj.data_type if col_obj else None
                        })
                        
                except Exception as e:
                    logger.warning(f"Failed to process model {downstream_model_name} in impact analysis: {e}", exc_info=True)
            
            if 'exposures' in downstream_refs and isinstance(downstream_refs['exposures'], set):
                for exposure_name in downstream_refs['exposures']:
                    try:
                        exposure = self.registry.get_exposure(exposure_name)
                        affected_exposures.append({
                            'name': exposure.name,
                            'type': exposure.type,
                            'url': exposure.url,
                            'description': exposure.description,
                            'depends_on_models': list(exposure.depends_on_models)
                        })
                    except Exception as e:
                        logger.warning(f"Failed to process exposure {exposure_name} in impact analysis: {e}")
            
            return {
                'summary': {
                    'affected_models': len(affected_models),
                    'affected_columns': len(affected_columns),
                    'affected_exposures': len(affected_exposures),
                    'critical_count': critical_count,
                    'low_impact_count': potential_count
                },
                'affected_models': list(affected_models.values()),
                'affected_columns': affected_columns,
                'affected_exposures': affected_exposures
            }
            
        except Exception as e:
            logger.error(f"Error in impact analysis for {model_name}.{column_name}: {e}")
            raise 