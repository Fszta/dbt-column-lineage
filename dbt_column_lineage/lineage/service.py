from pathlib import Path
from typing import Dict, Set, Optional
from dataclasses import dataclass
import logging

from dbt_column_lineage.artifacts.registry import ModelRegistry
from dbt_column_lineage.models.schema import Column, ColumnLineage

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
    
    def __init__(self, catalog_path: Path, manifest_path: Path):
        self.registry = ModelRegistry(str(catalog_path), str(manifest_path))
        self.registry.load()

    def get_model_info(self, selector: LineageSelector) -> Dict:
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

    def get_column_info(self, selector: LineageSelector) -> Dict:
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

    def _process_source_reference(self, source: str, upstream_refs: Dict) -> None:
        """Process a source reference and add it to upstream_refs."""
        if 'sources' not in upstream_refs:
            upstream_refs['sources'] = set()
        upstream_refs['sources'].add(source)
        logger.debug(f"Added source reference: {source}")

    def _process_model_reference(self, src_model: str, src_column: str, lineage: ColumnLineage, 
                               upstream_refs: Dict, visited: Set[str]) -> None:
        """Process a model reference and add it to upstream_refs."""
        try:
            self.registry.get_model(src_model)
            logger.debug(f"Found model {src_model} in registry")
            
            if src_model not in upstream_refs:
                upstream_refs[src_model] = {}
            upstream_refs[src_model][src_column] = lineage
            
            self._get_upstream_lineage(src_model, src_column, visited)
            
        except Exception as e:
            logger.debug(f"Processing as source: {src_model}.{src_column} (Error: {str(e)})")
            self._process_source_reference(f"{src_model}.{src_column}", upstream_refs)

    def _get_upstream_lineage(self, model_name: str, column_name: str, visited: Optional[Set[str]] = None) -> Dict:
        """Recursively get all upstream column references."""
        if visited is None:
            visited = set()
        
        current_ref = f"{model_name}.{column_name}"
        if current_ref in visited:
            return {}
        
        visited.add(current_ref)
        upstream_refs = {}
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
                        upstream_refs['direct_refs'].add(source)
                        continue
                    
                    src_model, src_column = source.split('.')
                    if src_model in current_model.upstream:
                        self._process_model_reference(src_model, src_column, lineage, upstream_refs, visited)
                    
        except Exception as e:
            logger.warning(f"Failed to process lineage for {current_ref}: {str(e)}")
            
        return upstream_refs

    def _get_downstream_lineage(self, model_name: str, column_name: str, visited: Optional[Set[str]] = None) -> Dict:
        """Get downstream column references following the model DAG."""
        if visited is None:
            visited = set()
        
        current_ref = f"{model_name}.{column_name}"
        if current_ref in visited:
            return {}
        
        visited.add(current_ref)
        downstream_refs = {}
        current_model = self.registry.get_model(model_name)
        
        try:
            for other_model_name in current_model.downstream:
                other_model = self.registry.get_model(other_model_name)
                for col_name, col in other_model.columns.items():
                    if not col.lineage:
                        continue
                        
                    for lineage in col.lineage:
                        if any(src == current_ref for src in lineage.source_columns):
                            if other_model_name not in downstream_refs:
                                downstream_refs[other_model_name] = {}
                            downstream_refs[other_model_name][col_name] = lineage
                            
                            next_refs = self._get_downstream_lineage(other_model_name, col_name, visited)
                            for model, cols in next_refs.items():
                                if model not in downstream_refs:
                                    downstream_refs[model] = {}
                                downstream_refs[model].update(cols)
                            
        except Exception as e:
            logger.warning(f"Failed to process downstream lineage for {current_ref}: {str(e)}")
            
        return downstream_refs 