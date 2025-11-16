import json
from typing import Dict, Optional, Set, Any
from pathlib import Path

from dbt_column_lineage.artifacts.adapter_mapping import normalize_adapter


class ManifestReader:
    def __init__(self, manifest_path: Optional[str] = None):
        self.manifest_path = Path(manifest_path) if manifest_path else None
        self.manifest: Dict[str, Any] = {}

    def load(self) -> None:
        if not self.manifest_path or not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")
        with open(self.manifest_path, "r") as f:
            self.manifest = json.load(f)
            
    def get_adapter(self) -> Optional[str]:
        adapter_name = self.manifest.get("metadata", {}).get("adapter_type")
        return normalize_adapter(adapter_name)



    def _find_node(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Find a node in the manifest by model name."""
        if not self.manifest:
            return None
        for _, node in self.manifest.get("nodes", {}).items():
            if node.get("name") == model_name:
                return dict(node)
        return None

    def get_model_dependencies(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model dependencies with full model names.
        
        Returns:
            Dict[str, Set[str]]: Key is full model name, value is set of full dependency names
        """
        dependencies = {}
        for model_id, model_data in self.manifest.get("nodes", {}).items():
            depends_on = set(
                f"{dep['alias']}.{dep['alias']}"
                for dep in model_data.get("depends_on", {}).get("nodes", [])
            )
            dependencies[model_id] = depends_on
        return dependencies

    def get_model_upstream(self) -> Dict[str, Set[str]]:
        """Get upstream dependencies for each model."""
        upstream: Dict[str, Set[str]] = {}
        
        for _, node in self.manifest.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                model_name = node.get("name")
                if not model_name:
                    continue
                
                upstream[model_name] = set()
                
                depends_on = node.get("depends_on", {})
                for dep_id in depends_on.get("nodes", []):
                    parts = dep_id.split(".")
                    if parts[0] == "model":
                        dep_name = parts[-1]
                        upstream[model_name].add(dep_name)
                    elif parts[0] == "source":
                        source_node = self.manifest.get("sources", {}).get(dep_id, {})
                        source_identifier = source_node.get("identifier")
                        if source_identifier:
                            upstream[model_name].add(source_identifier)
                        else:
                            # Fallback to source name if identifier not found
                            source_name = parts[-1]
                            upstream[model_name].add(source_name)
        
        return upstream
    
    def get_model_downstream(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model downstream dependencies."""
        downstream: Dict[str, Set[str]] = {}
        
        upstream_deps = self.get_model_upstream()
        
        for model_name, upstream_models in upstream_deps.items():
            for upstream_model in upstream_models:
                if upstream_model not in downstream:
                    downstream[upstream_model] = set()
                downstream[upstream_model].add(model_name)
            
        return downstream
    
    def get_compiled_sql(self, model_name: str) -> Optional[str]:
        """Get compiled SQL for a model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
            
        return node.get("compiled_sql") or node.get("compiled_code")

    def get_model_path(self, model_name: str) -> Optional[str]:
        """Get the path to the model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
            
        return node.get("path")

    def get_model_language(self, model_name: str) -> Optional[str]:
        """Get the language of a model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
        return node.get("language")

    def get_model_resource_path(self, model_name: str) -> Optional[str]:
        """Get the original file path of a model from the manifest."""
        node = self._find_node(model_name)
        if not node:
            return None
        return node.get("original_file_path")

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        node = self.manifest.get('nodes', {}).get(node_id)
        if node is None:
            return None
        return dict(node)

    def get_exposures(self) -> Dict[str, Dict[str, Any]]:
        """Get all exposures from the manifest.
        
        Returns:
            Dict[str, Dict[str, Any]]: Key is exposure unique_id, value is exposure data
        """
        return self.manifest.get("exposures", {})

    def get_exposure_dependencies(self) -> Dict[str, Set[str]]:
        """Get model dependencies for each exposure.
        
        Returns:
            Dict[str, Set[str]]: Key is exposure name, value is set of model names it depends on
        """
        exposure_deps: Dict[str, Set[str]] = {}
        
        for exposure_id, exposure_data in self.manifest.get("exposures", {}).items():
            exposure_name = exposure_data.get("name")
            if not exposure_name:
                continue
            
            exposure_deps[exposure_name] = set()
            
            depends_on = exposure_data.get("depends_on", {})
            for dep_id in depends_on.get("nodes", []):
                parts = dep_id.split(".")
                if parts[0] == "model":
                    dep_name = parts[-1]
                    exposure_deps[exposure_name].add(dep_name)
                elif parts[0] == "source":
                    source_node = self.manifest.get("sources", {}).get(dep_id, {})
                    source_identifier = source_node.get("identifier")
                    if source_identifier:
                        exposure_deps[exposure_name].add(source_identifier)
                    else:
                        source_name = parts[-1]
                        exposure_deps[exposure_name].add(source_name)
        
        return exposure_deps

    def get_model_exposures(self) -> Dict[str, Set[str]]:
        """Get exposures that depend on each model.
        
        Returns:
            Dict[str, Set[str]]: Key is model name, value is set of exposure names that depend on it
        """
        model_exposures: Dict[str, Set[str]] = {}
        
        exposure_deps = self.get_exposure_dependencies()
        
        for exposure_name, model_names in exposure_deps.items():
            for model_name in model_names:
                if model_name not in model_exposures:
                    model_exposures[model_name] = set()
                model_exposures[model_name].add(exposure_name)
        
        return model_exposures
