import json
from typing import Dict, Optional, Set, Any
from pathlib import Path


class ManifestReader:
    def __init__(self, manifest_path: Optional[str] = None):
        self.manifest_path = Path(manifest_path) if manifest_path else None
        self.manifest: Dict[str, Any] = {}

    def load(self) -> None:
        if not self.manifest_path or not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")
        with open(self.manifest_path, "r") as f:
            self.manifest = json.load(f)

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
                    if dep_id.startswith("model."):
                        dep_name = dep_id.split(".")[-1]
                        upstream[model_name].add(dep_name)
                    elif dep_id.startswith("source."):
                        source_name = dep_id.split(".")[-1]
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

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        node = self.manifest.get('nodes', {}).get(node_id)
        if node is None:
            return None
        return dict(node)
