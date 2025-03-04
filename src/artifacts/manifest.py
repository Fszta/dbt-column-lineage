import json
from typing import Dict, Optional, Set
from pathlib import Path


class ManifestReader:
    def __init__(self, manifest_path: Optional[str] = None):
        self.manifest_path = Path(manifest_path)
        self.manifest: Dict = {}

    def load(self):
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")
        with open(self.manifest_path, "r") as f:
            self.manifest = json.load(f)

    def get_model_upstream(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model dependencies.
        
        Returns:
            Dict[str, Set[str]]: Key is model name, value is set of models name it depends on
        """
        upstream = {}
        for _, model_data in self.manifest.get("nodes", {}).items():
            # Extract just the model name from the full model_name
            model_name_short = model_data["name"]
            depends_on = set(
                dep["alias"]  # Just use the alias which is the model name
                for dep in model_data.get("depends_on", {}).get("nodes", [])
            )
            upstream[model_name_short] = depends_on
        return upstream
    
    def get_model_downstream(self) -> Dict[str, Set[str]]:
        """Return a dictionary of model downstream dependencies.
        
        Returns:
            Dict[str, Set[str]]: Key is model name, value is set of models name that depend on it
        """
        downstream = {}
        
        
        upstream_deps = self.get_model_upstream()
        
        for model_name, upstream_models in upstream_deps.items():
            for upstream_model in upstream_models:
                if upstream_model not in downstream:
                    downstream[upstream_model] = set()
                downstream[upstream_model].add(model_name)
            
        return downstream 