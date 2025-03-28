class RegistryError(Exception):
    """Base exception for all registry-related errors."""
    pass

class ModelNotFoundError(RegistryError):
    """Raised when a requested model is not found."""
    pass

class RegistryNotLoadedError(RegistryError):
    """Raised when trying to access registry before loading data."""
    pass

class RegistryLoadError(Exception):
    """Base exception for registry loading errors."""
    pass

class DuplicateLoadError(RegistryLoadError):
    """Raised when attempting to load an already loaded registry."""
    pass

class ModelLoadError(RegistryLoadError):
    """Raised when failing to load model information."""
    pass

class DependencyLoadError(RegistryLoadError):
    """Raised when failing to load model dependencies."""
    pass

class LineageProcessError(RegistryLoadError):
    """Raised when failing to process model lineage."""
    pass