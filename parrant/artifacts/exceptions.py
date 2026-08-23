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
