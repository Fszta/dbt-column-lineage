from abc import ABC, abstractmethod
from typing import Dict
from dbt_column_lineage.models.schema import Column, ColumnLineage

class LineageDisplay(ABC):
    """Abstract base class for lineage display strategies."""
    
    @abstractmethod
    def display_column_info(self, column: Column) -> None:
        """Display basic column information."""
        pass

    @abstractmethod
    def display_upstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        """Display upstream lineage."""
        pass

    @abstractmethod
    def display_downstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        """Display downstream lineage."""
        pass