from abc import ABC, abstractmethod
from typing import Dict, Union, Set
from dbt_column_lineage.models.schema import Column, ColumnLineage

class LineageStaticDisplay(ABC):
    """Abstract base class for lineage display strategies."""
    
    @abstractmethod
    def display_column_info(self, column: Column) -> None:
        """Display basic column information."""
        pass

    @abstractmethod
    def display_upstream(self, refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]) -> None:
        """Display upstream lineage."""
        pass

    @abstractmethod
    def display_downstream(self, refs: Dict[str, Dict[str, ColumnLineage]]) -> None:
        """Display downstream lineage."""
        pass

    @abstractmethod
    def save(self) -> None:
        """Save or finalize the display output."""
        pass