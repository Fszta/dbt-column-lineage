from abc import ABC, abstractmethod
from typing import Dict, Union, Set
from parrant.models.schema import Column, ColumnLineage, Coverage


def format_coverage_line(coverage: Coverage) -> str:
    """Render a one-line coverage statement (terse when complete, detailed when partial)."""
    if coverage.complete:
        return (
            f"Coverage: {coverage.models_in_catalog}/{coverage.models_in_manifest} "
            f"models, complete."
        )
    return (
        f"Coverage: analyzed {coverage.parsed_ok}/{coverage.models_in_manifest} models "
        f"({coverage.models_in_catalog} in catalog; "
        f"{coverage.not_in_catalog_count} not in catalog, "
        f"{coverage.parse_failed} parse-failed, "
        f"{coverage.skipped_no_sql} no compiled SQL). "
        f"Impact counts are a lower bound."
    )


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
    def display_downstream(
        self, refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]
    ) -> None:
        """Display downstream lineage."""
        pass

    def display_coverage(self, coverage: Coverage) -> None:
        """Render a coverage statement. Default no-op; text/json override."""
        pass

    @abstractmethod
    def save(self) -> None:
        """Save or finalize the display output."""
        pass
