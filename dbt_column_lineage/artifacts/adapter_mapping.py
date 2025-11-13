"""
Centralized adapter/dialect normalization for dbt-column-lineage.

This module defines a single source of truth for mapping raw dbt adapter
names to sqlglot dialect names. For example, dbt may report the adapter
"sqlserver" while sqlglot expects the dialect name "tsql".

Extend ADAPTER_TO_DIALECT as needed to support additional adapters.
"""
from typing import Dict, Optional

# Mapping from dbt adapter name (metadata.adapter_type) to sqlglot dialect
ADAPTER_TO_DIALECT: Dict[str, str] = {
    # dbt-sqlserver adapter reports "sqlserver"; sqlglot uses "tsql"
    "sqlserver": "tsql",
}


def normalize_adapter(adapter_name: Optional[str]) -> Optional[str]:
    """Normalize a dbt adapter name to a sqlglot dialect name.

    If adapter_name is None or empty, returns it unchanged.
    If there is no mapping defined, returns the original adapter_name.
    """
    if not adapter_name:
        return adapter_name
    lower = adapter_name.lower()
    return ADAPTER_TO_DIALECT.get(lower, lower)
