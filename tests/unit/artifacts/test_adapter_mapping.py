"""Tests for adapter_mapping module."""
import pytest
from dbt_column_lineage.artifacts.adapter_mapping import normalize_adapter, ADAPTER_TO_DIALECT


class TestAdapterMapping:
    """Test adapter name normalization."""
    
    def test_sqlserver_maps_to_tsql(self):
        """Test that sqlserver adapter maps to tsql dialect."""
        assert normalize_adapter("sqlserver") == "tsql"
        assert normalize_adapter("SQLSERVER") == "tsql"
        assert normalize_adapter("SQLServer") == "tsql"
    
    def test_unmapped_adapters_return_as_is(self):
        """Test that unmapped adapters return their lowercase name."""
        assert normalize_adapter("snowflake") == "snowflake"
        assert normalize_adapter("bigquery") == "bigquery"
        assert normalize_adapter("postgres") == "postgres"
        assert normalize_adapter("duckdb") == "duckdb"
    
    def test_none_and_empty_strings(self):
        """Test that None and empty strings are handled gracefully."""
        assert normalize_adapter(None) is None
        assert normalize_adapter("") == ""
    
    def test_case_insensitivity(self):
        """Test that adapter names are case insensitive."""
        assert normalize_adapter("SNOWFLAKE") == "snowflake"
        assert normalize_adapter("BigQuery") == "bigquery"
    
    def test_adapter_to_dialect_mapping(self):
        """Test that the mapping dictionary contains expected entries."""
        assert "sqlserver" in ADAPTER_TO_DIALECT
        assert ADAPTER_TO_DIALECT["sqlserver"] == "tsql"

