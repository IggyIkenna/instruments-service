"""
Unit tests for Parquet schema definitions.
"""

from instruments_service.schemas.parquet import (
    get_required_columns,
    get_optional_columns,
    get_column_info,
    validate_schema_compliance,
    get_schema_summary,
    INSTRUMENTS_PARQUET_SCHEMA,
    SCHEMA_METADATA,
)


class TestParquetSchema:
    """Tests for Parquet schema functions."""

    def test_get_required_columns(self):
        """Test getting required columns."""
        required = get_required_columns()
        assert isinstance(required, list)
        assert "instrument_key" in required
        assert "venue" in required
        assert "instrument_type" in required
        assert "symbol" in required
        assert "available_from_datetime" in required
        assert "timestamp" in required

    def test_get_optional_columns(self):
        """Test getting optional columns."""
        optional = get_optional_columns()
        assert isinstance(optional, list)
        assert "venue_type" in optional
        assert "tardis_exchange" in optional
        assert "base_asset" in optional

    def test_get_column_info_existing(self):
        """Test getting info for existing column."""
        info = get_column_info("instrument_key")
        assert info is not None
        assert info["name"] == "instrument_key"
        assert info["required"] is True
        assert "description" in info

    def test_get_column_info_optional(self):
        """Test getting info for optional column."""
        info = get_column_info("base_asset")
        assert info is not None
        assert info["name"] == "base_asset"
        assert info.get("required", False) is False

    def test_get_column_info_nonexistent(self):
        """Test getting info for non-existent column."""
        info = get_column_info("nonexistent_column")
        assert info is None

    def test_validate_schema_compliance_valid(self):
        """Test schema validation with valid columns."""
        df_columns = [
            "instrument_key",
            "venue",
            "instrument_type",
            "symbol",
            "available_from_datetime",
            "timestamp",
        ]
        is_valid, missing = validate_schema_compliance(df_columns)
        assert is_valid is True
        assert len(missing) == 0

    def test_validate_schema_compliance_missing_required(self):
        """Test schema validation with missing required columns."""
        df_columns = ["instrument_key", "venue"]  # Missing required columns
        is_valid, missing = validate_schema_compliance(df_columns)
        assert is_valid is False
        assert len(missing) > 0
        assert "instrument_type" in missing or "symbol" in missing

    def test_get_schema_summary(self):
        """Test getting schema summary."""
        summary = get_schema_summary()
        assert isinstance(summary, dict)
        assert "total_fields" in summary
        assert "required_fields" in summary
        assert "optional_fields" in summary
        assert "required_column_names" in summary
        assert "metadata" in summary
        assert summary["total_fields"] == len(INSTRUMENTS_PARQUET_SCHEMA)
        assert summary["required_fields"] == len(get_required_columns())
        assert summary["optional_fields"] == len(get_optional_columns())

    def test_schema_metadata(self):
        """Test schema metadata structure."""
        assert "version" in SCHEMA_METADATA
        assert "last_updated" in SCHEMA_METADATA
        assert "source_model" in SCHEMA_METADATA
