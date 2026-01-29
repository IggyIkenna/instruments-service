"""
Unit tests for instruments-service output schema validation.

These tests verify the SchemaDefinition for instruments works correctly
with dimension-aware nullability.
"""

import numpy as np
import pandas as pd

from unified_cloud_services import ParquetSchemaEnforcer
from instruments_service.schemas.output_schemas import INSTRUMENTS_SCHEMA, get_instruments_schema


class TestInstrumentsSchema:
    """Tests for INSTRUMENTS_SCHEMA definition."""

    def test_schema_exists(self):
        """Test that schema is defined."""
        schema = get_instruments_schema()
        assert schema is not None
        assert schema.name == "instruments"
        assert "category" in schema.dimension_keys

    def test_required_columns_all_categories(self):
        """Test required columns are consistent across categories."""
        schema = INSTRUMENTS_SCHEMA

        # Core columns should be required for all categories
        for category in ["CEFI", "TRADFI", "DEFI"]:
            required = schema.get_required_columns({"category": category})
            assert "instrument_key" in required
            assert "venue" in required
            assert "instrument_type" in required
            assert "symbol" in required
            assert "available_from_datetime" in required
            assert "timestamp" in required

    def test_cefi_specific_columns(self):
        """Test CEFI-specific column requirements."""
        schema = INSTRUMENTS_SCHEMA

        # tardis_exchange should be required for CEFI
        assert schema.is_nullable("tardis_exchange", {"category": "CEFI"}) is False
        # But nullable for TRADFI
        assert schema.is_nullable("tardis_exchange", {"category": "TRADFI"}) is True

    def test_tradfi_specific_columns(self):
        """Test TRADFI-specific column requirements."""
        schema = INSTRUMENTS_SCHEMA

        # databento_symbol should be required for TRADFI
        assert schema.is_nullable("databento_symbol", {"category": "TRADFI"}) is False
        # But nullable for CEFI
        assert schema.is_nullable("databento_symbol", {"category": "CEFI"}) is True

        # Trading hours should be required for TRADFI
        assert schema.is_nullable("trading_hours_open", {"category": "TRADFI"}) is False
        assert schema.is_nullable("trading_hours_close", {"category": "TRADFI"}) is False

    def test_validate_valid_cefi_instruments(self):
        """Test validation with valid CEFI instruments."""
        df = pd.DataFrame({
            "instrument_key": ["BINANCE-FUTURES:PERPETUAL:BTC-USDT"],
            "venue": ["BINANCE-FUTURES"],
            "instrument_type": ["PERPETUAL"],
            "symbol": ["BTC-USDT"],
            "available_from_datetime": [pd.Timestamp("2020-01-01")],
            "timestamp": [pd.Timestamp("2024-01-01")],
            "tardis_exchange": ["binance-futures"],  # Required for CEFI
            "databento_symbol": [None],  # Nullable for CEFI
        })

        enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
        result = enforcer.validate_dataframe(df, {"category": "CEFI"})

        assert result.valid is True

    def test_validate_invalid_cefi_missing_tardis(self):
        """Test validation fails when CEFI instrument missing tardis_exchange."""
        df = pd.DataFrame({
            "instrument_key": ["BINANCE-FUTURES:PERPETUAL:BTC-USDT"],
            "venue": ["BINANCE-FUTURES"],
            "instrument_type": ["PERPETUAL"],
            "symbol": ["BTC-USDT"],
            "available_from_datetime": [pd.Timestamp("2020-01-01")],
            "timestamp": [pd.Timestamp("2024-01-01")],
            "tardis_exchange": [np.nan],  # Missing - should fail for CEFI
        })

        enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
        result = enforcer.validate_dataframe(df, {"category": "CEFI"})

        assert result.valid is False
        assert any("tardis_exchange" in str(e) for e in result.errors)

    def test_validate_valid_tradfi_instruments(self):
        """Test validation with valid TRADFI instruments."""
        df = pd.DataFrame({
            "instrument_key": ["CME:FUTURE:ES.FUT"],
            "venue": ["CME"],
            "instrument_type": ["FUTURE"],
            "symbol": ["ES.FUT"],
            "available_from_datetime": [pd.Timestamp("2020-01-01")],
            "timestamp": [pd.Timestamp("2024-01-01")],
            "databento_symbol": ["ES.FUT"],  # Required for TRADFI
            "trading_hours_open": ["09:30:00-05:00"],  # Required for TRADFI
            "trading_hours_close": ["16:00:00-05:00"],  # Required for TRADFI
            "tardis_exchange": [None],  # Nullable for TRADFI
        })

        enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
        result = enforcer.validate_dataframe(df, {"category": "TRADFI"})

        assert result.valid is True

    def test_validate_invalid_tradfi_missing_databento(self):
        """Test validation fails when TRADFI instrument missing databento_symbol."""
        df = pd.DataFrame({
            "instrument_key": ["CME:FUTURE:ES.FUT"],
            "venue": ["CME"],
            "instrument_type": ["FUTURE"],
            "symbol": ["ES.FUT"],
            "available_from_datetime": [pd.Timestamp("2020-01-01")],
            "timestamp": [pd.Timestamp("2024-01-01")],
            "databento_symbol": [np.nan],  # Missing - should fail for TRADFI
            "trading_hours_open": ["09:30:00-05:00"],
            "trading_hours_close": ["16:00:00-05:00"],
        })

        enforcer = ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA)
        result = enforcer.validate_dataframe(df, {"category": "TRADFI"})

        assert result.valid is False
        assert any("databento_symbol" in str(e) for e in result.errors)
