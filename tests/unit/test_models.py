"""
Unit tests for instrument models.
"""

import pytest
from datetime import datetime, timezone
from instruments_service.models import (
    Venue,
    InstrumentType,
    InstrumentKey,
    InstrumentDefinition,
)


class TestInstrumentKey:
    """Test InstrumentKey dataclass."""

    def test_instrument_key_creation(self):
        """Test creating InstrumentKey."""
        key = InstrumentKey(
            venue=Venue.BINANCE_FUTURES,
            instrument_type=InstrumentType.PERPETUAL,
            symbol="BTC-USDT",
        )
        assert key.venue == Venue.BINANCE_FUTURES
        assert key.instrument_type == InstrumentType.PERPETUAL
        assert key.symbol == "BTC-USDT"

    def test_instrument_key_string_format(self):
        """Test InstrumentKey string formatting."""
        key = InstrumentKey(
            venue=Venue.BINANCE_FUTURES,
            instrument_type=InstrumentType.PERPETUAL,
            symbol="BTC-USDT",
        )
        assert str(key) == "BINANCE-FUTURES:PERPETUAL:BTC-USDT"

    def test_instrument_key_with_expiry(self):
        """Test InstrumentKey with expiry."""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.FUTURE,
            symbol="BTC-USDT",
            expiry="250101",
        )
        assert str(key) == "DERIBIT:FUTURE:BTC-USDT:250101"

    def test_instrument_key_with_option(self):
        """Test InstrumentKey with option type."""
        key = InstrumentKey(
            venue=Venue.DERIBIT,
            instrument_type=InstrumentType.OPTION,
            symbol="BTC-USDT-50000",
            expiry="250101",
            option_type="C",
        )
        assert str(key) == "DERIBIT:OPTION:BTC-USDT-50000:250101:C"

    def test_instrument_key_from_string(self):
        """Test parsing InstrumentKey from string."""
        key_str = "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
        key = InstrumentKey.from_string(key_str)
        assert key.venue == Venue.BINANCE_FUTURES
        assert key.instrument_type == InstrumentType.PERPETUAL
        assert key.symbol == "BTC-USDT"

    def test_instrument_key_from_string_with_expiry(self):
        """Test parsing InstrumentKey with expiry."""
        key_str = "DERIBIT:FUTURE:BTC-USDT:250101"
        key = InstrumentKey.from_string(key_str)
        assert key.expiry == "250101"

    def test_instrument_key_from_string_invalid(self):
        """Test parsing invalid InstrumentKey."""
        with pytest.raises(ValueError):
            InstrumentKey.from_string("INVALID")


class TestInstrumentDefinition:
    """Test InstrumentDefinition Pydantic model."""

    def test_instrument_definition_creation_minimal(self):
        """Test creating minimal InstrumentDefinition."""
        inst = InstrumentDefinition(
            instrument_key="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            venue="BINANCE-FUTURES",
            instrument_type="PERPETUAL",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
        )
        assert inst.instrument_key == "BINANCE-FUTURES:PERPETUAL:BTC-USDT"
        assert inst.venue == "BINANCE-FUTURES"
        assert inst.instrument_type == "PERPETUAL"

    def test_instrument_definition_defaults(self):
        """Test InstrumentDefinition default values."""
        inst = InstrumentDefinition(
            instrument_key="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            venue="BINANCE-FUTURES",
            instrument_type="PERPETUAL",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
        )
        assert inst.venue_type == "exchange"
        assert inst.data_provider == "tardis"
        assert inst.asset_class == "crypto"

    def test_instrument_definition_with_option_fields(self):
        """Test InstrumentDefinition with option-specific fields."""
        inst = InstrumentDefinition(
            instrument_key="DERIBIT:OPTION:BTC-USDT-50000-CALL",
            venue="DERIBIT",
            instrument_type="OPTION",
            symbol="BTC-USDT-50000-CALL",
            available_from_datetime="2023-01-01T00:00:00Z",
            strike="50000.0",
            option_type="CALL",
        )
        assert inst.strike == "50000.0"
        assert inst.option_type == "CALL"

    def test_instrument_definition_validation(self):
        """Test InstrumentDefinition validation."""
        # Missing required field should raise validation error
        with pytest.raises(Exception):  # Pydantic validation error
            InstrumentDefinition(
                instrument_key="BINANCE-FUTURES:PERPETUAL:BTC-USDT",
                venue="BINANCE-FUTURES",
                instrument_type="PERPETUAL",
                symbol="BTC-USDT",
                # Missing available_from_datetime
            )
