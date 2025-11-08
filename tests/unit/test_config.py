"""
Unit tests for configuration classes.
"""

import pytest
from instruments_service.config import (
    VenueMapping,
    ExchangeInstrumentConfig,
    DataTypeConfig
)


class TestVenueMapping:
    """Test VenueMapping configuration."""
    
    def test_venue_mapping_creation(self):
        """Test creating VenueMapping."""
        mapping = VenueMapping()
        assert len(mapping.all_tardis_exchanges) > 0
        assert 'binance' in mapping.all_tardis_exchanges
        assert 'deribit' in mapping.all_tardis_exchanges
    
    def test_tardis_to_venue_mapping(self):
        """Test Tardis exchange to venue mapping."""
        mapping = VenueMapping()
        assert mapping.tardis_to_venue['binance'] == 'BINANCE-SPOT'  # Updated to match canonical spec
        assert mapping.tardis_to_venue['binance-futures'] == 'BINANCE-FUTURES'
        assert mapping.tardis_to_venue['deribit'] == 'DERIBIT'
    
    def test_venue_to_ccxt_mapping(self):
        """Test venue to CCXT exchange mapping."""
        mapping = VenueMapping()
        assert mapping.venue_to_ccxt['BINANCE-SPOT'] == 'binance'  # Updated to match canonical spec
        assert mapping.venue_to_ccxt['BINANCE-FUTURES'] == 'binance'
        assert mapping.venue_to_ccxt['DERIBIT'] == 'deribit'
    
    def test_venue_instrument_type_to_tardis(self):
        """Test venue+instrument_type to Tardis endpoint mapping."""
        mapping = VenueMapping()
        assert mapping.venue_instrument_type_to_tardis[('BINANCE-SPOT', 'SPOT_PAIR')] == 'binance'  # Updated
        assert mapping.venue_instrument_type_to_tardis[('BINANCE-FUTURES', 'PERPETUAL')] == 'binance-futures'


class TestExchangeInstrumentConfig:
    """Test ExchangeInstrumentConfig."""
    
    def test_exchange_instrument_config_creation(self):
        """Test creating ExchangeInstrumentConfig."""
        config = ExchangeInstrumentConfig()
        assert len(config.exchange_instrument_types) > 0
        assert 'BINANCE-FUTURES' in config.exchange_instrument_types
    
    def test_valid_instrument_types(self):
        """Test valid instrument types per exchange."""
        config = ExchangeInstrumentConfig()
        binance_types = config.exchange_instrument_types.get('BINANCE-FUTURES', [])
        assert 'PERPETUAL' in binance_types
        assert 'FUTURE' in binance_types
    
    def test_valid_quote_currencies(self):
        """Test valid quote currencies per exchange."""
        config = ExchangeInstrumentConfig()
        binance_quotes = config.valid_quote_currencies.get('BINANCE-FUTURES', [])
        assert 'USDT' in binance_quotes


class TestDataTypeConfig:
    """Test DataTypeConfig."""
    
    def test_data_type_config_creation(self):
        """Test creating DataTypeConfig."""
        config = DataTypeConfig()
        assert len(config.instrument_data_types) > 0
    
    def test_data_types_by_instrument_type(self):
        """Test data types mapping by instrument type."""
        config = DataTypeConfig()
        perpetual_types = config.instrument_data_types.get('PERPETUAL', [])
        assert 'trades' in perpetual_types
        assert 'book_snapshot_5' in perpetual_types
    
    def test_default_data_types(self):
        """Test default data types."""
        config = DataTypeConfig()
        # SPOT_PAIR should have trades and book_snapshot_5
        spot_types = config.instrument_data_types.get('SPOT_PAIR', [])
        assert 'trades' in spot_types



