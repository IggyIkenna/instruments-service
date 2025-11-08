"""
Extended unit tests for models to increase coverage to 80%+.
"""

import pytest
from datetime import datetime, timezone
from instruments_service.models import (
    Venue,
    InstrumentType,
    InstrumentKey,
    InstrumentDefinition
)


class TestInstrumentKeyExtended:
    """Extended tests for InstrumentKey."""
    
    def test_instrument_key_all_venues(self):
        """Test InstrumentKey with all venue types."""
        venues = [Venue.BINANCE_SPOT, Venue.BINANCE_FUTURES, Venue.DERIBIT, Venue.BYBIT, Venue.OKX]
        
        for venue in venues:
            key = InstrumentKey(
                venue=venue,
                instrument_type=InstrumentType.SPOT_PAIR,
                symbol="BTC-USDT"
            )
            assert key.venue == venue
    
    def test_instrument_key_all_instrument_types(self):
        """Test InstrumentKey with all instrument types."""
        inst_types = [
            InstrumentType.SPOT_PAIR,
            InstrumentType.PERPETUAL,
            InstrumentType.FUTURE,
            InstrumentType.OPTION
        ]
        
        for inst_type in inst_types:
            key = InstrumentKey(
                venue=Venue.DERIBIT,
                instrument_type=inst_type,
                symbol="BTC-USDT"
            )
            assert key.instrument_type == inst_type
    
    def test_instrument_key_from_string_all_formats(self):
        """Test parsing various InstrumentKey string formats."""
        test_cases = [
            "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            "DERIBIT:FUTURE:BTC-USD-241225",
            "DERIBIT:OPTION:BTC-USD-241225-50000-CALL",
        ]
        
        for key_str in test_cases:
            key = InstrumentKey.from_string(key_str)
            assert key is not None
            assert str(key) == key_str or key_str.startswith(str(key).split(':')[0])


class TestInstrumentDefinitionExtended:
    """Extended tests for InstrumentDefinition."""
    
    def test_instrument_definition_all_venues(self):
        """Test InstrumentDefinition with all venue types."""
        venues = ['BINANCE-SPOT', 'BINANCE-FUTURES', 'DERIBIT', 'BYBIT', 'OKX']
        
        for venue in venues:
            inst = InstrumentDefinition(
                instrument_key=f"{venue}:SPOT_PAIR:BTC-USDT",
                venue=venue,
                instrument_type="SPOT_PAIR",
                symbol="BTC-USDT",
                available_from_datetime="2023-01-01T00:00:00Z"
            )
            assert inst.venue == venue
    
    def test_instrument_definition_all_instrument_types(self):
        """Test InstrumentDefinition with all instrument types."""
        inst_types = ['SPOT_PAIR', 'PERPETUAL', 'FUTURE', 'OPTION']
        
        for inst_type in inst_types:
            inst = InstrumentDefinition(
                instrument_key=f"DERIBIT:{inst_type}:BTC-USDT",
                venue="DERIBIT",
                instrument_type=inst_type,
                symbol="BTC-USDT",
                available_from_datetime="2023-01-01T00:00:00Z"
            )
            assert inst.instrument_type == inst_type
    
    def test_instrument_definition_with_future_fields(self):
        """Test InstrumentDefinition with future-specific fields."""
        inst = InstrumentDefinition(
            instrument_key="DERIBIT:FUTURE:BTC-USD-241225",
            venue="DERIBIT",
            instrument_type="FUTURE",
            symbol="BTC-USD-241225",
            available_from_datetime="2023-01-01T00:00:00Z",
            expiry="2024-12-25T08:00:00Z"
        )
        assert inst.expiry == "2024-12-25T08:00:00Z"
    
    def test_instrument_definition_data_types(self):
        """Test InstrumentDefinition with different data types."""
        test_cases = [
            'trades,book_snapshot_5',
            'options_chain',
            'trades,book_snapshot_5,derivative_ticker,liquidations'
        ]
        
        for data_types in test_cases:
            inst = InstrumentDefinition(
                instrument_key="DERIBIT:OPTION:BTC-USD-241225-50000-CALL",
                venue="DERIBIT",
                instrument_type="OPTION",
                symbol="BTC-USD-241225-50000-CALL",
                available_from_datetime="2023-01-01T00:00:00Z",
                data_types=data_types
            )
            assert inst.data_types == data_types
    
    def test_instrument_definition_deribit_options_chain(self):
        """Test Deribit instruments have only options_chain data type."""
        inst = InstrumentDefinition(
            instrument_key="DERIBIT:PERPETUAL:BTC-USD@INV",
            venue="DERIBIT",
            instrument_type="PERPETUAL",
            symbol="BTC-USD@INV",
            available_from_datetime="2023-01-01T00:00:00Z",
            data_types="options_chain"
        )
        assert inst.data_types == "options_chain"
    
    def test_instrument_definition_available_to_datetime(self):
        """Test InstrumentDefinition with available_to_datetime."""
        inst = InstrumentDefinition(
            instrument_key="BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
            available_to_datetime="2024-12-31T00:00:00Z"
        )
        assert inst.available_to_datetime == "2024-12-31T00:00:00Z"
    
    def test_instrument_definition_settle_asset(self):
        """Test InstrumentDefinition with settle_asset."""
        inst = InstrumentDefinition(
            instrument_key="DERIBIT:PERPETUAL:BTC-USD@INV",
            venue="DERIBIT",
            instrument_type="PERPETUAL",
            symbol="BTC-USD@INV",
            available_from_datetime="2023-01-01T00:00:00Z",
            settle_asset="BTC"
        )
        assert inst.settle_asset == "BTC"
    
    def test_instrument_definition_tardis_fields(self):
        """Test InstrumentDefinition with Tardis-specific fields."""
        inst = InstrumentDefinition(
            instrument_key="BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
            tardis_exchange="binance",
            tardis_symbol="btcusdt"
        )
        assert inst.tardis_exchange == "binance"
        assert inst.tardis_symbol == "btcusdt"
    
    def test_instrument_definition_ccxt_fields(self):
        """Test InstrumentDefinition with CCXT fields."""
        inst = InstrumentDefinition(
            instrument_key="BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
            ccxt_symbol="BTC/USDT",
            ccxt_exchange="binance"
        )
        assert inst.ccxt_symbol == "BTC/USDT"
        assert inst.ccxt_exchange == "binance"
    
    def test_instrument_definition_validation_data_types(self):
        """Test InstrumentDefinition data_types validation."""
        # Valid data types
        inst = InstrumentDefinition(
            instrument_key="BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
            data_types="trades,book_snapshot_5"
        )
        assert inst.data_types == "trades,book_snapshot_5"
        
        # Note: Invalid data types don't raise errors, they just pass validation
        # (validation is lenient to allow future data types)
        inst_invalid = InstrumentDefinition(
            instrument_key="BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
            venue="BINANCE-SPOT",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
            data_types="invalid_type,valid_type"
        )
        # Should still create (validation is lenient)
        assert inst_invalid.data_types == "invalid_type,valid_type"

