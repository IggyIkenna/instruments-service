"""
Unit tests for configuration classes.
"""

import pytest
from instruments_service.config import (
    VenueMapping,
    ExchangeInstrumentConfig,
    DataTypeConfig,
)


class TestVenueMapping:
    """Test VenueMapping configuration."""

    def test_venue_mapping_creation(self):
        """Test creating VenueMapping."""
        mapping = VenueMapping()
        assert len(mapping.all_tardis_exchanges) > 0
        assert "binance" in mapping.all_tardis_exchanges
        assert "deribit" in mapping.all_tardis_exchanges

    def test_tardis_to_venue_mapping(self):
        """Test Tardis exchange to venue mapping."""
        mapping = VenueMapping()
        assert (
            mapping.tardis_to_venue["binance"] == "BINANCE-SPOT"
        )  # Updated to match canonical spec
        assert mapping.tardis_to_venue["binance-futures"] == "BINANCE-FUTURES"
        assert mapping.tardis_to_venue["deribit"] == "DERIBIT"

    def test_venue_to_ccxt_mapping(self):
        """Test venue to CCXT exchange mapping."""
        mapping = VenueMapping()
        assert (
            mapping.venue_to_ccxt["BINANCE-SPOT"] == "binance"
        )  # Updated to match canonical spec
        assert mapping.venue_to_ccxt["BINANCE-FUTURES"] == "binance"
        assert mapping.venue_to_ccxt["DERIBIT"] == "deribit"

    def test_venue_instrument_type_to_tardis(self):
        """Test venue+instrument_type to Tardis endpoint mapping."""
        mapping = VenueMapping()
        assert (
            mapping.venue_instrument_type_to_tardis[("BINANCE-SPOT", "SPOT_PAIR")]
            == "binance"
        )  # Updated
        assert (
            mapping.venue_instrument_type_to_tardis[("BINANCE-FUTURES", "PERPETUAL")]
            == "binance-futures"
        )


class TestExchangeInstrumentConfig:
    """Test ExchangeInstrumentConfig."""

    def test_exchange_instrument_config_creation(self):
        """Test creating ExchangeInstrumentConfig."""
        config = ExchangeInstrumentConfig()
        assert len(config.exchange_instrument_types) > 0
        assert "BINANCE-FUTURES" in config.exchange_instrument_types

    def test_valid_instrument_types(self):
        """Test valid instrument types per exchange."""
        config = ExchangeInstrumentConfig()
        binance_types = config.exchange_instrument_types.get("BINANCE-FUTURES", [])
        assert "PERPETUAL" in binance_types
        assert "FUTURE" in binance_types

    def test_valid_quote_currencies(self):
        """Test valid quote currencies per exchange."""
        config = ExchangeInstrumentConfig()
        binance_quotes = config.valid_quote_currencies.get("BINANCE-FUTURES", [])
        assert "USDT" in binance_quotes


class TestDataTypeConfig:
    """Test DataTypeConfig."""

    def test_data_type_config_creation(self):
        """Test creating DataTypeConfig."""
        config = DataTypeConfig()
        assert len(config.instrument_data_types) > 0

    def test_data_types_by_instrument_type(self):
        """Test data types mapping by instrument type."""
        config = DataTypeConfig()
        perpetual_types = config.instrument_data_types.get("PERPETUAL", [])
        assert "trades" in perpetual_types
        assert "book_snapshot_5" in perpetual_types

    def test_default_data_types(self):
        """Test default data types."""
        config = DataTypeConfig()
        # SPOT_PAIR should have trades and book_snapshot_5
        spot_types = config.instrument_data_types.get("SPOT_PAIR", [])
        assert "trades" in spot_types


class TestUnifiedInstrumentConfig:
    """Test UnifiedInstrumentConfig."""

    def test_unified_config_creation(self):
        """Test creating UnifiedInstrumentConfig."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        assert len(config.instruments) > 0

    def test_get_symbols_by_type(self):
        """Test getting symbols by instrument type."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        futures = config.get_symbols_by_type("FUTURE")
        assert isinstance(futures, list)
        assert len(futures) > 0

    def test_get_symbols_for_venue(self):
        """Test getting symbols for a venue."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        cme_symbols = config.get_symbols_for_venue("CME")
        assert isinstance(cme_symbols, list)
        assert len(cme_symbols) > 0

    def test_get_dataset_and_stype(self):
        """Test getting dataset and stype for a symbol."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("ES.FUT")
        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_human_readable_name(self):
        """Test getting human-readable name for exchange code."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        name = config.get_human_readable_name("ES")
        assert isinstance(name, str)
        assert len(name) > 0

    def test_get_all_instruments(self):
        """Test getting all instruments."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        instruments = config.get_all_instruments()
        assert isinstance(instruments, list)
        assert len(instruments) > 0


class TestDatabentoInstrumentConfig:
    """Test DatabentoInstrumentConfig."""

    def test_databento_config_creation(self):
        """Test creating DatabentoInstrumentConfig."""
        from instruments_service.config import DatabentoInstrumentConfig

        config = DatabentoInstrumentConfig()
        assert len(config.extended_symbols) > 0

    def test_sp500_stocks(self):
        """Test getting S&P 500 stocks."""
        from instruments_service.config import DatabentoInstrumentConfig

        config = DatabentoInstrumentConfig()
        stocks = config.sp500_stocks
        assert isinstance(stocks, list)

    def test_get_dataset_and_stype(self):
        """Test getting dataset and stype."""
        from instruments_service.config import DatabentoInstrumentConfig

        config = DatabentoInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("ES.FUT")
        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_symbols_for_venue(self):
        """Test getting symbols for venue."""
        from instruments_service.config import DatabentoInstrumentConfig

        config = DatabentoInstrumentConfig()
        symbols = config.get_symbols_for_venue("CME")
        assert isinstance(symbols, list)

    def test_get_symbols_for_dataset(self):
        """Test getting symbols for dataset."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        symbols = config.get_symbols_for_dataset("GLBX.MDP3")
        assert isinstance(symbols, list)
        assert len(symbols) > 0

    def test_get_instrument(self):
        """Test getting instrument by symbol."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("ES.FUT")
        assert inst is not None
        assert inst.symbol == "ES.FUT"
        assert inst.venue == "CME"

    def test_get_instrument_with_venue(self):
        """Test getting instrument by symbol with venue filter."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("ES.FUT", venue="CME")
        assert inst is not None
        assert inst.venue == "CME"

    def test_get_instrument_not_found(self):
        """Test getting instrument that doesn't exist."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("NONEXISTENT.FUT")
        assert inst is None

    def test_get_human_readable_name_micro(self):
        """Test getting human-readable name for micro version."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        name = config.get_human_readable_name("MES")
        assert isinstance(name, str)

    def test_get_human_readable_name_fallback(self):
        """Test getting human-readable name fallback."""
        from instruments_service.config import UnifiedInstrumentConfig

        config = UnifiedInstrumentConfig()
        name = config.get_human_readable_name("UNKNOWN")
        assert name == "UNKNOWN"
