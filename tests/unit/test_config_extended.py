"""
Extended unit tests for configuration to increase coverage to 80%+.
"""

import pytest
import os
from instruments_service.config import (
    VenueMapping,
    ExchangeInstrumentConfig,
    DataTypeConfig,
    UnifiedInstrumentConfig,
    DatabentoInstrumentConfig,
)


class TestUnifiedInstrumentConfig:
    """Tests for UnifiedInstrumentConfig methods."""

    def test_get_symbols_for_venue_cme(self):
        """Test getting symbols for CME venue."""
        config = UnifiedInstrumentConfig()
        cme_symbols = config.get_symbols_for_venue("CME")

        assert len(cme_symbols) > 0
        assert "ES.FUT" in cme_symbols  # SP500 future
        assert "GC.FUT" in cme_symbols  # Gold future
        assert "CL.FUT" in cme_symbols  # Crude oil future

    def test_get_symbols_for_venue_case_insensitive(self):
        """Test venue lookup is case-insensitive."""
        config = UnifiedInstrumentConfig()
        cme_upper = config.get_symbols_for_venue("CME")
        cme_lower = config.get_symbols_for_venue("cme")

        assert cme_upper == cme_lower

    def test_get_symbols_for_venue_ice(self):
        """Test getting symbols for ICE venue."""
        config = UnifiedInstrumentConfig()
        ice_symbols = config.get_symbols_for_venue("ICE")

        assert len(ice_symbols) > 0
        assert "BRN.FUT" in ice_symbols  # Brent crude
        assert "G.FUT" in ice_symbols  # Gasoil

    def test_get_symbols_for_venue_nasdaq(self):
        """Test getting symbols for NASDAQ venue - should include ETFs and equities."""
        config = UnifiedInstrumentConfig()
        nasdaq_symbols = config.get_symbols_for_venue("NASDAQ")

        assert len(nasdaq_symbols) > 0
        assert "SPY" in nasdaq_symbols  # ETF
        assert "QQQ" in nasdaq_symbols  # ETF
        assert "AAPL" in nasdaq_symbols  # Equity

    def test_get_symbols_for_venue_empty(self):
        """Test getting symbols for non-existent venue returns empty list."""
        config = UnifiedInstrumentConfig()
        symbols = config.get_symbols_for_venue("NONEXISTENT")

        assert symbols == []

    def test_get_symbols_for_dataset_glbx(self):
        """Test getting symbols for GLBX.MDP3 dataset."""
        config = UnifiedInstrumentConfig()
        glbx_symbols = config.get_symbols_for_dataset("GLBX.MDP3")

        assert len(glbx_symbols) > 0
        assert "ES.FUT" in glbx_symbols
        assert "NQ.FUT" in glbx_symbols
        assert "GC.FUT" in glbx_symbols

    def test_get_symbols_for_dataset_dbeq(self):
        """Test getting symbols for DBEQ.BASIC dataset."""
        config = UnifiedInstrumentConfig()
        dbeq_symbols = config.get_symbols_for_dataset("DBEQ.BASIC")

        assert len(dbeq_symbols) > 0
        assert "SPY" in dbeq_symbols
        assert "AAPL" in dbeq_symbols

    def test_get_symbols_by_type_future(self):
        """Test getting all FUTURE type instruments."""
        config = UnifiedInstrumentConfig()
        futures = config.get_symbols_by_type("FUTURE")

        assert len(futures) > 0
        assert "ES.FUT" in futures
        assert "BRN.FUT" in futures
        # Should not include equities
        assert "AAPL" not in futures

    def test_get_symbols_by_type_equity(self):
        """Test getting all EQUITY type instruments."""
        config = UnifiedInstrumentConfig()
        equities = config.get_symbols_by_type("EQUITY")

        assert len(equities) > 0
        assert "AAPL" in equities
        assert "MSFT" in equities
        # Should not include futures
        assert "ES.FUT" not in equities

    def test_get_symbols_by_type_option(self):
        """Test getting all OPTION type instruments."""
        config = UnifiedInstrumentConfig()
        options = config.get_symbols_by_type("OPTION")

        assert len(options) > 0
        assert "SPY.OPT" in options

    def test_get_symbols_by_type_case_insensitive(self):
        """Test instrument type lookup is case-insensitive."""
        config = UnifiedInstrumentConfig()
        futures_upper = config.get_symbols_by_type("FUTURE")
        futures_lower = config.get_symbols_by_type("future")

        assert futures_upper == futures_lower

    def test_get_dataset_and_stype_future(self):
        """Test getting dataset and stype for a future."""
        config = UnifiedInstrumentConfig()
        result = config.get_dataset_and_stype("ES.FUT")

        assert result is not None
        dataset, stype = result
        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_dataset_and_stype_equity(self):
        """Test getting dataset and stype for an equity."""
        config = UnifiedInstrumentConfig()
        result = config.get_dataset_and_stype("AAPL")

        assert result is not None
        dataset, stype = result
        assert dataset == "DBEQ.BASIC"
        assert stype == "raw_symbol"

    def test_get_dataset_and_stype_nonexistent(self):
        """Test getting dataset for non-existent symbol returns None."""
        config = UnifiedInstrumentConfig()
        result = config.get_dataset_and_stype("NONEXISTENT")

        assert result is None

    def test_get_instrument_by_symbol_only(self):
        """Test getting instrument by symbol without venue filter."""
        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("ES.FUT")

        assert inst is not None
        assert inst.symbol == "ES.FUT"
        assert inst.venue == "CME"
        assert inst.instrument_type == "FUTURE"

    def test_get_instrument_with_venue_filter(self):
        """Test getting instrument with venue filter."""
        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("ES.FUT", venue="CME")

        assert inst is not None
        assert inst.venue == "CME"

    def test_get_instrument_with_wrong_venue_filter(self):
        """Test getting instrument with wrong venue returns None."""
        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("ES.FUT", venue="NASDAQ")

        assert inst is None

    def test_get_instrument_nonexistent(self):
        """Test getting non-existent instrument returns None."""
        config = UnifiedInstrumentConfig()
        inst = config.get_instrument("NONEXISTENT")

        assert inst is None

    def test_get_human_readable_name_standard(self):
        """Test converting exchange codes to human-readable names."""
        config = UnifiedInstrumentConfig()

        assert config.get_human_readable_name("ES") == "SP500"
        assert config.get_human_readable_name("GC") == "GOLD"
        assert config.get_human_readable_name("CL") == "CRUDE"
        assert config.get_human_readable_name("BRN") == "BRENT"

    def test_get_human_readable_name_micro_contracts(self):
        """Test converting micro contract codes (M prefix) to readable names."""
        config = UnifiedInstrumentConfig()

        # Micro contracts should resolve to same base asset
        assert config.get_human_readable_name("MES") == "SP500"
        assert config.get_human_readable_name("MGC") == "GOLD"
        assert config.get_human_readable_name("MCL") == "CRUDE"

    def test_get_human_readable_name_unknown(self):
        """Test unknown exchange codes return the code itself."""
        config = UnifiedInstrumentConfig()

        assert config.get_human_readable_name("UNKNOWN") == "UNKNOWN"

    def test_get_all_instruments_includes_sp500(self):
        """Test get_all_instruments includes dynamically generated S&P 500 stocks."""
        config = UnifiedInstrumentConfig()
        all_insts = config.get_all_instruments()

        # Should include base instruments
        assert any(inst.symbol == "ES.FUT" for inst in all_insts)

        # Should include S&P 500 stocks (500+ stocks)
        equity_count = sum(1 for inst in all_insts if inst.instrument_type == "EQUITY")
        assert equity_count > 500  # S&P 500 has 503 stocks

    def test_get_all_instruments_no_duplicates(self):
        """Test get_all_instruments doesn't create duplicates."""
        config = UnifiedInstrumentConfig()
        all_insts = config.get_all_instruments()

        # Check that AAPL (defined in base list) isn't duplicated
        aapl_count = sum(1 for inst in all_insts if inst.symbol == "AAPL")
        assert aapl_count == 1


class TestDatabentoInstrumentConfig:
    """Tests for DatabentoInstrumentConfig (legacy wrapper)."""

    def test_extended_symbols_property(self):
        """Test extended_symbols returns all base instrument symbols."""
        config = DatabentoInstrumentConfig()
        symbols = config.extended_symbols

        assert len(symbols) > 0
        assert "ES.FUT" in symbols
        assert "SPY" in symbols

    def test_sp500_stocks_property(self):
        """Test sp500_stocks returns EQUITY type symbols."""
        config = DatabentoInstrumentConfig()
        stocks = config.sp500_stocks

        assert len(stocks) > 0
        # Should include equities
        assert "AAPL" in stocks
        assert "MSFT" in stocks
        # Should not include futures
        assert "ES.FUT" not in stocks

    def test_get_dataset_and_stype_future(self):
        """Test get_dataset_and_stype for future."""
        config = DatabentoInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("ES.FUT")

        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_dataset_and_stype_equity(self):
        """Test get_dataset_and_stype for equity."""
        config = DatabentoInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("AAPL")

        assert dataset == "DBEQ.BASIC"
        assert stype == "raw_symbol"

    def test_get_dataset_and_stype_fallback_future(self):
        """Test fallback logic for unknown future symbol."""
        config = DatabentoInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("UNKNOWN.FUT")

        # Should fallback to future defaults
        assert dataset == "GLBX.MDP3"
        assert stype == "parent"

    def test_get_dataset_and_stype_fallback_default(self):
        """Test fallback logic for completely unknown symbol."""
        config = DatabentoInstrumentConfig()
        dataset, stype = config.get_dataset_and_stype("UNKNOWN")

        # Should fallback to equity defaults
        assert dataset == "DBEQ.BASIC"
        assert stype == "raw_symbol"

    def test_get_human_readable_name(self):
        """Test get_human_readable_name delegates to unified config."""
        config = DatabentoInstrumentConfig()

        assert config.get_human_readable_name("ES") == "SP500"
        assert config.get_human_readable_name("GC") == "GOLD"

    def test_get_symbols_for_venue(self):
        """Test get_symbols_for_venue delegates to unified config."""
        config = DatabentoInstrumentConfig()
        cme_symbols = config.get_symbols_for_venue("CME")

        assert len(cme_symbols) > 0
        assert "ES.FUT" in cme_symbols


class TestVenueMappingExtended:
    """Extended tests for VenueMapping."""

    def test_all_exchanges_property(self):
        """Test all_exchanges property combines all exchange types."""
        mapping = VenueMapping()
        all_exch = mapping.all_exchanges

        # Should include Tardis exchanges
        assert "binance" in all_exch
        assert "deribit" in all_exch

        # Should include Databento venues
        assert "CME" in all_exch
        assert "NASDAQ" in all_exch

        # Should include DeFi venues
        assert "HYPERLIQUID" in all_exch
        assert "UNISWAPV3-ETH" in all_exch

    def test_is_databento_venue_true(self):
        """Test is_databento_venue returns True for Databento venues."""
        mapping = VenueMapping()

        assert mapping.is_databento_venue("CME") is True
        assert mapping.is_databento_venue("NASDAQ") is True
        assert mapping.is_databento_venue("NYSE") is True
        assert mapping.is_databento_venue("ICE") is True
        assert mapping.is_databento_venue("CBOE") is True

    def test_is_databento_venue_false(self):
        """Test is_databento_venue returns False for non-Databento venues."""
        mapping = VenueMapping()

        assert mapping.is_databento_venue("BINANCE-SPOT") is False
        assert mapping.is_databento_venue("HYPERLIQUID") is False
        assert mapping.is_databento_venue("UNKNOWN") is False

    def test_is_tardis_exchange_true(self):
        """Test is_tardis_exchange returns True for Tardis exchanges."""
        mapping = VenueMapping()

        assert mapping.is_tardis_exchange("binance") is True
        assert mapping.is_tardis_exchange("binance-futures") is True
        assert mapping.is_tardis_exchange("deribit") is True
        assert mapping.is_tardis_exchange("bybit") is True

    def test_is_tardis_exchange_false(self):
        """Test is_tardis_exchange returns False for non-Tardis exchanges."""
        mapping = VenueMapping()

        assert mapping.is_tardis_exchange("CME") is False
        assert mapping.is_tardis_exchange("HYPERLIQUID") is False
        assert mapping.is_tardis_exchange("unknown") is False

    def test_is_defi_venue_true(self):
        """Test is_defi_venue returns True for DeFi venues."""
        mapping = VenueMapping()

        assert mapping.is_defi_venue("HYPERLIQUID") is True
        assert mapping.is_defi_venue("UNISWAPV3-ETH") is True
        assert mapping.is_defi_venue("AAVE_V3_ETH") is True
        assert mapping.is_defi_venue("ASTER") is True

    def test_is_defi_venue_false(self):
        """Test is_defi_venue returns False for non-DeFi venues."""
        mapping = VenueMapping()

        assert mapping.is_defi_venue("CME") is False
        assert mapping.is_defi_venue("binance") is False
        assert mapping.is_defi_venue("UNKNOWN") is False

    def test_get_defi_mvp_tokens_default(self):
        """Test get_defi_mvp_tokens returns default tokens."""
        mapping = VenueMapping()
        tokens = mapping.get_defi_mvp_tokens()

        assert len(tokens) > 0
        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "USDT" in tokens
        assert "USDC" in tokens
        assert "BTC" in tokens

    def test_get_databento_exchange_id_valid(self):
        """Test get_databento_exchange_id for valid venues."""
        mapping = VenueMapping()

        assert mapping.get_databento_exchange_id("CME") == "GLBX.MDP3"
        assert mapping.get_databento_exchange_id("NASDAQ") == "DBEQ.BASIC"
        assert mapping.get_databento_exchange_id("ICE") == "IFEU.IMPACT"

    def test_get_databento_exchange_id_invalid(self):
        """Test get_databento_exchange_id returns None for invalid venue."""
        mapping = VenueMapping()

        assert mapping.get_databento_exchange_id("UNKNOWN") is None
        assert mapping.get_databento_exchange_id("binance") is None

    def test_get_data_provider_tardis(self):
        """Test get_data_provider returns 'tardis' for Tardis venues."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("BINANCE-SPOT") == "tardis"
        assert mapping.get_data_provider("DERIBIT") == "tardis"
        assert mapping.get_data_provider("OKX") == "tardis"

    def test_get_data_provider_databento(self):
        """Test get_data_provider returns 'databento' for Databento venues."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("CME") == "databento"
        assert mapping.get_data_provider("NASDAQ") == "databento"
        assert mapping.get_data_provider("ICE") == "databento"

    def test_get_data_provider_hyperliquid(self):
        """Test get_data_provider returns 'hyperliquid_api' for Hyperliquid."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("HYPERLIQUID") == "hyperliquid_api"

    def test_get_data_provider_the_graph(self):
        """Test get_data_provider returns 'the_graph' for The Graph venues."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("UNISWAPV3-ETH") == "the_graph"
        assert mapping.get_data_provider("CURVE-ETH") == "the_graph"

    def test_get_data_provider_protocol_sdk(self):
        """Test get_data_provider returns 'protocol_sdk' for protocol SDK venues."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("AAVE_V3_ETH") == "protocol_sdk"
        assert mapping.get_data_provider("MORPHO-ETHEREUM") == "protocol_sdk"

    def test_get_data_provider_unknown(self):
        """Test get_data_provider returns None for unknown venue."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("UNKNOWN") is None

    def test_all_tardis_exchanges_complete(self):
        """Test all Tardis exchanges are included."""
        mapping = VenueMapping()
        expected_exchanges = [
            "binance",
            "binance-futures",
            "deribit",
            "bybit",
            "bybit-spot",
            "okex",
            "okex-futures",
            "okex-swap",
        ]

        for exchange in expected_exchanges:
            assert exchange in mapping.all_tardis_exchanges

    def test_venue_to_ccxt_all_venues(self):
        """Test venue to CCXT mapping for all venues."""
        mapping = VenueMapping()

        test_cases = {
            "BINANCE-SPOT": "binance",
            "BINANCE-FUTURES": "binance",
            "DERIBIT": "deribit",
            "BYBIT": "bybit",
            "OKX": "okx",
        }

        for venue, expected_ccxt in test_cases.items():
            assert mapping.venue_to_ccxt.get(venue) == expected_ccxt

    def test_tardis_to_venue_all_exchanges(self):
        """Test Tardis to venue mapping for all exchanges."""
        mapping = VenueMapping()

        test_cases = {
            "binance": "BINANCE-SPOT",
            "binance-futures": "BINANCE-FUTURES",
            "deribit": "DERIBIT",
            "bybit": "BYBIT",
            "bybit-spot": "BYBIT",
            "okex": "OKX",
            "okex-futures": "OKX",
            "okex-swap": "OKX",
        }

        for tardis_exchange, expected_venue in test_cases.items():
            assert mapping.tardis_to_venue.get(tardis_exchange) == expected_venue

    def test_venue_instrument_type_to_tardis_all_combinations(self):
        """Test venue+instrument_type to Tardis mapping for key combinations."""
        mapping = VenueMapping()

        test_cases = [
            (("BINANCE-SPOT", "SPOT_PAIR"), "binance"),
            (("BINANCE-FUTURES", "PERPETUAL"), "binance-futures"),
            (("BINANCE-FUTURES", "FUTURE"), "binance-futures"),
            (("DERIBIT", "PERPETUAL"), "deribit"),
            (("DERIBIT", "FUTURE"), "deribit"),
            (("DERIBIT", "OPTION"), "deribit"),
            (("OKX", "SPOT_PAIR"), "okex"),
            (("OKX", "PERPETUAL"), "okex-swap"),
            (("OKX", "FUTURE"), "okex-futures"),
        ]

        for (venue, inst_type), expected_tardis in test_cases:
            assert (
                mapping.venue_instrument_type_to_tardis.get((venue, inst_type)) == expected_tardis
            )


class TestExchangeInstrumentConfigExtended:
    """Extended tests for ExchangeInstrumentConfig."""

    def test_exchange_instrument_types_all_exchanges(self):
        """Test instrument types for all exchanges."""
        config = ExchangeInstrumentConfig()

        test_cases = {
            "BINANCE-SPOT": ["SPOT_PAIR"],
            "BINANCE-FUTURES": ["PERPETUAL", "FUTURE"],
            "DERIBIT": ["PERPETUAL", "FUTURE", "OPTION"],
            "BYBIT": ["SPOT_PAIR", "PERPETUAL"],
            "OKX": ["SPOT_PAIR", "PERPETUAL", "FUTURE"],
        }

        for venue, expected_types in test_cases.items():
            assert config.exchange_instrument_types.get(venue) == expected_types

    def test_valid_quote_currencies_all_exchanges(self):
        """Test valid quote currencies for all exchanges."""
        config = ExchangeInstrumentConfig()

        test_cases = {
            "BINANCE-SPOT": ["USDT"],
            "BINANCE-FUTURES": ["USDT"],
            "DERIBIT": ["USD", "USDC"],
            "BYBIT": ["USDT"],
            "OKX": ["USDT"],
        }

        for venue, expected_quotes in test_cases.items():
            assert config.valid_quote_currencies.get(venue) == expected_quotes

    def test_derivative_exchanges(self):
        """Test derivative exchanges list."""
        config = ExchangeInstrumentConfig()

        expected_derivatives = ["DERIBIT", "BINANCE-FUTURES", "OKX", "BYBIT"]
        for exchange in expected_derivatives:
            assert exchange in config.derivative_exchanges


class TestDataTypeConfigExtended:
    """Extended tests for DataTypeConfig."""

    def test_instrument_data_types_all_types(self):
        """Test data types for all instrument types."""
        config = DataTypeConfig()

        test_cases = {
            "SPOT_PAIR": ["trades", "book_snapshot_5"],
            "PERPETUAL": [
                "trades",
                "book_snapshot_5",
                "derivative_ticker",
                "liquidations",
            ],
            "FUTURE": [
                "trades",
                "book_snapshot_5",
                "derivative_ticker",
                "liquidations",
            ],
            "OPTION": ["options_chain"],
        }

        for inst_type, expected_types in test_cases.items():
            assert config.instrument_data_types.get(inst_type) == expected_types

    def test_default_data_types(self):
        """Test default data types list."""
        config = DataTypeConfig()

        expected_defaults = [
            "trades",
            "book_snapshot_5",
            "derivative_ticker",
            "liquidations",
            "options_chain",
        ]
        for dt in expected_defaults:
            assert dt in config.default_data_types

    def test_excluded_instrument_types(self):
        """Test excluded instrument types."""
        config = DataTypeConfig()

        assert "combo" in config.excluded_instrument_types

    def test_excluded_deribit_strategies(self):
        """Test excluded Deribit strategies."""
        config = DataTypeConfig()

        expected_strategies = ["PS-", "STRG-", "CBUT-", "CCOND-", "PDIAG-", "PBUT-"]
        for strategy in expected_strategies:
            assert strategy in config.excluded_deribit_strategies
