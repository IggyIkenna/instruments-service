"""
Extended unit tests for configuration to increase coverage to 80%+.
"""

from unified_config_interface import (
    DataTypeConfig,
    ExchangeInstrumentConfig,
    VenueMapping,
)

from instruments_service.config import UnifiedInstrumentConfig


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
        """Test getting symbols for NASDAQ venue - should include equities."""
        config = UnifiedInstrumentConfig()
        nasdaq_symbols = config.get_symbols_for_venue("NASDAQ")

        assert len(nasdaq_symbols) > 0
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
        # Verify options have .OPT suffix
        assert all(opt.endswith(".OPT") for opt in options)

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

        # Should include S&P 500 stocks (500 stocks)
        equity_count = sum(1 for inst in all_insts if inst.instrument_type == "EQUITY")
        assert equity_count >= 500  # S&P 500 has ~500 stocks

    def test_get_all_instruments_no_duplicates(self):
        """Test get_all_instruments doesn't create duplicates."""
        config = UnifiedInstrumentConfig()
        all_insts = config.get_all_instruments()

        # Check that AAPL (defined in base list) isn't duplicated
        aapl_count = sum(1 for inst in all_insts if inst.symbol == "AAPL")
        assert aapl_count == 1


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

        # These are actual DeFi venues (DEXes, lending protocols)
        assert mapping.is_defi_venue("UNISWAPV3-ETH") is True
        assert mapping.is_defi_venue("AAVE_V3_ETH") is True
        # Note: HYPERLIQUID and ASTER are CEFI onchain CLOBs, not DeFi
        # They're in all_cefi_onchain_clob_venues, not all_defi_venues

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

    def test_get_defi_mvp_tokens_from_env(self, monkeypatch):
        """Test get_defi_mvp_tokens reads from environment variable."""
        mapping = VenueMapping()

        # Set environment variable
        monkeypatch.setenv("DEFI_MVP_TOKENS", "ETH,BTC,USDT")

        tokens = mapping.get_defi_mvp_tokens()

        assert tokens == ["ETH", "BTC", "USDT"]

    def test_get_defi_mvp_tokens_from_env_case_normalization(self, monkeypatch):
        """Test get_defi_mvp_tokens normalizes case from env."""
        mapping = VenueMapping()

        # Set environment variable with mixed case
        monkeypatch.setenv("DEFI_MVP_TOKENS", "eth,btc,usdt")

        tokens = mapping.get_defi_mvp_tokens()

        # Should be uppercase
        assert tokens == ["ETH", "BTC", "USDT"]

    def test_get_databento_exchange_id_valid(self):
        """Test get_databento_exchange_id for valid venues."""
        mapping = VenueMapping()

        assert mapping.get_databento_exchange_id("CME") == "GLBX.MDP3"
        assert mapping.get_databento_exchange_id("NASDAQ") == "DBEQ.BASIC"
        assert mapping.get_databento_exchange_id("ICE") == "IFUS.IMPACT"

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
        assert mapping.get_data_provider("UNISWAPV2-ETH") == "the_graph"
        # Note: CURVE-ETH is not currently in venue_to_data_provider mapping

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
            "upbit",
            "coinbase",
        ]

        for exchange in expected_exchanges:
            assert exchange in mapping.all_tardis_exchanges

    def test_is_tardis_exchange_upbit_coinbase(self):
        """Test is_tardis_exchange returns True for Upbit and Coinbase."""
        mapping = VenueMapping()

        assert mapping.is_tardis_exchange("upbit") is True
        assert mapping.is_tardis_exchange("coinbase") is True

    def test_tardis_to_venue_upbit_coinbase(self):
        """Test Tardis to venue mapping for Upbit and Coinbase."""
        mapping = VenueMapping()

        assert mapping.tardis_to_venue.get("upbit") == "UPBIT"
        assert mapping.tardis_to_venue.get("coinbase") == "COINBASE"

    def test_venue_to_ccxt_upbit_coinbase(self):
        """Test venue to CCXT mapping for Upbit and Coinbase."""
        mapping = VenueMapping()

        assert mapping.venue_to_ccxt.get("UPBIT") == "upbit"
        assert mapping.venue_to_ccxt.get("COINBASE") == "coinbase"

    def test_venue_instrument_type_to_tardis_upbit_coinbase(self):
        """Test venue+instrument_type to Tardis mapping for Upbit and Coinbase."""
        mapping = VenueMapping()

        # Upbit (spot only)
        assert mapping.venue_instrument_type_to_tardis.get(("UPBIT", "SPOT_PAIR")) == "upbit"

        # Coinbase (spot only)
        assert mapping.venue_instrument_type_to_tardis.get(("COINBASE", "SPOT_PAIR")) == "coinbase"

    def test_tardis_exchange_instrument_types_upbit_coinbase(self):
        """Test Tardis exchange to instrument types for Upbit and Coinbase."""
        mapping = VenueMapping()

        # Both should be spot only
        assert mapping.tardis_exchange_instrument_types.get("upbit") == ["SPOT_PAIR"]
        assert mapping.tardis_exchange_instrument_types.get("coinbase") == ["SPOT_PAIR"]

    def test_spot_mvp_filtered_venues(self):
        """Test spot_mvp_filtered_venues includes Upbit and Coinbase."""
        mapping = VenueMapping()

        assert "UPBIT" in mapping.spot_mvp_filtered_venues
        assert "COINBASE" in mapping.spot_mvp_filtered_venues
        # Should not include other Tardis venues
        assert "BINANCE-SPOT" not in mapping.spot_mvp_filtered_venues
        assert "OKX" not in mapping.spot_mvp_filtered_venues

    def test_get_data_provider_upbit_coinbase(self):
        """Test get_data_provider returns 'tardis' for Upbit and Coinbase."""
        mapping = VenueMapping()

        assert mapping.get_data_provider("UPBIT") == "tardis"
        assert mapping.get_data_provider("COINBASE") == "tardis"

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
            assert mapping.venue_instrument_type_to_tardis.get((venue, inst_type)) == expected_tardis

    def test_get_venue_to_tardis_exchanges(self):
        """Test get_venue_to_tardis_exchanges returns correct reverse mapping."""
        mapping = VenueMapping()
        venue_to_exchanges = mapping.get_venue_to_tardis_exchanges()

        # UPBIT should map to ['upbit']
        assert "UPBIT" in venue_to_exchanges
        assert "upbit" in venue_to_exchanges["UPBIT"]

        # COINBASE should map to ['coinbase']
        assert "COINBASE" in venue_to_exchanges
        assert "coinbase" in venue_to_exchanges["COINBASE"]

        # OKX should map to multiple exchanges
        assert "OKX" in venue_to_exchanges
        assert set(venue_to_exchanges["OKX"]) == {"okex", "okex-futures", "okex-swap"}

    def test_get_tardis_exchange_for_venue_simple(self):
        """Test get_tardis_exchange_for_venue for simple venues."""
        mapping = VenueMapping()

        # UPBIT -> upbit
        assert mapping.get_tardis_exchange_for_venue("UPBIT") == "upbit"

        # COINBASE -> coinbase
        assert mapping.get_tardis_exchange_for_venue("COINBASE") == "coinbase"

        # Unknown venue -> None
        assert mapping.get_tardis_exchange_for_venue("UNKNOWN") is None

    def test_convert_to_tardis_exchange_canonical_venues(self):
        """Test convert_to_tardis_exchange converts canonical venue names."""
        mapping = VenueMapping()

        # Canonical venue names (uppercase) -> Tardis names (lowercase)
        assert mapping.convert_to_tardis_exchange("UPBIT") == "upbit"
        assert mapping.convert_to_tardis_exchange("COINBASE") == "coinbase"
        assert mapping.convert_to_tardis_exchange("BINANCE-SPOT") == "binance"
        assert mapping.convert_to_tardis_exchange("DERIBIT") == "deribit"

    def test_convert_to_tardis_exchange_already_lowercase(self):
        """Test convert_to_tardis_exchange handles already lowercase names."""
        mapping = VenueMapping()

        # Already lowercase Tardis names should pass through
        assert mapping.convert_to_tardis_exchange("upbit") == "upbit"
        assert mapping.convert_to_tardis_exchange("coinbase") == "coinbase"
        assert mapping.convert_to_tardis_exchange("binance") == "binance"
        assert mapping.convert_to_tardis_exchange("deribit") == "deribit"

    def test_convert_to_tardis_exchange_mixed_case(self):
        """Test convert_to_tardis_exchange handles mixed case input."""
        mapping = VenueMapping()

        # Mixed case should be normalized
        assert mapping.convert_to_tardis_exchange("Upbit") == "upbit"
        assert mapping.convert_to_tardis_exchange("Coinbase") == "coinbase"
        assert mapping.convert_to_tardis_exchange("BINANCE-futures") == "binance-futures"


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

    def test_exchange_instrument_types_upbit_coinbase(self):
        """Test instrument types for Upbit and Coinbase (spot only)."""
        config = ExchangeInstrumentConfig()

        # Both should be spot only
        assert config.exchange_instrument_types.get("UPBIT") == ["SPOT_PAIR"]
        assert config.exchange_instrument_types.get("COINBASE") == ["SPOT_PAIR"]

    def test_valid_quote_currencies_upbit_coinbase(self):
        """Test valid quote currencies for Upbit and Coinbase."""
        config = ExchangeInstrumentConfig()

        # Upbit uses KRW (Korean Won) for kimchi premium
        assert config.valid_quote_currencies.get("UPBIT") == ["KRW"]
        # Coinbase uses USD for coinbase premium
        assert config.valid_quote_currencies.get("COINBASE") == ["USD"]

    def test_upbit_coinbase_not_in_derivative_exchanges(self):
        """Test Upbit and Coinbase are not in derivative exchanges (spot only)."""
        config = ExchangeInstrumentConfig()

        assert "UPBIT" not in config.derivative_exchanges
        assert "COINBASE" not in config.derivative_exchanges


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
            "OPTION": ["options_chain", "trades", "book_snapshot_5", "liquidations"],
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


class TestInstrumentsServiceConfig:
    """Tests for InstrumentsServiceConfig (Pydantic BaseSettings)."""

    def test_instruments_service_config_singleton(self):
        """Test that instruments_config singleton is loaded."""
        from instruments_service.config import instruments_config

        assert instruments_config is not None
        assert instruments_config.service_name == "instruments-service"

    def test_instruments_service_config_default_values(self):
        """Test InstrumentsServiceConfig has expected default values."""
        from instruments_service.config import instruments_config

        assert instruments_config.enable_ccxt_integration is True
        assert instruments_config.enable_metadata_caching is True
        assert instruments_config.cache_ttl_hours == 24
        assert instruments_config.max_batch_size == 1000
        assert instruments_config.lookback_days == 0

    def test_instruments_service_config_gcs_buckets(self):
        """Test that GCS bucket properties exist."""
        from instruments_service.config import instruments_config

        # Test that bucket properties are accessible
        assert instruments_config.gcs_bucket is not None
        assert hasattr(instruments_config, "gcs_bucket_cefi")
        assert hasattr(instruments_config, "gcs_bucket_tradfi")
        assert hasattr(instruments_config, "gcs_bucket_defi")

    def test_instruments_service_config_get_cloud_target(self):
        """Test get_cloud_target method."""
        from instruments_service.config import instruments_config

        cloud_target = instruments_config.get_cloud_target()
        assert cloud_target is not None
        assert cloud_target.gcs_bucket is not None
        assert cloud_target.bigquery_dataset is not None

    def test_instruments_service_config_get_cloud_target_with_category(self):
        """Test get_cloud_target with category."""
        from instruments_service.config import instruments_config

        cefi_target = instruments_config.get_cloud_target(category="CEFI")
        tradfi_target = instruments_config.get_cloud_target(category="TRADFI")
        defi_target = instruments_config.get_cloud_target(category="DEFI")

        assert cefi_target is not None
        assert tradfi_target is not None
        assert defi_target is not None

    def test_instruments_service_config_is_test_environment(self):
        """Test is_test_environment method."""
        from instruments_service.config import instruments_config

        # Default should not be test environment in most cases
        assert isinstance(instruments_config.is_test_environment(), bool)

    def test_instruments_service_config_get_bucket_for_category(self):
        """Test get_bucket_for_category method."""
        from instruments_service.config import instruments_config

        cefi_bucket = instruments_config.get_bucket_for_category("CEFI")
        tradfi_bucket = instruments_config.get_bucket_for_category("TRADFI")
        defi_bucket = instruments_config.get_bucket_for_category("DEFI")

        assert cefi_bucket is not None
        assert tradfi_bucket is not None
        assert defi_bucket is not None

    def test_instruments_service_config_uppercase_properties(self):
        """Test uppercase property aliases for bucket names."""
        from instruments_service.config import instruments_config

        # Test uppercase properties exist and are accessible
        assert instruments_config.INSTRUMENTS_GCS_BUCKET is not None
        assert instruments_config.INSTRUMENTS_GCS_BUCKET_CEFI is not None
        assert instruments_config.INSTRUMENTS_GCS_BUCKET_TRADFI is not None
        assert instruments_config.INSTRUMENTS_GCS_BUCKET_DEFI is not None


class TestInstrumentDefinitionsDefi:
    """Tests for DEFI_PROTOCOLS and DEFI_VENUE_TO_PROTOCOL (Issue #91)."""

    def test_defi_venue_to_protocol_has_expected_venues(self):
        """Test DEFI_VENUE_TO_PROTOCOL contains key DeFi venues."""
        from instruments_service.config import DEFI_VENUE_TO_PROTOCOL

        assert "UNISWAPV3-ETH" in DEFI_VENUE_TO_PROTOCOL
        assert DEFI_VENUE_TO_PROTOCOL["UNISWAPV3-ETH"] == ("uniswap_v3", "ETHEREUM")
        assert "HYPERLIQUID" in DEFI_VENUE_TO_PROTOCOL
        assert DEFI_VENUE_TO_PROTOCOL["HYPERLIQUID"][1] is None

    def test_defi_protocols_non_empty(self):
        """Test DEFI_PROTOCOLS is non-empty and has correct structure."""
        from instruments_service.config import DEFI_PROTOCOLS

        assert len(DEFI_PROTOCOLS) > 0
        for item in DEFI_PROTOCOLS:
            assert isinstance(item, tuple)
            assert len(item) == 2
            assert isinstance(item[0], str)
            assert item[1] is None or isinstance(item[1], str)
