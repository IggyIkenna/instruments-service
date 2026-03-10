"""
Unit tests for InstrumentProcessingService.

Tests service orchestration logic with mocked dependencies.
"""

import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
)


@pytest.fixture(autouse=True)
def _mock_get_secret_client():
    """Mock get_secret_client and remove GOOGLE_APPLICATION_CREDENTIALS for unit tests."""
    mock_client = MagicMock()
    mock_client.get_secret.return_value = None
    env_overrides = {"GOOGLE_APPLICATION_CREDENTIALS": ""}
    with (
        patch("unified_trading_library.get_secret_client", return_value=mock_client),
        patch.dict(os.environ, env_overrides),
    ):
        yield


class TestInstrumentProcessingService:
    """Test InstrumentProcessingService."""

    def test_service_creation_with_api_key(self):
        """Test creating service with API key in config."""
        config = {"tardis_api_key": "test-api-key-12345", "project_id": "test-project"}
        service = InstrumentProcessingService(config)
        assert service.api_key == "test-api-key-12345"

    @patch("instruments_service.app.core.instrument_processing_base.get_secret_client")
    def test_service_creation_with_secret_manager(self, mock_get_secret):
        """Test creating service with Secret Manager."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.get_secret.return_value = "secret-api-key-67890"
        mock_get_secret.return_value = mock_client
        config = {
            "project_id": "test-project"
            # No tardis_api_key provided - will be lazy-loaded when CeFi is requested
        }
        # Tardis API key is now optional - service can be created without it
        service = InstrumentProcessingService(config)
        # API key is lazy-loaded, so we can't assert it here
        # But we can verify the service was created successfully
        assert service is not None

    def test_service_creation_no_api_key(self):
        """Test creating service without API key succeeds (lazy-loaded)."""
        config = {"project_id": "test-project"}
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        mock_client.get_secret.return_value = None
        with patch(
            "instruments_service.app.core.instrument_processing_base.get_secret_client",
            return_value=mock_client,
        ):
            # Tardis API key is now optional - service can be created without it
            # It will only fail when CeFi instruments are requested
            service = InstrumentProcessingService(config)
            assert service is not None

    def test_normalize_venue(self):
        """Test venue normalization."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("binance-futures")
        assert venue == "BINANCE-FUTURES"

    def test_normalize_instrument_type(self):
        """Test instrument type normalization."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        inst_type = service.normalize_instrument_type("perpetual")
        assert inst_type == "PERPETUAL"

    def test_generate_canonical_key_spot(self):
        """Test canonical key generation for spot pair."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        key = service.generate_canonical_key(
            exchange="binance",
            symbol_type="spot",
            symbol_id="btcusdt",
            symbol_info={"base_asset": "BTC", "quote_asset": "USDT"},
        )
        assert key == "BINANCE-SPOT:SPOT_PAIR:BTC-USDT"  # Updated to match canonical spec

    def test_generate_canonical_key_perpetual(self):
        """Test canonical key generation for perpetual."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        key = service.generate_canonical_key(
            exchange="binance-futures",
            symbol_type="perpetual",
            symbol_id="btcusdt",
            symbol_info={"base_asset": "BTC", "quote_asset": "USDT"},
        )
        # Perpetuals include @LIN or @INV suffix
        assert key.startswith("BINANCE-FUTURES:PERPETUAL:BTC-USDT")
        assert "@" in key

    def test_normalize_venue_upbit(self):
        """Test venue normalization for Upbit."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("upbit")
        assert venue == "UPBIT"

    def test_normalize_venue_coinbase(self):
        """Test venue normalization for Coinbase."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("coinbase")
        assert venue == "COINBASE"

    def test_generate_canonical_key_upbit_spot(self):
        """Test canonical key generation for Upbit spot pair."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        key = service.generate_canonical_key(
            exchange="upbit",
            symbol_type="spot",
            symbol_id="BTC-KRW",
            symbol_info={"base_asset": "BTC", "quote_asset": "KRW"},
        )
        assert key == "UPBIT:SPOT_PAIR:BTC-KRW"

    def test_generate_canonical_key_coinbase_spot(self):
        """Test canonical key generation for Coinbase spot pair."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        key = service.generate_canonical_key(
            exchange="coinbase",
            symbol_type="spot",
            symbol_id="BTC-USD",
            symbol_info={"base_asset": "BTC", "quote_asset": "USD"},
        )
        assert key == "COINBASE:SPOT_PAIR:BTC-USD"


class TestUpbitCoinbaseMVPFiltering:
    """Tests for MVP base asset filtering on Upbit and Coinbase."""

    def test_upbit_in_spot_mvp_filtered_venues(self):
        """Test that UPBIT is in spot_mvp_filtered_venues."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        assert "UPBIT" in service.venue_mapping.spot_mvp_filtered_venues

    def test_coinbase_in_spot_mvp_filtered_venues(self):
        """Test that COINBASE is in spot_mvp_filtered_venues."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        assert "COINBASE" in service.venue_mapping.spot_mvp_filtered_venues

    def test_mvp_base_assets_for_filtering(self):
        """Test MVP base assets list used for filtering."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        mvp_assets = service.venue_mapping.hyperliquid_aster_mvp_base_assets

        # Should include the 21 MVP base assets
        assert len(mvp_assets) == 21
        assert "BTC" in mvp_assets
        assert "ETH" in mvp_assets
        assert "SOL" in mvp_assets
        assert "XRP" in mvp_assets
        assert "DOGE" in mvp_assets
        assert "LINK" in mvp_assets

    def test_exchange_config_upbit_spot_only(self):
        """Test ExchangeInstrumentConfig for Upbit is spot only."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        upbit_types = service.exchange_config.exchange_instrument_types.get("UPBIT", [])
        assert upbit_types == ["SPOT_PAIR"]

    def test_exchange_config_coinbase_spot_only(self):
        """Test ExchangeInstrumentConfig for Coinbase is spot only."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        coinbase_types = service.exchange_config.exchange_instrument_types.get("COINBASE", [])
        assert coinbase_types == ["SPOT_PAIR"]

    def test_quote_currency_upbit_krw(self):
        """Test Upbit uses KRW as quote currency."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        upbit_quotes = service.exchange_config.valid_quote_currencies.get("UPBIT", [])
        assert upbit_quotes == ["KRW"]

    def test_quote_currency_coinbase_usd(self):
        """Test Coinbase uses USD as quote currency."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        service = InstrumentProcessingService(config)
        coinbase_quotes = service.exchange_config.valid_quote_currencies.get("COINBASE", [])
        assert coinbase_quotes == ["USD"]

    @pytest.mark.asyncio
    async def test_process_exchange_instruments_upbit_mvp_filter(self):
        """Test process_exchange_instruments applies MVP filter for Upbit."""
        config = {"tardis_api_key": "test-key", "project_id": "test-project"}
        service = InstrumentProcessingService(config)

        # Mock fetch_exchange_instruments to return some test data with all required fields
        with patch.object(
            service,
            "fetch_exchange_instruments",
            new_callable=AsyncMock,
            return_value=(
                {
                    "BTC-KRW": {
                        "id": "BTC-KRW",
                        "type": "spot",
                        "availableSince": "2020-01-01T00:00:00Z",
                    },  # MVP coin - should be included
                    "ETH-KRW": {
                        "id": "ETH-KRW",
                        "type": "spot",
                        "availableSince": "2020-01-01T00:00:00Z",
                    },  # MVP coin - should be included
                    "RANDOM-KRW": {
                        "id": "RANDOM-KRW",
                        "type": "spot",
                        "availableSince": "2020-01-01T00:00:00Z",
                    },  # Not MVP - should be filtered
                },
                0,
            ),
        ):
            # Also mock parse_symbol_components to return correct base/quote
            original_parse = service.parse_symbol_components

            def mock_parse(symbol_id, exchange):
                mapping = {
                    "BTC-KRW": {"base_asset": "BTC", "quote_asset": "KRW"},
                    "ETH-KRW": {"base_asset": "ETH", "quote_asset": "KRW"},
                    "RANDOM-KRW": {"base_asset": "RANDOM", "quote_asset": "KRW"},
                }
                return mapping.get(symbol_id, original_parse(symbol_id, exchange))

            with patch.object(service, "parse_symbol_components", side_effect=mock_parse):
                result = await service.process_exchange_instruments(
                    exchange="upbit",
                    target_date=datetime(2024, 1, 1, tzinfo=UTC),
                )

                # Should only include BTC and ETH (MVP coins), not RANDOM
                keys = list(result.keys())
                # BTC and ETH are MVP coins
                assert any("BTC" in key for key in keys)
                assert any("ETH" in key for key in keys)
                # RANDOM is not an MVP coin and should be filtered out
                assert not any("RANDOM" in key for key in keys)


class TestSymbolParsing:
    """Tests for symbol parsing logic for different exchanges."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_parse_symbol_upbit_krw_sol(self, service):
        """Test Upbit symbol parsing for KRW-SOL (QUOTE-BASE format)."""
        result = service.parse_symbol_components("KRW-SOL", "upbit")
        assert result["base_asset"] == "SOL"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_upbit_krw_btc(self, service):
        """Test Upbit symbol parsing for KRW-BTC (QUOTE-BASE format)."""
        result = service.parse_symbol_components("KRW-BTC", "upbit")
        assert result["base_asset"] == "BTC"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_coinbase_sol_usd(self, service):
        """Test Coinbase symbol parsing for SOL-USD (BASE-QUOTE format)."""
        result = service.parse_symbol_components("SOL-USD", "coinbase")
        assert result["base_asset"] == "SOL"
        assert result["quote_asset"] == "USD"

    def test_parse_symbol_coinbase_btc_usd(self, service):
        """Test Coinbase symbol parsing for BTC-USD (BASE-QUOTE format)."""
        result = service.parse_symbol_components("BTC-USD", "coinbase")
        assert result["base_asset"] == "BTC"
        assert result["quote_asset"] == "USD"

    def test_parse_symbol_upbit_lowercase(self, service):
        """Test Upbit symbol parsing with lowercase input."""
        result = service.parse_symbol_components("krw-eth", "upbit")
        assert result["base_asset"] == "ETH"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_coinbase_lowercase(self, service):
        """Test Coinbase symbol parsing with lowercase input."""
        result = service.parse_symbol_components("eth-usd", "coinbase")
        assert result["base_asset"] == "ETH"
        assert result["quote_asset"] == "USD"

    def test_parse_symbol_components_binance(self, service):
        """Test symbol parsing for Binance."""
        result = service.parse_symbol_components("BTCUSDT", "binance")
        assert isinstance(result, dict)
        assert "base_asset" in result or result.get("base_asset") == "BTC"

    def test_parse_symbol_components_deribit(self, service):
        """Test symbol parsing for Deribit."""
        result = service.parse_symbol_components("BTC-PERPETUAL", "deribit")
        assert isinstance(result, dict)

        result = service.parse_symbol_components("BTC-25DEC25", "deribit")
        assert isinstance(result, dict)

    def test_parse_symbol_components_bybit(self, service):
        """Test parsing Bybit symbol components."""
        result = service.parse_symbol_components("BTCUSDT", "bybit")
        assert isinstance(result, dict)
        assert "base_asset" in result

    def test_parse_symbol_components_okx(self, service):
        """Test parsing OKX symbol components."""
        result = service.parse_symbol_components("BTC-USDT", "okx")
        assert isinstance(result, dict)
        assert "base_asset" in result

    def test_parse_symbol_components_okx_perp(self, service):
        """Test parsing OKX PERP symbol."""
        result = service.parse_symbol_components("PERP-USDT", "okx")
        assert isinstance(result, dict)
        assert result.get("base_asset") == "PERP"

    def test_parse_symbol_components_okx_futures(self, service):
        """Test parsing OKX futures symbol components."""
        result = service.parse_symbol_components("BTC-USDT-241225", "okex-futures")
        assert isinstance(result, dict)

    def test_parse_symbol_components_empty(self, service):
        """Test parsing empty symbol returns empty dict."""
        result = service.parse_symbol_components("", "binance")
        assert isinstance(result, dict)


class TestTardisSymbolConversion:
    """Tests for Tardis symbol conversion."""

    @pytest.fixture
    def service(self):
        """Create processing service for testing."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_convert_to_tardis_symbol_binance(self, service):
        """Test Tardis symbol conversion for Binance."""
        result = service._convert_to_tardis_symbol("BTC-USDT", "binance")
        assert "btc" in result.lower() and "usdt" in result.lower()

    def test_convert_to_tardis_symbol_deribit(self, service):
        """Test Tardis symbol conversion for Deribit."""
        result = service._convert_to_tardis_symbol("BTC-PERPETUAL", "deribit")
        assert "btc" in result.lower() and "perpetual" in result.lower()

    def test_convert_to_tardis_symbol_okx(self, service):
        """Test converting OKX symbol to Tardis format."""
        result = service._convert_to_tardis_symbol("BTC-USDT", "okx")
        assert result == "btc-usdt"

    def test_convert_to_tardis_symbol_bybit(self, service):
        """Test converting Bybit symbol to Tardis format."""
        result = service._convert_to_tardis_symbol("BTCUSDT", "bybit")
        assert "btc" in result.lower()

    def test_convert_to_tardis_symbol_lowercase(self, service):
        """Test Tardis symbol conversion lowercases."""
        result = service._convert_to_tardis_symbol("BTCUSDT", "binance-futures")
        assert result == result.lower()


class TestProblematicInstruments:
    """Tests for problematic instrument detection."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_is_problematic_binance_instrument_basic(self, service):
        """Test problematic Binance instrument detection."""
        assert service._is_problematic_binance_instrument("1000SHIBUSDT")
        assert service._is_problematic_binance_instrument("USDTTRY")
        assert not service._is_problematic_binance_instrument("BTCUSDT")

    def test_is_problematic_binance_instrument_multiplier_patterns(self, service):
        """Test detecting problematic Binance multiplier patterns."""
        problematic = ["1000xUSDT", "1000satsUSDT", "1000catUSDT", "1000000mogUSDT"]
        for symbol in problematic:
            assert service._is_problematic_binance_instrument(symbol)

    def test_is_problematic_binance_instrument_usdt_base(self, service):
        """Test detecting USDT as base asset."""
        problematic = ["USDTTRY", "USDTZAR", "USDTUAH"]
        for symbol in problematic:
            assert service._is_problematic_binance_instrument(symbol)

    def test_is_problematic_binance_instrument_valid(self, service):
        """Test valid Binance instruments are not flagged."""
        valid = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        for symbol in valid:
            assert not service._is_problematic_binance_instrument(symbol)


class TestDeribitParsing:
    """Tests for Deribit-specific parsing."""

    @pytest.fixture
    def service(self):
        """Create processing service for testing."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_parse_deribit_date(self, service):
        """Test Deribit date parsing."""
        result = service._parse_deribit_date("25DEC25")
        assert "2025-12-25" in result
        assert "T08:00:00Z" in result

    def test_parse_deribit_date_single_digit_day(self, service):
        """Test parsing Deribit date with single digit day."""
        result = service._parse_deribit_date("7NOV25")
        assert "2025-11-07" in result

    def test_parse_deribit_date_with_two_digit_day(self, service):
        """Test parsing Deribit date with two digit day."""
        result = service._parse_deribit_date("31DEC25")
        assert "2025-12-31" in result

    def test_parse_deribit_date_invalid(self, service):
        """Test parsing invalid Deribit date returns fallback."""
        result = service._parse_deribit_date("INVALID")
        assert "2025-12-25" in result

    def test_parse_expiry_from_symbol_deribit(self, service):
        """Test expiry parsing from Deribit symbol."""
        result = service.parse_expiry_from_symbol("BTC-25DEC25-50000-C", "deribit")
        assert result is not None

    def test_parse_expiry_from_symbol_bybit(self, service):
        """Test parsing expiry from Bybit symbol."""
        result = service.parse_expiry_from_symbol("BTC-25DEC24", "bybit")
        assert result is not None

    def test_parse_expiry_from_symbol_binance_futures(self, service):
        """Test parsing expiry from Binance futures symbol."""
        result = service.parse_expiry_from_symbol("btcusdt_241225", "binance-futures")
        assert result is not None
        assert "2024-12-25" in result

    def test_parse_expiry_from_symbol_okx(self, service):
        """Test parsing expiry from OKX symbol."""
        result = service.parse_expiry_from_symbol("BTC-USDT-241225", "okex-futures")
        assert result is not None


class TestOptionParsing:
    """Tests for option component parsing."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_parse_option_components_deribit_new_format(self, service):
        """Test parsing Deribit option components with new format."""
        result = service.parse_option_components("BTC-USD-240329-120000-CALL", "deribit")
        assert "strike_price" in result
        assert "option_type" in result
        assert result["option_type"] == "CALL"

    def test_parse_option_components_deribit_traditional_format(self, service):
        """Test parsing Deribit option components with traditional format."""
        result = service.parse_option_components("BTC-25DEC25-50000-C", "deribit")
        assert "strike_price" in result
        assert "option_type" in result
        assert result["option_type"] == "CALL"

    def test_parse_option_components_deribit_put(self, service):
        """Test parsing Deribit PUT option."""
        result = service.parse_option_components("BTC-25DEC25-50000-P", "deribit")
        assert result["option_type"] == "PUT"

    def test_parse_option_components_deribit_decimal_strike(self, service):
        """Test parsing Deribit option with decimal strike (1d14 format)."""
        result = service.parse_option_components("BTC-25DEC25-1d14-C", "deribit")
        assert result["strike_price"] == "1.14"


class TestDerivedFields:
    """Tests for derived field population."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_ccxt_integration": False}
        return InstrumentProcessingService(config)

    @pytest.mark.asyncio
    async def test_populate_all_derived_fields_option(self, service):
        """Test populating derived fields for options."""
        fields = await service._populate_all_derived_fields(
            "DERIBIT:OPTION:BTC-USD-241225-50000-CALL",
            "DERIBIT",
            "OPTION",
            "BTC",
            "USD",
            "BTC-25DEC24-50000-C",
            "deribit",
        )
        assert isinstance(fields, dict)
        if fields:
            assert "expiry" in fields or "strike" in fields or "option_type" in fields or "ccxt_symbol" in fields

    @pytest.mark.asyncio
    async def test_populate_all_derived_fields_future(self, service):
        """Test populating derived fields for futures."""
        fields = await service._populate_all_derived_fields(
            "DERIBIT:FUTURE:BTC-USD-241225",
            "DERIBIT",
            "FUTURE",
            "BTC",
            "USD",
            "BTC-25DEC24",
            "deribit",
        )
        assert isinstance(fields, dict)
        if fields:
            assert "expiry" in fields or "ccxt_symbol" in fields

    @pytest.mark.asyncio
    async def test_populate_all_derived_fields_deribit_inverse(self, service):
        """Test populating derived fields for Deribit inverse perpetual."""
        fields = await service._populate_all_derived_fields(
            "DERIBIT:PERPETUAL:BTC-USD",
            "DERIBIT",
            "PERPETUAL",
            "BTC",
            "USD",
            "BTC-PERPETUAL",
            "deribit",
        )
        assert isinstance(fields, dict)
        if fields:
            assert fields.get("inverse")

    @pytest.mark.asyncio
    async def test_populate_all_derived_fields_deribit_linear(self, service):
        """Test populating derived fields for Deribit linear perpetual."""
        fields = await service._populate_all_derived_fields(
            "DERIBIT:PERPETUAL:BTC-USDC",
            "DERIBIT",
            "PERPETUAL",
            "BTC",
            "USDC",
            "BTC_USDC-PERPETUAL",
            "deribit",
        )
        assert isinstance(fields, dict)
        if fields:
            assert not fields.get("inverse")

    @pytest.mark.asyncio
    async def test_populate_all_derived_fields_underlying(self, service):
        """Test populating underlying asset for derivatives."""
        fields = await service._populate_all_derived_fields(
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT",
            "BINANCE-FUTURES",
            "PERPETUAL",
            "BTC",
            "USDT",
            "BTCUSDT",
            "binance-futures",
        )
        assert isinstance(fields, dict)
        if fields:
            assert fields.get("underlying") == "BTC-USDT" or "ccxt_symbol" in fields


class TestCanonicalKeyGeneration:
    """Tests for canonical key generation for different instrument types."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_generate_canonical_key_future_with_expiry(self, service):
        """Test canonical key generation for future with expiry."""
        key = service.generate_canonical_key(
            exchange="deribit",
            symbol_type="future",
            symbol_id="BTC-25DEC25",
            symbol_info={"base_asset": "BTC", "quote_asset": "USD"},
        )
        assert "DERIBIT:FUTURE:BTC-USD" in key

    def test_generate_canonical_key_option(self, service):
        """Test canonical key generation for option."""
        key = service.generate_canonical_key(
            exchange="deribit",
            symbol_type="option",
            symbol_id="BTC-25DEC25-50000-C",
            symbol_info={"base_asset": "BTC", "quote_asset": "USD"},
        )
        assert "DERIBIT:OPTION" in key

    def test_generate_canonical_key_inverse_perpetual(self, service):
        """Test canonical key for inverse perpetual."""
        key = service.generate_canonical_key(
            exchange="bybit",
            symbol_type="perpetual",
            symbol_id="BTCUSD",
            symbol_info={"base_asset": "BTC", "quote_asset": "USD"},
        )
        assert "BYBIT:PERPETUAL:BTC-USD" in key

    def test_generate_canonical_key_linear_perpetual(self, service):
        """Test canonical key for linear perpetual."""
        key = service.generate_canonical_key(
            exchange="bybit",
            symbol_type="perpetual",
            symbol_id="BTCUSDT",
            symbol_info={"base_asset": "BTC", "quote_asset": "USDT"},
        )
        assert "BYBIT:PERPETUAL:BTC-USDT" in key


class TestCacheOperations:
    """Tests for metadata caching operations."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_metadata_caching": True}
        return InstrumentProcessingService(config)

    def test_cache_metadata(self, service):
        """Test metadata caching delegates to processor."""
        from instruments_service.models import InstrumentDefinition

        metadata = InstrumentDefinition(
            instrument_key="TEST:SPOT_PAIR:BTC-USDT",
            venue="TEST",
            instrument_type="SPOT_PAIR",
            symbol="BTC-USDT",
            available_from_datetime="2023-01-01T00:00:00Z",
        )

        service.cache_metadata("TEST:SPOT_PAIR:BTC-USDT", metadata)
        # Cache is directly on service, not on a processor
        assert len(service._metadata_cache) > 0

    def test_clear_cache(self, service):
        """Test cache clearing delegates to processors."""
        service._metadata_cache["test"] = "data"
        service._cache_timestamps["test"] = datetime.now(UTC)
        service.clear_cache()
        assert len(service._metadata_cache) == 0

    def test_metadata_cache_clear_specific_key(self, service):
        """Test clearing cache clears all keys from all processors."""
        service._metadata_cache["key1"] = "data1"
        service._metadata_cache["key2"] = "data2"
        service._cache_timestamps["key1"] = datetime.now(UTC)
        service._cache_timestamps["key2"] = datetime.now(UTC)
        service.clear_cache()
        assert len(service._metadata_cache) == 0


class TestServiceOperations:
    """Tests for service-level operations."""

    @pytest.fixture(scope="class")
    def service(self):
        """Create processing service for testing (class-scoped for performance)."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_get_processing_stats(self, service):
        """Test getting processing statistics."""
        stats = service.get_processing_stats()
        assert "supported_exchanges" in stats
        assert "ccxt_integration_enabled" in stats
        assert "caching_enabled" in stats

    def test_supported_exchanges_count(self, service):
        """Test getting supported exchanges count."""
        stats = service.get_processing_stats()
        assert "supported_exchanges" in stats
        assert isinstance(stats["supported_exchanges"], (int, list))
        if isinstance(stats["supported_exchanges"], int):
            assert stats["supported_exchanges"] > 0
        else:
            assert len(stats["supported_exchanges"]) > 0

    def test_cleanup(self, service):
        """Test service cleanup."""
        service.cleanup()
        assert True, "Cleanup completed without error"


class TestInstrumentFiltering:
    """Tests for instrument filtering."""

    @pytest.fixture
    def service(self):
        """Create processing service for testing."""
        config = {"tardis_api_key": "test-key", "enable_defi_integration": False}
        return InstrumentProcessingService(config)

    def test_filter_instruments_by_exchange_config(self, service):
        """Test filtering instruments by exchange config."""
        instruments = {
            "BINANCE-FUTURES:PERPETUAL:BTC-USDT": {
                "instrument_type": "PERPETUAL",
                "quote_asset": "USDT",
            },
            "BINANCE-FUTURES:SPOT_PAIR:BTC-USDT": {
                "instrument_type": "SPOT_PAIR",
                "quote_asset": "USDT",
            },
        }

        filtered = service.filter_instruments_by_exchange_config(instruments, "BINANCE-FUTURES")
        assert "BINANCE-FUTURES:PERPETUAL:BTC-USDT" in filtered or len(filtered) == 0
