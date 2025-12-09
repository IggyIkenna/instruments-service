"""
Unit tests for InstrumentProcessingService.

Tests service orchestration logic with mocked dependencies.
"""

import os
import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
    InstrumentProcessingConfig,
)


class TestInstrumentProcessingService:
    """Test InstrumentProcessingService."""

    def test_service_creation_with_api_key(self):
        """Test creating service with API key in config."""
        config = {"tardis_api_key": "test-api-key-12345", "project_id": "test-project"}
        service = InstrumentProcessingService(config)
        assert service.api_key == "test-api-key-12345"

    @patch("instruments_service.app.core.instrument_processing_service.get_secret_with_fallback")
    def test_service_creation_with_secret_manager(self, mock_get_secret):
        """Test creating service with Secret Manager."""
        mock_get_secret.return_value = "secret-api-key-67890"
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
        with patch(
            "instruments_service.app.core.instrument_processing_service.get_secret_with_fallback",
            return_value=None,
        ):
            # Tardis API key is now optional - service can be created without it
            # It will only fail when CeFi instruments are requested
            service = InstrumentProcessingService(config)
            assert service is not None

    def test_normalize_venue(self):
        """Test venue normalization."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("binance-futures")
        assert venue == "BINANCE-FUTURES"

    def test_normalize_instrument_type(self):
        """Test instrument type normalization."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        inst_type = service.normalize_instrument_type("perpetual")
        assert inst_type == "PERPETUAL"

    def test_generate_canonical_key_spot(self):
        """Test canonical key generation for spot pair."""
        config = {"tardis_api_key": "test-key"}
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
        config = {"tardis_api_key": "test-key"}
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
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("upbit")
        assert venue == "UPBIT"

    def test_normalize_venue_coinbase(self):
        """Test venue normalization for Coinbase."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        venue = service.normalize_venue("coinbase")
        assert venue == "COINBASE"

    def test_generate_canonical_key_upbit_spot(self):
        """Test canonical key generation for Upbit spot pair."""
        config = {"tardis_api_key": "test-key"}
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
        config = {"tardis_api_key": "test-key"}
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
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        assert "UPBIT" in service.venue_mapping.spot_mvp_filtered_venues

    def test_coinbase_in_spot_mvp_filtered_venues(self):
        """Test that COINBASE is in spot_mvp_filtered_venues."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        assert "COINBASE" in service.venue_mapping.spot_mvp_filtered_venues

    def test_mvp_base_assets_for_filtering(self):
        """Test MVP base assets list used for filtering."""
        config = {"tardis_api_key": "test-key"}
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
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        upbit_types = service.exchange_config.exchange_instrument_types.get("UPBIT", [])
        assert upbit_types == ["SPOT_PAIR"]

    def test_exchange_config_coinbase_spot_only(self):
        """Test ExchangeInstrumentConfig for Coinbase is spot only."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        coinbase_types = service.exchange_config.exchange_instrument_types.get("COINBASE", [])
        assert coinbase_types == ["SPOT_PAIR"]

    def test_quote_currency_upbit_krw(self):
        """Test Upbit uses KRW as quote currency."""
        config = {"tardis_api_key": "test-key"}
        service = InstrumentProcessingService(config)
        upbit_quotes = service.exchange_config.valid_quote_currencies.get("UPBIT", [])
        assert upbit_quotes == ["KRW"]

    def test_quote_currency_coinbase_usd(self):
        """Test Coinbase uses USD as quote currency."""
        config = {"tardis_api_key": "test-key"}
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
            # Also mock _parse_symbol_components to return correct base/quote
            original_parse = service._parse_symbol_components

            def mock_parse(symbol_id, exchange):
                if symbol_id == "BTC-KRW":
                    return {"base_asset": "BTC", "quote_asset": "KRW"}
                elif symbol_id == "ETH-KRW":
                    return {"base_asset": "ETH", "quote_asset": "KRW"}
                elif symbol_id == "RANDOM-KRW":
                    return {"base_asset": "RANDOM", "quote_asset": "KRW"}
                return original_parse(symbol_id, exchange)

            with patch.object(service, "_parse_symbol_components", side_effect=mock_parse):
                result = await service.process_exchange_instruments(
                    exchange="upbit",
                    target_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
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

    @pytest.fixture
    def service(self):
        """Create processing service for testing."""
        config = {"tardis_api_key": "test-key"}
        return InstrumentProcessingService(config)

    def test_parse_symbol_upbit_krw_sol(self, service):
        """Test Upbit symbol parsing for KRW-SOL (QUOTE-BASE format)."""
        result = service._parse_symbol_components("KRW-SOL", "upbit")
        assert result["base_asset"] == "SOL"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_upbit_krw_btc(self, service):
        """Test Upbit symbol parsing for KRW-BTC (QUOTE-BASE format)."""
        result = service._parse_symbol_components("KRW-BTC", "upbit")
        assert result["base_asset"] == "BTC"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_coinbase_sol_usd(self, service):
        """Test Coinbase symbol parsing for SOL-USD (BASE-QUOTE format)."""
        result = service._parse_symbol_components("SOL-USD", "coinbase")
        assert result["base_asset"] == "SOL"
        assert result["quote_asset"] == "USD"

    def test_parse_symbol_coinbase_btc_usd(self, service):
        """Test Coinbase symbol parsing for BTC-USD (BASE-QUOTE format)."""
        result = service._parse_symbol_components("BTC-USD", "coinbase")
        assert result["base_asset"] == "BTC"
        assert result["quote_asset"] == "USD"

    def test_parse_symbol_upbit_lowercase(self, service):
        """Test Upbit symbol parsing with lowercase input."""
        result = service._parse_symbol_components("krw-eth", "upbit")
        assert result["base_asset"] == "ETH"
        assert result["quote_asset"] == "KRW"

    def test_parse_symbol_coinbase_lowercase(self, service):
        """Test Coinbase symbol parsing with lowercase input."""
        result = service._parse_symbol_components("eth-usd", "coinbase")
        assert result["base_asset"] == "ETH"
        assert result["quote_asset"] == "USD"
