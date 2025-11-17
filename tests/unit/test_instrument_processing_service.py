"""
Unit tests for InstrumentProcessingService.

Tests service orchestration logic with mocked dependencies.
"""

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
