"""
Integration tests for InstrumentProcessingService.

Tests API interaction patterns with MOCKED Tardis responses (Codex-compliant).
No real external calls - fast, reliable, same code paths exercised.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from unified_cloud_services import VenueMapping

from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
)

# Minimal Tardis-style symbol data that passes exchange config filters
# BINANCE-FUTURES: PERPETUAL/FUTURE, USDT quote
MOCK_TARDIS_SYMBOLS_BINANCE = [
    {
        "id": "btcusdt",
        "type": "perpetual",
        "availableSince": "2023-01-01T00:00:00.000Z",
        "availableTo": None,
    },
    {
        "id": "ethusdt",
        "type": "perpetual",
        "availableSince": "2023-01-01T00:00:00.000Z",
        "availableTo": None,
    },
]

# DERIBIT: PERPETUAL/FUTURE/OPTION, USD/USDC quote
MOCK_TARDIS_SYMBOLS_DERIBIT = [
    {
        "id": "btc-usd-perpetual",
        "type": "perpetual",
        "availableSince": "2023-01-01T00:00:00.000Z",
        "availableTo": None,
    },
]


def _make_mock_tardis_adapter():
    """Create mock TardisAdapter that returns canned data per exchange."""
    mock = MagicMock()

    def fetch(exchange, target_date=None, force_refresh=False):
        if "deribit" in (exchange or "").lower():
            return (MOCK_TARDIS_SYMBOLS_DERIBIT, len(MOCK_TARDIS_SYMBOLS_DERIBIT))
        return (MOCK_TARDIS_SYMBOLS_BINANCE, len(MOCK_TARDIS_SYMBOLS_BINANCE))

    mock.fetch_exchange_instruments = fetch
    return mock


@pytest.mark.integration
class TestInstrumentProcessingIntegration:
    """Test InstrumentProcessingService with mocked Tardis API."""

    @pytest.mark.asyncio
    async def test_fetch_exchange_instruments(self, gcp_project_id):
        """Test fetching instruments (mocked Tardis - no network)."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": "mock-key-for-testing",
            "enable_ccxt_integration": False,
            "enable_metadata_caching": True,
        }

        with patch.object(
            InstrumentProcessingService,
            "_get_tardis_adapter",
            return_value=_make_mock_tardis_adapter(),
        ):
            service = InstrumentProcessingService(config)
            target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            result = await service.fetch_exchange_instruments(
                exchange="binance-futures", target_date=target_date, force=False
            )

        assert isinstance(result, tuple)
        assert len(result) == 2
        instruments, date_filtered_count = result
        assert isinstance(instruments, dict)
        assert len(instruments) > 0, "Should return mocked instruments"
        assert isinstance(date_filtered_count, int)

    @pytest.mark.asyncio
    async def test_process_exchange_instruments(self, gcp_project_id):
        """Test processing instruments for an exchange (mocked Tardis)."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": "mock-key-for-testing",
            "enable_ccxt_integration": False,
            "enable_metadata_caching": True,
        }

        with patch.object(
            InstrumentProcessingService,
            "_get_tardis_adapter",
            return_value=_make_mock_tardis_adapter(),
        ):
            service = InstrumentProcessingService(config)
            target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            processed = await service.process_exchange_instruments(
                exchange="binance-futures", target_date=target_date, force=False
            )

        assert isinstance(processed, dict)
        # Mock data yields at least btcusdt, ethusdt after filtering
        assert len(processed) > 0

    @pytest.mark.asyncio
    async def test_generate_instruments_for_exchanges(self, gcp_project_id):
        """Test generating instruments for multiple exchanges (mocked)."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": "mock-key-for-testing",
            "enable_ccxt_integration": False,
            "enable_metadata_caching": True,
        }

        with patch.object(
            InstrumentProcessingService,
            "_get_tardis_adapter",
            return_value=_make_mock_tardis_adapter(),
        ):
            service = InstrumentProcessingService(config)
            VenueMapping()
            test_exchanges = ["binance-futures", "deribit"]
            target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
            all_instruments = await service.generate_instruments_for_exchanges(
                exchanges=test_exchanges, target_date=target_date
            )

        assert isinstance(all_instruments, dict)
        assert len(all_instruments) > 0


@pytest.mark.integration
@pytest.mark.api
class TestInstrumentProcessingSecretManager:
    """Tests requiring real Secret Manager (skipped in CI by -k 'not api')."""

    @pytest.mark.asyncio
    async def test_secret_manager_api_key_retrieval(self, gcp_project_id, tardis_api_key):
        """Test that API key can be retrieved from Secret Manager (real credentials)."""
        config = {"project_id": gcp_project_id, "enable_ccxt_integration": False}
        service = InstrumentProcessingService(config)
        assert service.api_key is not None
        assert len(service.api_key) > 0
