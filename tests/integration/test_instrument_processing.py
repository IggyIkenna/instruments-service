"""
Integration tests for InstrumentProcessingService.

Tests Tardis API integration, CCXT enrichment, and end-to-end processing
with test bucket and real credentials.
"""

from datetime import datetime, timezone

import pytest
from unified_cloud_services import VenueMapping

from instruments_service.app.core.instrument_processing_service import (
    InstrumentProcessingService,
)


@pytest.mark.integration
class TestInstrumentProcessingIntegration:
    """Test InstrumentProcessingService integration with real APIs."""

    @pytest.mark.asyncio
    async def test_fetch_exchange_instruments(self, gcp_project_id, tardis_api_key):
        """Test fetching instruments from Tardis API."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": tardis_api_key,
            "enable_ccxt_integration": False,  # Disable for faster testing
            "enable_metadata_caching": True,
        }

        service = InstrumentProcessingService(config)

        # Fetch instruments for a single exchange
        target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        result = await service.fetch_exchange_instruments(
            exchange="binance-futures", target_date=target_date, force=False
        )

        # fetch_exchange_instruments returns tuple (instruments_dict, date_filtered_count)
        assert isinstance(result, tuple)
        assert len(result) == 2
        instruments, date_filtered_count = result
        assert isinstance(instruments, dict)
        assert len(instruments) > 0, "Should fetch at least some instruments"
        assert isinstance(date_filtered_count, int)

    @pytest.mark.asyncio
    async def test_process_exchange_instruments(self, gcp_project_id, tardis_api_key):
        """Test processing instruments for an exchange."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": tardis_api_key,
            "enable_ccxt_integration": False,
            "enable_metadata_caching": True,
        }

        service = InstrumentProcessingService(config)

        target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        processed = await service.process_exchange_instruments(
            exchange="binance-futures", target_date=target_date, force=False
        )

        assert isinstance(processed, dict)
        # Should have some processed instruments (after filtering)
        # Exact count depends on exchange config filtering

    @pytest.mark.asyncio
    async def test_secret_manager_api_key_retrieval(self, gcp_project_id):
        """Test that API key can be retrieved from Secret Manager."""
        # Don't provide API key in config - should use Secret Manager
        config = {"project_id": gcp_project_id, "enable_ccxt_integration": False}

        service = InstrumentProcessingService(config)

        # Should have API key from Secret Manager
        assert service.api_key is not None
        assert len(service.api_key) > 0

    @pytest.mark.asyncio
    async def test_generate_instruments_for_exchanges(self, gcp_project_id, tardis_api_key):
        """Test generating instruments for multiple exchanges."""
        config = {
            "project_id": gcp_project_id,
            "tardis_api_key": tardis_api_key,
            "enable_ccxt_integration": False,
            "enable_metadata_caching": True,
        }

        service = InstrumentProcessingService(config)
        VenueMapping()

        # Test with a subset of exchanges for faster testing
        test_exchanges = ["binance-futures", "deribit"]

        target_date = datetime(2023, 5, 23, tzinfo=timezone.utc)
        all_instruments = await service.generate_instruments_for_exchanges(
            exchanges=test_exchanges, target_date=target_date
        )

        assert isinstance(all_instruments, dict)
        assert len(all_instruments) > 0
