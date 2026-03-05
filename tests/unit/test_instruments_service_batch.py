"""
Unit tests for InstrumentsService — batch, query, cleanup, and edge-case scenarios.

Continuation of test_instruments_service.py — split for file-size compliance.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd
import pytest

from instruments_service.app.core.instruments_service import InstrumentsService


class TestGenerateInstrumentsDateRange:
    """Tests for generate_instruments_date_range method."""

    @pytest.mark.asyncio
    async def test_date_range_single_date(self):
        """Test date range processing with single date."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch_class,
        ):
            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            mock_batch = Mock()
            start_date = datetime(2024, 1, 1, tzinfo=UTC)
            mock_batch.get_required_periods = Mock(return_value=[start_date])
            mock_batch_class.return_value = mock_batch

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Mock generate_instruments_for_date
            service.generate_instruments_for_date = AsyncMock(
                return_value={
                    "status": "success",
                    "date": "2024-01-01",
                    "instruments_generated": 100,
                }
            )

            result = await service.generate_instruments_date_range(
                start_date=start_date, end_date=start_date, cefi=True
            )

            assert result["status"] == "success"
            assert result["dates_processed"] == 1
            assert result["dates_successful"] == 1
            assert result["dates_failed"] == 0
            assert result["total_instruments_generated"] == 100
            assert result["success_rate_percent"] == 100.0

    @pytest.mark.asyncio
    async def test_date_range_multiple_dates(self):
        """Test date range processing with multiple dates."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch_class,
        ):
            mock_batch = Mock()
            dates = [
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
                datetime(2024, 1, 3, tzinfo=UTC),
            ]
            mock_batch.get_required_periods = Mock(return_value=dates)
            mock_batch_class.return_value = mock_batch

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Mock generate_instruments_for_date with different results
            service.generate_instruments_for_date = AsyncMock(
                side_effect=[
                    {"status": "success", "date": "2024-01-01", "instruments_generated": 100},
                    {"status": "success", "date": "2024-01-02", "instruments_generated": 150},
                    {"status": "success", "date": "2024-01-03", "instruments_generated": 200},
                ]
            )

            result = await service.generate_instruments_date_range(start_date=dates[0], end_date=dates[2], cefi=True)

            assert result["status"] == "success"
            assert result["dates_processed"] == 3
            assert result["dates_successful"] == 3
            assert result["dates_failed"] == 0
            assert result["total_instruments_generated"] == 450
            assert result["success_rate_percent"] == 100.0

    @pytest.mark.asyncio
    async def test_date_range_partial_failures(self):
        """Test date range with some dates failing."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch_class,
        ):
            mock_batch = Mock()
            dates = [
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 2, tzinfo=UTC),
                datetime(2024, 1, 3, tzinfo=UTC),
            ]
            mock_batch.get_required_periods = Mock(return_value=dates)
            mock_batch_class.return_value = mock_batch

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # One success, one error, one exception
            service.generate_instruments_for_date = AsyncMock(
                side_effect=[
                    {"status": "success", "date": "2024-01-01", "instruments_generated": 100},
                    {"status": "error", "date": "2024-01-02", "instruments_generated": 0},
                    Exception("Processing failed"),
                ]
            )

            result = await service.generate_instruments_date_range(start_date=dates[0], end_date=dates[2], cefi=True)

            assert result["status"] == "partial"
            assert result["dates_processed"] == 3
            assert result["dates_successful"] == 1
            assert result["dates_failed"] == 2
            assert result["total_instruments_generated"] == 100
            assert result["success_rate_percent"] == pytest.approx(33.3, rel=0.1)


class TestQueryAndStats:
    """Tests for query and statistics methods."""

    def test_query_instruments(self):
        """Test querying stored instruments."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_storage = Mock()
            expected_df = pd.DataFrame({"instrument_key": ["TEST:SPOT:BTC-USDT"], "venue": ["TEST"]})
            mock_storage.query_instruments = Mock(return_value=expected_df)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            result = service.query_instruments(venue="TEST", instrument_type="SPOT")

            mock_storage.query_instruments.assert_called_once_with(venue="TEST", instrument_type="SPOT")
            pd.testing.assert_frame_equal(result, expected_df)

    def test_get_processing_stats(self):
        """Test getting processing statistics."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch_class,
        ):
            mock_proc = Mock()
            mock_proc.get_processing_stats = Mock(return_value={"total_processed": 1000})
            mock_proc_class.return_value = mock_proc

            mock_batch = Mock()
            mock_batch.max_batch_size = 500
            mock_batch.lookback_days = 7
            mock_batch_class.return_value = mock_batch

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            stats = service.get_processing_stats()

            assert "processing_service" in stats
            assert stats["processing_service"]["total_processed"] == 1000
            assert "batch_processor" in stats
            assert stats["batch_processor"]["max_batch_size"] == 500
            assert stats["batch_processor"]["lookback_days"] == 7


class TestCleanup:
    """Tests for cleanup method."""

    def test_cleanup(self):
        """Test cleanup calls processing service cleanup."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.cleanup = Mock()
            mock_proc_class.return_value = mock_proc

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            service.cleanup()

            mock_proc.cleanup.assert_called_once()

    def test_cleanup_without_processing_service(self):
        """Test cleanup handles missing processing service gracefully."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Delete processing_service attribute
            delattr(service, "processing_service")

            # Should not raise exception
            service.cleanup()
        assert True, "Cleanup completed without error"


class TestGenerateInstrumentsEdgeCases:
    """Tests for edge cases in generate_instruments_for_date (from extended)."""

    @pytest.mark.asyncio
    async def test_generate_instruments_date_range_empty_range(self):
        """Test generating instruments for empty date range."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch_class,
        ):
            mock_batch = Mock()
            mock_batch.get_required_periods = Mock(return_value=[])
            mock_batch_class.return_value = mock_batch

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            start_date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_date_range(
                start_date=start_date,
                end_date=start_date,
                cefi=True,
            )

            assert result["dates_processed"] == 0
            assert result["success_rate_percent"] == 0

    @pytest.mark.asyncio
    async def test_generate_instruments_dict_format(self):
        """Test generating instruments when UMI returns list[dict] (InstrumentDefinition schema)."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(
                return_value=[
                    {
                        "instrument_key": "TEST:SPOT:BTC-USDT",
                        "venue": "TEST",
                        "instrument_type": "SPOT_PAIR",
                        "symbol": "BTC-USDT",
                        "available_from_datetime": "2024-01-01T00:00:00Z",
                    }
                ]
            )
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            assert result["status"] in ["success", "warning"]


class TestUpbitCoinbaseIntegration:
    """Tests for Upbit and Coinbase spot venues (kimchi/coinbase premium)."""

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_upbit(self):
        """Test generating instruments in CeFi mode with Upbit exchange."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(
                return_value=[
                    {
                        "instrument_key": "UPBIT:SPOT_PAIR:BTC-KRW",
                        "venue": "UPBIT",
                        "instrument_type": "SPOT_PAIR",
                        "symbol": "BTC-KRW",
                        "available_from_datetime": "2024-01-01T00:00:00Z",
                    }
                ]
            )
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"upbit": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, exchanges=["upbit"], cefi=True)

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            mock_tardis_adapter.fetch_instruments.assert_called()

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_coinbase(self):
        """Test generating instruments in CeFi mode with Coinbase exchange."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(
                return_value=[
                    {
                        "instrument_key": "COINBASE:SPOT_PAIR:BTC-USD",
                        "venue": "COINBASE",
                        "instrument_type": "SPOT_PAIR",
                        "symbol": "BTC-USD",
                        "available_from_datetime": "2024-01-01T00:00:00Z",
                    }
                ]
            )
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"coinbase": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, exchanges=["coinbase"], cefi=True)

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            mock_tardis_adapter.fetch_instruments.assert_called()

    def test_venue_mapping_includes_upbit_coinbase(self):
        """Test venue mapping includes Upbit and Coinbase in Tardis exchanges."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Verify Upbit and Coinbase are in all_tardis_exchanges
            assert "upbit" in service.venue_mapping.all_tardis_exchanges
            assert "coinbase" in service.venue_mapping.all_tardis_exchanges

            # Verify they map to correct venues
            assert service.venue_mapping.tardis_to_venue.get("upbit") == "UPBIT"
            assert service.venue_mapping.tardis_to_venue.get("coinbase") == "COINBASE"

            # Verify they are in spot_mvp_filtered_venues
            assert "UPBIT" in service.venue_mapping.spot_mvp_filtered_venues
            assert "COINBASE" in service.venue_mapping.spot_mvp_filtered_venues
