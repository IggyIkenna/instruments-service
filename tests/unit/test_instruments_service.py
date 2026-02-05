"""
Unit tests for InstrumentsService - Main orchestration service.

Tests cover real-world usage scenarios:
- Initialization with various config options
- CeFi/TradFi/DeFi mode processing
- Date range batch processing
- Error handling and recovery
- Resource cleanup
"""

import pytest
import pandas as pd
from datetime import datetime, timezone
from unittest.mock import Mock, AsyncMock, patch

from instruments_service.app.core.instruments_service import InstrumentsService


class TestInstrumentsServiceInitialization:
    """Tests for service initialization."""

    def test_initialization_minimal_config(self):
        """Test initialization with minimal configuration."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService"),
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            assert service.config == config
            assert hasattr(service, "processing_service")
            assert hasattr(service, "cloud_storage")
            assert hasattr(service, "batch_processor")
            assert hasattr(service, "venue_mapping")

    def test_initialization_full_config(self):
        """Test initialization with full configuration."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor") as mock_batch,
        ):
            config = {
                "project_id": "test-project",
                "gcs_bucket": "test-bucket",
                "bigquery_dataset": "test-dataset",
                "enable_ccxt_integration": False,
                "enable_metadata_caching": False,
                "max_batch_size": 500,
                "lookback_days": 7,
            }
            InstrumentsService(config)

            # Verify processing service initialized with correct config
            mock_proc.assert_called_once()
            proc_config = mock_proc.call_args[0][0]
            assert proc_config["project_id"] == "test-project"
            assert proc_config["enable_ccxt_integration"] is False
            assert proc_config["enable_metadata_caching"] is False

            # Verify batch processor initialized with correct config
            mock_batch.assert_called_once()
            batch_config = mock_batch.call_args[0][0]
            assert batch_config["max_batch_size"] == 500
            assert batch_config["lookback_days"] == 7

    def test_initialization_default_project_id(self):
        """Test initialization uses default project ID if not provided."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            config = {}  # No project_id
            InstrumentsService(config)

            # Should use default project_id
            proc_config = mock_proc.call_args[0][0]
            assert proc_config["project_id"] == "central-element-323112"


class TestGenerateInstrumentsSingleDate:
    """Tests for generate_instruments_for_date method."""

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_exchanges(self):
        """Test generating instruments in CeFi mode with specific exchanges."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            # Setup mocks
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(
                return_value={
                    "BINANCE-SPOT:SPOT_PAIR:BTC-USDT": Mock(
                        model_dump=lambda: {
                            "instrument_key": "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                            "venue": "BINANCE-SPOT",
                        }
                    )
                }
            )
            # Mock tardis_adapter.check_venues_access to return access results dict
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"binance": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Test CeFi mode with specific exchanges
            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            assert result["date"] == "2024-01-01"
            mock_proc.process_exchange_instruments.assert_called_once_with(
                exchange="binance", target_date=date, force=False
            )

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_all_exchanges(self):
        """Test CeFi mode processes all Tardis exchanges when none specified."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(return_value={})
            # Mock tardis_adapter.check_venues_access - return all accessible
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(
                side_effect=lambda exchanges: {ex: (True, None) for ex in exchanges}
            )
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            await service.generate_instruments_for_date(
                date=date,
                cefi=True,  # No exchanges specified
            )

            # Should call process_exchange_instruments for all Tardis exchanges
            assert mock_proc.process_exchange_instruments.call_count == len(service.venue_mapping.all_tardis_exchanges)

    @pytest.mark.asyncio
    async def test_generate_tradfi_mode(self):
        """Test generating instruments in TradFi (Databento) mode."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.UnifiedInstrumentConfig") as mock_config,
        ):
            # Setup mocks
            mock_proc = Mock()
            mock_proc.fetch_databento_instruments = Mock(
                return_value={
                    "CME:FUTURE:ES.FUT": Mock(
                        model_dump=lambda: {"instrument_key": "CME:FUTURE:ES.FUT", "venue": "CME"}
                    )
                }
            )
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            # Mock UnifiedInstrumentConfig
            mock_config_inst = Mock()
            mock_config_inst.get_symbols_for_dataset = Mock(return_value=["ES.FUT", "NQ.FUT"])
            mock_config_inst.get_symbols_for_venue = Mock(return_value=["ES.FUT", "GC.FUT"])
            mock_config.return_value = mock_config_inst

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, tradfi=True)

            assert result["status"] == "success"
            # Should process CME, NASDAQ, NYSE, ICE, CBOE, YAHOO_FINANCE
            # But NASDAQ/NYSE share DBEQ.BASIC, so NYSE is skipped
            assert mock_proc.fetch_databento_instruments.call_count >= 1

    @pytest.mark.asyncio
    async def test_generate_defi_mode(self):
        """Test generating instruments in DeFi mode."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.fetch_defi_instruments = Mock(
                return_value={
                    "UNISWAPV3-ETH:SPOT_PAIR:ETH-USDC": Mock(
                        model_dump=lambda: {
                            "instrument_key": "UNISWAPV3-ETH:SPOT_PAIR:ETH-USDC",
                            "venue": "UNISWAPV3-ETH",
                        }
                    )
                }
            )
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, defi=True)

            assert result["status"] == "success"
            # Should process all DeFi protocols
            assert mock_proc.fetch_defi_instruments.call_count >= 1

    @pytest.mark.asyncio
    async def test_generate_defi_mode_with_venue_filter(self):
        """Test DeFi mode with venue filter."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.fetch_defi_instruments = Mock(return_value={})
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            await service.generate_instruments_for_date(date=date, defi=True, venues=["UNISWAPV3-ETH"])

            # Should only process Uniswap V3
            mock_proc.fetch_defi_instruments.assert_called()
            # Verify only uniswap_v3 was called
            calls = mock_proc.fetch_defi_instruments.call_args_list
            protocols_called = [call[1]["protocol"] for call in calls]
            assert "uniswap_v3" in protocols_called

    @pytest.mark.asyncio
    async def test_generate_no_mode_specified_processes_all(self):
        """Test that when no mode flags are specified, all modes are processed."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.UnifiedInstrumentConfig"),
        ):
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(return_value={})
            mock_proc.fetch_databento_instruments = Mock(return_value={})
            mock_proc.fetch_defi_instruments = Mock(return_value={})
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(
                side_effect=lambda exchanges: {ex: (True, None) for ex in exchanges}
            )
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            await service.generate_instruments_for_date(date=date)

            # Should call all three processing methods
            assert mock_proc.process_exchange_instruments.call_count > 0
            assert mock_proc.fetch_databento_instruments.call_count > 0
            assert mock_proc.fetch_defi_instruments.call_count > 0

    @pytest.mark.asyncio
    async def test_generate_no_instruments_warning(self):
        """Test warning when no instruments are generated."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(return_value={})  # No instruments
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"binance": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            assert result["status"] == "warning"
            assert result["instruments_generated"] == 0
            assert "message" in result

    @pytest.mark.asyncio
    async def test_generate_storage_failure(self):
        """Test handling of storage failure."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(
                return_value={
                    "TEST:SPOT:BTC-USDT": Mock(
                        model_dump=lambda: {"instrument_key": "TEST:SPOT:BTC-USDT", "venue": "TEST"}
                    )
                }
            )
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"binance": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=False)  # Storage fails
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            assert result["status"] == "error"
            assert result["instruments_generated"] == 1
            assert "Storage failed" in result["message"]

    @pytest.mark.asyncio
    async def test_generate_exchange_processing_error(self):
        """Test error handling when exchange processing fails."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            # First exchange fails, second succeeds
            mock_proc.process_exchange_instruments = AsyncMock(
                side_effect=[
                    Exception("API error"),
                    {
                        "TEST:SPOT:BTC-USDT": Mock(
                            model_dump=lambda: {
                                "instrument_key": "TEST:SPOT:BTC-USDT",
                                "venue": "TEST",
                            }
                        )
                    },
                ]
            )
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(
                return_value={"binance": (True, None), "deribit": (True, None)}
            )
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance", "deribit"], cefi=True)

            # Should still succeed with instruments from deribit
            assert result["status"] == "success"
            assert result["instruments_generated"] == 1

    @pytest.mark.asyncio
    async def test_generate_force_mode(self):
        """Test force regeneration flag is passed through."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(return_value={})
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"binance": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True, force=True)

            # Verify force=True was passed
            mock_proc.process_exchange_instruments.assert_called_once_with(
                exchange="binance", target_date=date, force=True
            )


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
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
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
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
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
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
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


class TestUpbitCoinbaseIntegration:
    """Tests for Upbit and Coinbase spot venues (kimchi/coinbase premium)."""

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_upbit(self):
        """Test generating instruments in CeFi mode with Upbit exchange."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            # Setup mocks
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(
                return_value={
                    "UPBIT:SPOT_PAIR:BTC-KRW": Mock(
                        model_dump=lambda: {
                            "instrument_key": "UPBIT:SPOT_PAIR:BTC-KRW",
                            "venue": "UPBIT",
                        }
                    )
                }
            )
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"upbit": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Test CeFi mode with Upbit
            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["upbit"], cefi=True)

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            mock_proc.process_exchange_instruments.assert_called_once_with(
                exchange="upbit", target_date=date, force=False
            )

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_coinbase(self):
        """Test generating instruments in CeFi mode with Coinbase exchange."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            # Setup mocks
            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(
                return_value={
                    "COINBASE:SPOT_PAIR:BTC-USD": Mock(
                        model_dump=lambda: {
                            "instrument_key": "COINBASE:SPOT_PAIR:BTC-USD",
                            "venue": "COINBASE",
                        }
                    )
                }
            )
            # Mock tardis_adapter.check_venues_access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access = Mock(return_value={"coinbase": (True, None)})
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            # Test CeFi mode with Coinbase
            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["coinbase"], cefi=True)

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            mock_proc.process_exchange_instruments.assert_called_once_with(
                exchange="coinbase", target_date=date, force=False
            )

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
