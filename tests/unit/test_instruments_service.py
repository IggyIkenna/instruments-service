"""
Unit tests for InstrumentsService - Main orchestration service.

Tests cover real-world usage scenarios:
- Initialization with various config options
- CeFi/TradFi/DeFi mode processing
- Date range batch processing
- Error handling and recovery
- Resource cleanup
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from instruments_service.app.core.instruments_service import ErrorWarningCounter, InstrumentsService


class TestErrorWarningCounter:
    """Tests for ErrorWarningCounter logging handler (Task 110 coverage)."""

    def test_init(self):
        """Test counter initializes to zero."""
        counter = ErrorWarningCounter()
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_emit_error(self):
        """Test emit counts ERROR records."""
        counter = ErrorWarningCounter()
        record = type("Record", (), {"levelno": 40})()  # ERROR = 40
        record.levelno = 40
        counter.emit(record)
        assert counter.error_count == 1
        assert counter.warning_count == 0

    def test_emit_warning(self):
        """Test emit counts WARNING records."""
        counter = ErrorWarningCounter()
        record = type("Record", (), {"levelno": 30})()  # WARNING = 30
        record.levelno = 30
        counter.emit(record)
        assert counter.error_count == 0
        assert counter.warning_count == 1

    def test_emit_info_ignored(self):
        """Test emit ignores INFO and below."""
        counter = ErrorWarningCounter()
        record = type("Record", (), {"levelno": 20})()  # INFO = 20
        record.levelno = 20
        counter.emit(record)
        assert counter.error_count == 0
        assert counter.warning_count == 0

    def test_reset(self):
        """Test reset clears counts."""
        counter = ErrorWarningCounter()
        record_err = type("Record", (), {"levelno": 40})()
        record_err.levelno = 40
        record_warn = type("Record", (), {"levelno": 30})()
        record_warn.levelno = 30
        counter.emit(record_err)
        counter.emit(record_warn)
        counter.reset()
        assert counter.error_count == 0
        assert counter.warning_count == 0


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
        """Test initialization uses project_id from config when not provided."""
        mock_config = Mock()
        mock_config.gcp_project_id = "test-project"
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.config.instruments_config", mock_config),
        ):
            config = {}  # No project_id
            InstrumentsService(config)

            # Should use project_id from config
            proc_config = mock_proc.call_args[0][0]
            assert proc_config["project_id"] == "test-project"


class TestGenerateInstrumentsSingleDate:
    """Tests for generate_instruments_for_date method."""

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_exchanges(self):
        """Test generating instruments in CeFi mode with specific exchanges.

        CeFi path uses UMI get_adapter + adapter.fetch_instruments (thin consumer).
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            # Setup mocks - CeFi uses UMI get_adapter + fetch_instruments
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(
                return_value=[
                    {
                        "instrument_key": "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                        "venue": "BINANCE-SPOT",
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

            assert result["status"] == "success"
            assert result["instruments_generated"] == 1
            assert result["date"] == "2024-01-01"
            mock_get_adapter.assert_called()
            mock_tardis_adapter.fetch_instruments.assert_called()

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_all_exchanges(self):
        """Test CeFi mode processes all Tardis exchanges when none specified.

        CeFi path uses UMI get_adapter + adapter.fetch_instruments.
        """
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
            mock_tardis_adapter.fetch_instruments = AsyncMock(return_value=[])
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(
                side_effect=lambda exchanges: dict.fromkeys(exchanges, (True, ""))
            )
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            await service.generate_instruments_for_date(
                date=date,
                cefi=True,  # No exchanges specified
            )

            # Should call fetch_instruments for all Tardis exchanges
            assert mock_tardis_adapter.fetch_instruments.call_count == len(service.venue_mapping.all_tardis_exchanges)

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

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, tradfi=True)

            assert result["status"] == "success"
            # Should process CME, NASDAQ, NYSE, ICE, CBOE, FX
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

            date = datetime(2024, 1, 1, tzinfo=UTC)
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

            date = datetime(2024, 1, 1, tzinfo=UTC)
            await service.generate_instruments_for_date(date=date, defi=True, venues=["UNISWAPV3-ETH"])

            # Should only process Uniswap V3
            mock_proc.fetch_defi_instruments.assert_called()
            # Verify only uniswap_v3 was called
            calls = mock_proc.fetch_defi_instruments.call_args_list
            protocols_called = [call[1]["protocol"] for call in calls]
            assert "uniswap_v3" in protocols_called

    @pytest.mark.asyncio
    async def test_generate_tradfi_holiday_placeholder_for_missing_venue(self):
        """When TradFi is run with venue filter and one venue returns no instruments (e.g. holiday),
        a placeholder row is added so a by-venue file is still written and missing-data checks pass."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.UnifiedInstrumentConfig") as mock_config_class,
        ):
            mock_config = Mock()
            mock_config.get_symbols_for_venue.return_value = ["SPY"]
            mock_config_class.return_value = mock_config

            mock_proc = Mock()
            mock_proc.process_exchange_instruments = AsyncMock(return_value={})

            # Return instruments only for NASDAQ, none for NYSE (e.g. NYSE holiday)
            def databento_side_effect(exchange, **kwargs):
                if exchange == "NASDAQ":
                    return {
                        "NASDAQ:ETF:SPY-USD": Mock(
                            model_dump=lambda: {
                                "instrument_key": "NASDAQ:ETF:SPY-USD",
                                "venue": "NASDAQ",
                                "instrument_type": "ETF",
                                "symbol": "SPY-USD",
                                "available_from_datetime": "2024-01-01T00:00:00Z",
                                "market_category": "TRADFI",
                            }
                        )
                    }
                return {}

            mock_proc.fetch_databento_instruments = AsyncMock(side_effect=databento_side_effect)
            mock_proc.fetch_defi_instruments = Mock(return_value={})
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            stored_dfs = []

            def capture_store(instruments_df=None, **kwargs):
                stored_dfs.append(instruments_df)
                return True

            mock_storage.store_instruments = Mock(side_effect=capture_store)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, tradfi=True, venues=["NYSE", "NASDAQ"])

            assert result["status"] == "success"
            assert len(stored_dfs) == 1
            df = stored_dfs[0]
            assert "venue" in df.columns
            assert "NYSE" in df["venue"].values
            assert "NASDAQ" in df["venue"].values
            nyse_rows = df[df["venue"] == "NYSE"]
            assert len(nyse_rows) == 1
            assert nyse_rows.iloc[0]["instrument_type"] == "MARKET_CLOSED"
            assert nyse_rows.iloc[0]["is_trading_day"] is False
            assert nyse_rows.iloc[0].get("trading_hours_open") == "holiday"

    @pytest.mark.asyncio
    async def test_generate_no_mode_specified_processes_all(self):
        """Test that when no mode flags are specified, all modes are processed."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.UnifiedInstrumentConfig"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_databento_instruments = Mock(return_value={})
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(return_value=[])
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(
                side_effect=lambda exchanges: dict.fromkeys(exchanges, (True, ""))
            )
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            await service.generate_instruments_for_date(date=date)

            # CeFi uses UMI get_adapter + fetch_instruments; TradFi and DeFi use processing_service
            assert mock_tardis_adapter.fetch_instruments.call_count > 0
            assert mock_proc.fetch_databento_instruments.call_count > 0
            assert mock_proc.fetch_defi_instruments.call_count > 0

    @pytest.mark.asyncio
    async def test_generate_no_instruments_warning(self):
        """Test warning when no instruments are generated."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(return_value=[])  # No instruments
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            # CeFi with no instruments = ERROR (unexpected)
            assert result["status"] == "error"
            assert result["instruments_generated"] == 0
            assert "message" in result

    @pytest.mark.asyncio
    async def test_generate_storage_failure(self):
        """Test handling of storage failure."""
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
            mock_storage.store_instruments = Mock(return_value=False)  # Storage fails
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
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
            patch("instruments_service.app.core.instruments_service.get_adapter") as mock_get_adapter,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = Mock(return_value={})

            # First exchange (binance) fails, second (deribit) succeeds
            async def fetch_side_effect(exchange, **kwargs):
                if exchange == "binance":
                    raise Exception("API error")
                return [
                    {
                        "instrument_key": "TEST:SPOT:BTC-USDT",
                        "venue": "TEST",
                        "instrument_type": "SPOT_PAIR",
                        "symbol": "BTC-USDT",
                        "available_from_datetime": "2024-01-01T00:00:00Z",
                    }
                ]

            mock_tardis_adapter = Mock()
            mock_tardis_adapter.fetch_instruments = AsyncMock(side_effect=fetch_side_effect)
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, ""), "deribit": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance", "deribit"], cefi=True)

            # Should still succeed with instruments from deribit
            assert result["status"] == "success"
            assert result["instruments_generated"] == 1

    @pytest.mark.asyncio
    async def test_generate_force_mode(self):
        """Test force regeneration flag is passed through to UMI adapter.fetch_instruments."""
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
            mock_tardis_adapter.fetch_instruments = AsyncMock(return_value=[])
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
            await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True, force=True)

            # Verify force_refresh=True was passed to adapter.fetch_instruments
            mock_tardis_adapter.fetch_instruments.assert_called()
            call_kwargs = mock_tardis_adapter.fetch_instruments.call_args[1]
            assert call_kwargs.get("force_refresh") is True
