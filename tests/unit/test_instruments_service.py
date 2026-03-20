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
            patch("instruments_service.app.core.instruments_service.instruments_config", mock_config),
        ):
            config = {}  # No project_id
            InstrumentsService(config)

            # Should use project_id from config
            proc_config = mock_proc.call_args[0][0]
            assert proc_config["project_id"] == "test-project"


def _build_all_mode_mocks() -> dict[str, Mock]:
    """Build mock objects for processing service, tardis adapter, and storage.

    Returns a dict with keys: proc, tardis_adapter, storage.
    """
    mock_proc = Mock()
    mock_proc.api_key = "test-key"
    mock_proc._tardis_project_id = "test-project"
    mock_proc.fetch_databento_instruments = AsyncMock(return_value={})
    mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

    mock_tardis_adapter = Mock()
    mock_tardis_adapter.fetch_instruments = AsyncMock(return_value=[])
    mock_base_client = Mock()
    mock_base_client.check_venues_access = Mock(side_effect=lambda exchanges: dict.fromkeys(exchanges, (True, "")))
    mock_tardis_adapter.base_client = mock_base_client

    mock_storage = Mock()
    mock_storage.store_instruments = Mock(return_value=True)

    return {"proc": mock_proc, "tardis_adapter": mock_tardis_adapter, "storage": mock_storage}


def _make_tradfi_record(
    raw_symbol: str = "ES.FUT",
    instrument_type: str = "FUTURE",
    base: str = "",
    quote: str = "USD",
) -> Mock:
    """Mock InstrumentRecord with all attributes consumed by _map_instrument_record_to_tradfi_definition."""
    rec = Mock()
    rec.raw_symbol = raw_symbol
    rec.symbol = raw_symbol
    rec.instrument_type = instrument_type
    rec.available_since = datetime(2024, 1, 1, tzinfo=UTC)
    rec.updated_at = None
    rec.base_asset = base
    rec.base = base
    rec.quote_asset = quote
    rec.quote = quote
    rec.expiry = None
    rec.option_type = None
    rec.strike = None
    rec.tick_size = None
    rec.contract_size = None
    return rec


def _make_cefi_record(
    instrument_key: str = "BINANCE:SPOT_PAIR:BTC-USDT",
    venue: str = "BINANCE",
    base: str = "BTC",
    quote: str = "USDT",
) -> Mock:
    """Mock InstrumentRecord with all attributes consumed by _fetch_tardis_instruments."""
    rec = Mock()
    rec.instrument_key = instrument_key
    rec.venue = venue
    rec.instrument_type = "SPOT_PAIR"
    rec.symbol = instrument_key.split(":", 2)[2] if instrument_key.count(":") >= 2 else instrument_key
    rec.raw_symbol = rec.symbol.replace("-", "").replace("/", "")
    rec.available_since = datetime(2024, 1, 1, tzinfo=UTC)
    rec.base_asset = base
    rec.base = base
    rec.quote_asset = quote
    rec.quote = quote
    rec.expiry = None
    rec.option_type = None
    rec.strike = None
    rec.tick_size = None
    rec.contract_size = None
    return rec


class TestGenerateInstrumentsSingleDate:
    """Tests for generate_instruments_for_date method."""

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_with_exchanges(self):
        """Test generating instruments in CeFi mode with specific exchanges.

        CeFi fetch uses URDI TardisReferenceDataAdapter.get_instruments().
        UMI get_adapter is used only for the pre-flight venue access check.
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            # get_adapter: covers _check_venue_access pre-flight only
            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            # TardisReferenceDataAdapter: actual data boundary for _fetch_tardis_instruments
            mock_urdi_cls.return_value.get_instruments = AsyncMock(
                return_value=[_make_cefi_record("BINANCE:SPOT_PAIR:BTC-USDT", "BINANCE")]
            )

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
            mock_urdi_cls.return_value.get_instruments.assert_called()

    @pytest.mark.asyncio
    async def test_generate_cefi_mode_all_exchanges(self):
        """Test CeFi mode processes all Tardis exchanges when none specified.

        CeFi fetch uses URDI TardisReferenceDataAdapter — one instance per exchange.
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(
                side_effect=lambda exchanges: dict.fromkeys(exchanges, (True, ""))
            )
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_urdi_cls.return_value.get_instruments = AsyncMock(return_value=[])

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

            # One TardisReferenceDataAdapter instantiation per exchange
            assert mock_urdi_cls.call_count == len(service.venue_mapping.all_tardis_exchanges)

    @pytest.mark.asyncio
    async def test_generate_tradfi_mode(self):
        """Test generating instruments in TradFi (Databento) mode.

        TradFi fetch uses URDI DatabentoReferenceDataAdapter.get_instruments().
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.UnifiedInstrumentConfig") as mock_config,
            patch("instruments_service.app.core.instrument_sync.DatabentoReferenceDataAdapter") as mock_tradfi_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            mock_config_inst = Mock()
            mock_config_inst.get_symbols_for_venue = Mock(return_value=["ES.FUT"])
            mock_config.return_value = mock_config_inst

            # DatabentoReferenceDataAdapter: actual data boundary for _fetch_databento_instruments_via_urdi
            mock_tradfi_urdi_cls.return_value.get_instruments = AsyncMock(
                return_value=[_make_tradfi_record("ES.FUT", "FUTURE")]
            )

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            result = await service.generate_instruments_for_date(date=date, tradfi=True)

            assert result["status"] == "success"
            # One DatabentoReferenceDataAdapter instantiation per dataset-mapped exchange
            assert mock_tradfi_urdi_cls.call_count >= 1

    @pytest.mark.asyncio
    async def test_generate_defi_mode(self):
        """Test generating instruments in DeFi mode."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            mock_proc = Mock()
            mock_proc.fetch_defi_instruments = AsyncMock(
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
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})
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
            patch("instruments_service.app.core.instrument_sync.UnifiedInstrumentConfig") as mock_config_class,
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
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})
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
            assert not nyse_rows.iloc[0]["is_trading_day"]
            assert nyse_rows.iloc[0].get("trading_hours_open") == "holiday"

    @pytest.mark.asyncio
    async def test_generate_no_mode_specified_processes_all(self):
        """Test that when no mode flags are specified, all modes are processed."""
        import sys
        import types

        mock_sports_module = types.ModuleType(
            "instruments_service.engine.operations.instruments.orchestration.sports_orchestration"
        )
        mock_sports_orchestrator = Mock()
        mock_sports_orchestrator.process_sports = AsyncMock(return_value={})
        mock_sports_module.SportsOrchestrator = Mock(return_value=mock_sports_orchestrator)  # type: ignore[attr-defined]

        mocks = _build_all_mode_mocks()

        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.UnifiedInstrumentConfig"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
            patch("instruments_service.app.core.instrument_sync.DatabentoReferenceDataAdapter") as mock_tradfi_urdi_cls,
            patch.dict(
                sys.modules,
                {
                    "instruments_service.engine.operations.instruments.orchestration.sports_orchestration": mock_sports_module,
                },
            ),
        ):
            mock_get_adapter.return_value = mocks["tardis_adapter"]
            mock_proc_class.return_value = mocks["proc"]
            mock_storage_class.return_value = mocks["storage"]
            mock_urdi_cls.return_value.get_instruments = AsyncMock(return_value=[])
            mock_tradfi_urdi_cls.return_value.get_instruments = AsyncMock(return_value=[])

            service = InstrumentsService({"project_id": "test-project"})
            await service.generate_instruments_for_date(date=datetime(2024, 1, 1, tzinfo=UTC))

            # CeFi: one TardisReferenceDataAdapter per exchange
            assert mock_urdi_cls.call_count > 0
            # TradFi: one DatabentoReferenceDataAdapter per dataset-mapped exchange
            assert mock_tradfi_urdi_cls.call_count > 0
            # DeFi: processing_service.fetch_defi_instruments
            assert mocks["proc"].fetch_defi_instruments.call_count > 0

    @pytest.mark.asyncio
    async def test_generate_no_instruments_warning(self):
        """Test warning when no instruments are generated."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_urdi_cls.return_value.get_instruments = AsyncMock(return_value=[])  # No instruments

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
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_urdi_cls.return_value.get_instruments = AsyncMock(
                return_value=[_make_cefi_record("TEST:SPOT:BTC-USDT", "TEST", "BTC", "USDT")]
            )

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
        """Test error handling when exchange processing fails.

        binance URDI get_instruments raises ValueError — caught per-venue.
        deribit URDI get_instruments succeeds — result carries through.
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, ""), "deribit": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            # Per-exchange side_effect: binance fails, deribit succeeds
            deribit_record = _make_cefi_record("DERIBIT:PERPETUAL:BTC-USDT", "DERIBIT")

            def urdi_constructor_side_effect(*args: object, **kwargs: object) -> Mock:
                exchange_list = kwargs.get("exchanges", [])
                exchange = exchange_list[0] if exchange_list else None
                mock_inst = Mock()
                if exchange == "binance":
                    mock_inst.get_instruments = AsyncMock(side_effect=ValueError("API error"))
                else:
                    mock_inst.get_instruments = AsyncMock(return_value=[deribit_record])
                return mock_inst

            mock_urdi_cls.side_effect = urdi_constructor_side_effect

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
        """Test force=True triggers URDI TardisReferenceDataAdapter fetch.

        The force flag is accepted by generate_instruments_for_date and propagated to
        _fetch_tardis_instruments. URDI get_instruments() does not expose a force_refresh
        kwarg — the adapter manages caching internally.
        """
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
            patch("instruments_service.app.core.instrument_sync.get_adapter") as mock_get_adapter,
            patch("instruments_service.app.core.instrument_sync.TardisReferenceDataAdapter") as mock_urdi_cls,
        ):
            mock_proc = Mock()
            mock_proc.api_key = "test-key"
            mock_proc._tardis_project_id = "test-project"
            mock_proc.fetch_defi_instruments = AsyncMock(return_value={})

            mock_tardis_adapter = Mock()
            mock_base_client = Mock()
            mock_base_client.check_venues_access = Mock(return_value={"binance": (True, "")})
            mock_tardis_adapter.base_client = mock_base_client
            mock_get_adapter.return_value = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_urdi_cls.return_value.get_instruments = AsyncMock(return_value=[])

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=UTC)
            await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True, force=True)

            # URDI adapter was instantiated (force path still calls URDI)
            mock_urdi_cls.assert_called()
            mock_urdi_cls.return_value.get_instruments.assert_called()
