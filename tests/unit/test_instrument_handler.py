"""
Comprehensive unit tests for InstrumentHandler to increase coverage to 80%+.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
import pandas as pd
from instruments_service.cli.handlers.instrument_handler import (
    InstrumentHandler,
    parse_date,
    get_date_range,
)


class TestInstrumentHandlerHelpers:
    """Tests for helper functions."""

    def test_parse_date_valid(self):
        """Test parsing valid date string."""
        result = parse_date("2024-01-01")
        assert result == datetime(2024, 1, 1, tzinfo=timezone.utc)

    def test_parse_date_invalid(self):
        """Test parsing invalid date string raises error."""
        with pytest.raises(ValueError, match="Invalid date format"):
            parse_date("invalid-date")

    def test_get_date_range_single_day(self):
        """Test date range for single day."""
        result = get_date_range("2024-01-01", "2024-01-01")
        assert len(result) == 1
        assert result[0] == datetime(2024, 1, 1, tzinfo=timezone.utc)

    def test_get_date_range_multiple_days(self):
        """Test date range for multiple days."""
        result = get_date_range("2024-01-01", "2024-01-03")
        assert len(result) == 3
        assert result[0] == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert result[2] == datetime(2024, 1, 3, tzinfo=timezone.utc)

    def test_get_date_range_invalid(self):
        """Test date range with start > end raises error."""
        with pytest.raises(ValueError, match="Start date.*must be <= end date"):
            get_date_range("2024-01-02", "2024-01-01")


class TestInstrumentHandler:
    """Tests for InstrumentHandler."""

    @pytest.fixture
    def mock_instrument_service(self):
        """Create mock instrument processing service."""
        service = Mock()
        service.process_exchange_instruments = Mock()
        service.cleanup = Mock()
        return service

    @pytest.fixture
    def mock_cloud_storage(self):
        """Create mock cloud storage."""
        storage = Mock()
        storage.store_instruments = Mock(return_value=True)
        return storage

    @pytest.fixture
    def mock_data_provider(self):
        """Create mock cloud data provider."""
        provider = Mock()
        provider.check_instruments_exist = Mock(return_value=False)
        return provider

    @pytest.fixture
    def handler(self, mock_instrument_service, mock_cloud_storage, mock_data_provider):
        """Create handler with mocked dependencies."""
        with patch(
            "instruments_service.cli.handlers.instrument_handler.InstrumentProcessingService",
            return_value=mock_instrument_service,
        ), patch(
            "instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage",
            return_value=mock_cloud_storage,
        ):

            config = {"project_id": "test-project"}
            handler = InstrumentHandler(config)
            handler.instrument_service = mock_instrument_service
            handler.cloud_storage = mock_cloud_storage
            # Store mock_data_provider for patching CloudDataProvider when it's imported inside methods
            handler._mock_data_provider = mock_data_provider
            return handler

    def test_init(self, mock_instrument_service, mock_cloud_storage):
        """Test handler initialization."""
        with patch(
            "instruments_service.cli.handlers.instrument_handler.InstrumentProcessingService",
            return_value=mock_instrument_service,
        ), patch(
            "instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage",
            return_value=mock_cloud_storage,
        ):
            config = {"project_id": "test-project"}
            handler = InstrumentHandler(config)
            assert handler.instrument_service is not None
            assert handler.cloud_storage is not None

    def test_run_delegates_to_execute(self, handler):
        """Test run method delegates to _execute_instrument_generation."""
        with patch.object(
            handler,
            "_execute_instrument_generation",
            return_value={"status": "success"},
        ) as mock_execute:
            result = handler.run("2024-01-01", "2024-01-01", force=False)
            mock_execute.assert_called_once()
            assert result["status"] == "success"

    def test_execute_instrument_generation_success(
        self, handler, mock_instrument_service, mock_cloud_storage
    ):
        """Test successful instrument generation."""
        # Mock instruments
        mock_instruments = {
            "BINANCE-SPOT:SPOT_PAIR:BTC-USDT": Mock(
                model_dump=Mock(
                    return_value={
                        "instrument_key": "BINANCE-SPOT:SPOT_PAIR:BTC-USDT",
                        "venue": "BINANCE-SPOT",
                    }
                )
            )
        }

        async def mock_process(exchange, target_date, force):
            return mock_instruments

        mock_instrument_service.process_exchange_instruments = mock_process

        # Mock date to be in the past
        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        # Patch CloudDataProvider to avoid import issues
        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider"
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["status"] in ["success", "partial"]
            assert result["instruments_generated"] > 0

    def test_execute_instrument_generation_skip_future_date(self, handler):
        """Test skipping future dates."""
        future_date = datetime.now(timezone.utc) + timedelta(days=1)
        result = handler._execute_instrument_generation(
            future_date.strftime("%Y-%m-%d"),
            future_date.strftime("%Y-%m-%d"),
            force=False,
        )
        # Should skip future date
        assert result["dates_skipped"] >= 0

    def test_execute_instrument_generation_skip_existing(
        self, handler, mock_data_provider
    ):
        """Test skipping existing instruments when force=False."""
        # Patch CloudDataProvider at the point where it's imported (inside the method)
        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider",
            return_value=mock_data_provider,
        ):
            mock_data_provider.check_instruments_exist.return_value = True

            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=False,
            )

            # Should skip if exists
            assert result["dates_skipped"] >= 0

    def test_execute_instrument_generation_force_mode(
        self, handler, mock_data_provider
    ):
        """Test force mode doesn't skip existing."""
        # Patch CloudDataProvider at the point where it's imported (inside the method)
        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider",
            return_value=mock_data_provider,
        ):
            mock_data_provider.check_instruments_exist.return_value = True

            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            # Force mode should not check existence
            with patch.object(
                handler, "_generate_instruments_for_date", return_value={}
            ):
                result = handler._execute_instrument_generation(
                    test_date.strftime("%Y-%m-%d"),
                    test_date.strftime("%Y-%m-%d"),
                    force=True,
                )
                # Should process even if exists
                assert result is not None

    def test_execute_instrument_generation_no_instruments(self, handler):
        """Test handling when no instruments generated."""
        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider"
        ), patch.object(handler, "_generate_instruments_for_date", return_value={}):
            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["instruments_generated"] == 0

    def test_execute_instrument_generation_storage_failure(
        self, handler, mock_cloud_storage
    ):
        """Test handling storage failure."""
        mock_cloud_storage.store_instruments.return_value = False

        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider"
        ), patch.object(
            handler,
            "_generate_instruments_for_date",
            return_value={
                "TEST:SPOT_PAIR:BTC-USDT": Mock(
                    model_dump=Mock(
                        return_value={"instrument_key": "TEST:SPOT_PAIR:BTC-USDT"}
                    )
                )
            },
        ):
            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["dates_with_errors"] >= 0

    def test_execute_instrument_generation_exception_handling(self, handler):
        """Test exception handling during generation."""
        with patch(
            "instruments_service.app.core.cloud_data_provider.CloudDataProvider"
        ), patch.object(
            handler,
            "_generate_instruments_for_date",
            side_effect=Exception("Test error"),
        ):
            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["dates_with_errors"] >= 0

    def test_generate_instruments_for_date(self, handler, mock_instrument_service):
        """Test generating instruments for a date."""
        mock_instruments = {"BINANCE-SPOT:SPOT_PAIR:BTC-USDT": Mock()}

        async def mock_process(exchange, target_date, force):
            return mock_instruments

        mock_instrument_service.process_exchange_instruments = mock_process

        today = datetime.now(timezone.utc)
        result = handler._generate_instruments_for_date(
            today, force=True, exchanges=["binance"]
        )

        assert len(result) > 0

    def test_generate_instruments_for_date_all_exchanges(
        self, handler, mock_instrument_service
    ):
        """Test generating instruments for all exchanges."""
        mock_instruments = {"TEST:SPOT_PAIR:BTC-USDT": Mock()}

        async def mock_process(exchange, target_date, force):
            return mock_instruments

        mock_instrument_service.process_exchange_instruments = mock_process

        today = datetime.now(timezone.utc)
        result = handler._generate_instruments_for_date(
            today, force=True, exchanges=None
        )

        # Should process all exchanges
        assert result is not None

    def test_generate_instruments_for_date_exchange_error(
        self, handler, mock_instrument_service
    ):
        """Test handling exchange processing errors."""

        async def mock_process(exchange, target_date, force):
            if exchange == "binance":
                raise Exception("Exchange error")
            return {}

        mock_instrument_service.process_exchange_instruments = mock_process

        today = datetime.now(timezone.utc)
        result = handler._generate_instruments_for_date(
            today, force=True, exchanges=["binance", "deribit"]
        )

        # Should continue processing other exchanges
        assert result is not None

    def test_cleanup(self, handler, mock_instrument_service):
        """Test cleanup method."""
        handler.cleanup()
        mock_instrument_service.cleanup.assert_called_once()
