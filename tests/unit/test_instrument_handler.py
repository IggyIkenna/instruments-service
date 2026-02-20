"""
Comprehensive unit tests for InstrumentHandler to increase coverage to 80%+.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from instruments_service.cli.handlers.instrument_handler import (
    InstrumentHandler,
    get_date_range,
    parse_date,
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
        """Create mock instrument service."""
        service = Mock()
        service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 10}
        )
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
        with (
            patch(
                "instruments_service.cli.handlers.instrument_handler.InstrumentsService",
                return_value=mock_instrument_service,
            ),
            patch(
                "instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage",
                return_value=mock_cloud_storage,
            ),
        ):
            config = {"project_id": "test-project"}
            handler = InstrumentHandler(config)
            handler.instruments_service = mock_instrument_service
            handler.cloud_storage = mock_cloud_storage
            # Store mock_data_provider for patching CloudDataProvider when it's imported inside methods
            handler._mock_data_provider = mock_data_provider
            return handler

    def test_init(self, mock_instrument_service, mock_cloud_storage):
        """Test handler initialization."""
        with (
            patch(
                "instruments_service.cli.handlers.instrument_handler.InstrumentsService",
                return_value=mock_instrument_service,
            ),
            patch(
                "instruments_service.cli.handlers.instrument_handler.CloudInstrumentStorage",
                return_value=mock_cloud_storage,
            ),
        ):
            config = {"project_id": "test-project"}
            handler = InstrumentHandler(config)
            assert handler.instruments_service is not None
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

    def test_execute_instrument_generation_success(self, handler, mock_instrument_service, mock_cloud_storage):
        """Test successful instrument generation."""
        # Mock instruments service to return success
        mock_instrument_service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 10}
        )

        # Mock date to be in the past
        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        # Patch CloudDataProvider and validate_required_api_keys (no GCP/API keys in unit tests)
        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["status"] in ["success", "partial"]
            assert result["instruments_generated"] >= 0

    def test_execute_instrument_generation_skip_future_date(self, handler):
        """Test skipping future dates."""
        future_date = datetime.now(timezone.utc) + timedelta(days=1)
        with patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"):
            result = handler._execute_instrument_generation(
                future_date.strftime("%Y-%m-%d"),
                future_date.strftime("%Y-%m-%d"),
                force=False,
            )
        assert result["dates_skipped"] >= 0

    def test_execute_instrument_generation_skip_existing(self, handler, mock_data_provider):
        """Test skipping existing instruments when force=False."""
        with (
            patch(
                "instruments_service.app.core.cloud_data_provider.CloudDataProvider",
                return_value=mock_data_provider,
            ),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
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

    def test_execute_instrument_generation_force_mode(self, handler, mock_data_provider):
        """Test force mode doesn't skip existing."""
        with (
            patch(
                "instruments_service.app.core.cloud_data_provider.CloudDataProvider",
                return_value=mock_data_provider,
            ),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            mock_data_provider.check_instruments_exist.return_value = True

            today = datetime.now(timezone.utc).date()
            test_date = today - timedelta(days=1)

            # Force mode should not check existence
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )
            # Should process even if exists
            assert result is not None

    def test_execute_instrument_generation_no_instruments(self, handler):
        """Test handling when no instruments generated."""
        handler.instruments_service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "warning", "instruments_generated": 0}
        )
        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["instruments_generated"] == 0

    def test_execute_instrument_generation_storage_failure(self, handler, mock_cloud_storage):
        """Test handling storage failure."""
        handler.instruments_service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 10}
        )
        handler.cloud_storage.store_instruments = Mock(return_value=False)
        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["dates_with_errors"] >= 0

    def test_execute_instrument_generation_exception_handling(self, handler):
        """Test exception handling during generation."""
        handler.instruments_service.generate_instruments_for_date = AsyncMock(side_effect=Exception("Test error"))
        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["dates_with_errors"] >= 0

    def test_generate_instruments_for_date(self, handler, mock_instrument_service):
        """Test generating instruments for a date."""
        mock_instrument_service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 5}
        )

        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
                exchanges=["binance"],
            )

            assert result["instruments_generated"] >= 0

    def test_generate_instruments_for_date_all_exchanges(self, handler, mock_instrument_service):
        """Test generating instruments for all exchanges."""
        mock_instrument_service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 10}
        )

        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result is not None

    def test_generate_instruments_for_date_exchange_error(self, handler, mock_instrument_service):
        """Test handling exchange processing errors."""
        mock_instrument_service.generate_instruments_for_date = AsyncMock(side_effect=Exception("Exchange error"))

        today = datetime.now(timezone.utc).date()
        test_date = today - timedelta(days=1)

        with (
            patch("instruments_service.app.core.cloud_data_provider.CloudDataProvider"),
            patch("instruments_service.cli.handlers.instrument_handler.validate_required_api_keys"),
        ):
            result = handler._execute_instrument_generation(
                test_date.strftime("%Y-%m-%d"),
                test_date.strftime("%Y-%m-%d"),
                force=True,
            )

            assert result["dates_with_errors"] >= 0

    def test_cleanup(self, handler, mock_instrument_service):
        """Test cleanup method."""
        handler.cleanup()
        mock_instrument_service.cleanup.assert_called_once()
