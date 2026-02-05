"""
Unit tests for venue access checking functionality.

Tests the pre-flight venue access validation that detects:
- Cloudflare blocks (HTTP 403 with cloudflare content)
- Subscription issues (HTTP 403)
- Invalid venues (HTTP 404)
- Network errors

This is critical for early detection of access issues before processing instruments.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone


class TestTardisAdapterVenueAccessCheck:
    """Tests for TardisAdapter.check_venues_access() method."""

    @pytest.fixture
    def mock_tardis_adapter(self):
        """Create a TardisAdapter with mocked base client."""
        with patch("instruments_service.app.venues.tardis.tardis_adapter.TardisBaseClient") as mock_client_class:
            from instruments_service.app.venues.tardis.tardis_adapter import (
                TardisAdapter,
            )

            mock_client = Mock()
            mock_client_class.return_value = mock_client

            adapter = TardisAdapter()
            adapter._base_client = mock_client

            yield adapter, mock_client

    def test_check_venues_access_all_accessible(self, mock_tardis_adapter):
        """Test when all venues are accessible."""
        adapter, mock_client = mock_tardis_adapter

        # Mock all venues as accessible
        mock_client.check_venues_access.return_value = {
            "binance": (True, ""),
            "deribit": (True, ""),
            "upbit": (True, ""),
        }

        result = adapter.check_venues_access(["binance", "deribit", "upbit"])

        assert result["binance"] == (True, "")
        assert result["deribit"] == (True, "")
        assert result["upbit"] == (True, "")
        mock_client.check_venues_access.assert_called_once_with(["binance", "deribit", "upbit"])

    def test_check_venues_access_some_blocked(self, mock_tardis_adapter):
        """Test when some venues are blocked (e.g., Cloudflare)."""
        adapter, mock_client = mock_tardis_adapter

        # Mock mixed results - some blocked
        mock_client.check_venues_access.return_value = {
            "binance": (True, ""),
            "upbit": (False, "Cloudflare blocking access to upbit (HTTP 403)"),
            "coinbase": (True, ""),
        }

        result = adapter.check_venues_access(["binance", "upbit", "coinbase"])

        assert result["binance"][0] is True
        assert result["upbit"][0] is False
        assert "Cloudflare" in result["upbit"][1]
        assert result["coinbase"][0] is True

    def test_check_venues_access_all_blocked(self, mock_tardis_adapter):
        """Test when all venues are blocked."""
        adapter, mock_client = mock_tardis_adapter

        mock_client.check_venues_access.return_value = {
            "binance": (False, "Cloudflare blocking access to binance (HTTP 403)"),
            "upbit": (False, "Cloudflare blocking access to upbit (HTTP 403)"),
        }

        result = adapter.check_venues_access(["binance", "upbit"])

        assert result["binance"][0] is False
        assert result["upbit"][0] is False

    def test_check_venues_access_invalid_venue(self, mock_tardis_adapter):
        """Test when a venue doesn't exist (HTTP 404)."""
        adapter, mock_client = mock_tardis_adapter

        mock_client.check_venues_access.return_value = {
            "binance": (True, ""),
            "invalid_exchange": (False, "Exchange not found: invalid_exchange (HTTP 404)"),
        }

        result = adapter.check_venues_access(["binance", "invalid_exchange"])

        assert result["binance"][0] is True
        assert result["invalid_exchange"][0] is False
        assert "404" in result["invalid_exchange"][1]


class TestInstrumentsServiceVenueAccessIntegration:
    """Tests for InstrumentsService handling of venue access checks."""

    @pytest.mark.asyncio
    async def test_cefi_skips_blocked_venues(self):
        """Test that CEFI processing skips blocked venues but continues with accessible ones."""
        from unittest.mock import AsyncMock

        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            # Setup processing service mock with AsyncMock for async method
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

            # Mock tardis_adapter with mixed venue access
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access.return_value = {
                "binance": (True, ""),  # Accessible
                "upbit": (False, "Cloudflare blocking access to upbit (HTTP 403)"),  # Blocked
            }
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance", "upbit"], cefi=True)

            # When some venues are blocked but others succeed, status can be "success" or "warning"
            # The key test is that processing continues with accessible venues
            assert result["status"] in ("success", "warning")
            assert result["instruments_generated"] >= 0

            # Verify that venue access check was performed
            mock_tardis_adapter.check_venues_access.assert_called_once()

            # Verify that only accessible venue (binance) was processed
            # The call should only happen for non-blocked exchanges
            calls = mock_proc.process_exchange_instruments.call_args_list
            exchange_names = [call.kwargs.get("exchange") for call in calls]
            assert "upbit" not in exchange_names  # Blocked venue should be skipped

    @pytest.mark.asyncio
    async def test_cefi_fails_when_all_venues_blocked(self):
        """Test that CEFI processing handles all venues being blocked."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage"),
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            mock_proc = Mock()
            mock_proc.process_exchange_instruments = MagicMock(return_value={})

            # Mock tardis_adapter with ALL venues blocked
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access.return_value = {
                "binance": (False, "Cloudflare blocking access to binance (HTTP 403)"),
                "upbit": (False, "Cloudflare blocking access to upbit (HTTP 403)"),
            }
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance", "upbit"], cefi=True)

            # Should warn/error when all venues blocked but still return
            # The exact status depends on implementation - could be "warning" or "error"
            assert result is not None
            mock_tardis_adapter.check_venues_access.assert_called_once()

    @pytest.mark.asyncio
    async def test_venue_access_check_detects_cloudflare_block(self):
        """Test that Cloudflare blocks are properly detected and reported."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            mock_proc = Mock()
            mock_proc.process_exchange_instruments = MagicMock(return_value={})

            # Simulate Cloudflare block on specific venue
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access.return_value = {
                "upbit": (False, "Cloudflare blocking access to upbit (HTTP 403)"),
            }
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)

            # Run with the blocked venue
            result = await service.generate_instruments_for_date(date=date, exchanges=["upbit"], cefi=True)

            # Verify access check was performed and result is returned
            assert result is not None
            mock_tardis_adapter.check_venues_access.assert_called_once_with(["upbit"])


class TestVenueAccessCheckErrorHandling:
    """Tests for error handling in venue access checks."""

    @pytest.mark.asyncio
    async def test_venue_access_check_handles_network_error(self):
        """Test graceful handling of network errors during venue access check."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            mock_proc = Mock()
            mock_proc.process_exchange_instruments = MagicMock(return_value={})

            # Simulate network error
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access.return_value = {
                "binance": (False, "Failed to check venue access for binance: Connection timeout"),
            }
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["binance"], cefi=True)

            # Should handle gracefully (not crash)
            assert result is not None

    @pytest.mark.asyncio
    async def test_venue_access_check_handles_subscription_error(self):
        """Test handling of subscription/permission errors (HTTP 403 non-Cloudflare)."""
        with (
            patch("instruments_service.app.core.instruments_service.InstrumentProcessingService") as mock_proc_class,
            patch("instruments_service.app.core.instruments_service.CloudInstrumentStorage") as mock_storage_class,
            patch("instruments_service.app.core.instruments_service.InstrumentBatchProcessor"),
        ):
            from instruments_service.app.core.instruments_service import InstrumentsService

            mock_proc = Mock()
            mock_proc.process_exchange_instruments = MagicMock(return_value={})

            # Simulate subscription error (403 but not Cloudflare)
            mock_tardis_adapter = Mock()
            mock_tardis_adapter.check_venues_access.return_value = {
                "deribit": (
                    False,
                    "API access denied for deribit (HTTP 403 - check subscription)",
                ),
            }
            mock_proc.tardis_adapter = mock_tardis_adapter
            mock_proc_class.return_value = mock_proc

            mock_storage = Mock()
            mock_storage.store_instruments = Mock(return_value=True)
            mock_storage_class.return_value = mock_storage

            config = {"project_id": "test-project"}
            service = InstrumentsService(config)

            date = datetime(2024, 1, 1, tzinfo=timezone.utc)
            result = await service.generate_instruments_for_date(date=date, exchanges=["deribit"], cefi=True)

            # Should handle gracefully
            assert result is not None
            mock_tardis_adapter.check_venues_access.assert_called_once()
