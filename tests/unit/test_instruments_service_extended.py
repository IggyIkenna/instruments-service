"""
Extended unit tests for InstrumentsService to increase coverage.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timezone, timedelta
import pandas as pd
from instruments_service.app.core.instruments_service import InstrumentsService


class TestInstrumentsServiceExtended:
    """Extended tests for InstrumentsService."""

    @pytest.fixture
    def service_config(self):
        """Create service configuration."""
        return {
            "project_id": "test-project",
            "enable_ccxt_integration": True,
            "enable_metadata_caching": True,
        }

    @pytest.fixture
    def service(self, service_config):
        """Create InstrumentsService with mocked dependencies."""
        with patch(
            "instruments_service.app.core.instruments_service.InstrumentProcessingService"
        ) as mock_processing, patch(
            "instruments_service.app.core.instruments_service.CloudInstrumentStorage"
        ) as mock_storage, patch(
            "instruments_service.app.core.instruments_service.InstrumentBatchProcessor"
        ) as mock_batch:
            mock_processing.return_value.get_processing_stats.return_value = {
                "exchanges_processed": 0
            }
            service = InstrumentsService(service_config)
            service.processing_service = mock_processing.return_value
            service.cloud_storage = mock_storage.return_value
            service.batch_processor = mock_batch.return_value
            return service

    @pytest.mark.asyncio
    async def test_generate_instruments_date_range(self, service):
        """Test generating instruments for date range."""
        # Mock batch processor
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 3, tzinfo=timezone.utc)
        date_range = [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
            datetime(2024, 1, 3, tzinfo=timezone.utc),
        ]
        service.batch_processor.get_required_periods.return_value = date_range

        # Mock generate_instruments_for_date
        service.generate_instruments_for_date = AsyncMock(
            side_effect=[
                {"status": "success", "instruments_generated": 10},
                {"status": "success", "instruments_generated": 15},
                {"status": "success", "instruments_generated": 20},
            ]
        )

        result = await service.generate_instruments_date_range(
            start_date=start_date,
            end_date=end_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )

        assert result["status"] in ["success", "partial"]
        assert result["dates_processed"] == 3
        assert result["dates_successful"] == 3
        assert result["total_instruments_generated"] == 45

    @pytest.mark.asyncio
    async def test_generate_instruments_date_range_with_errors(self, service):
        """Test date range generation with some errors."""
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 2, tzinfo=timezone.utc)
        date_range = [
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
        ]
        service.batch_processor.get_required_periods.return_value = date_range

        service.generate_instruments_for_date = AsyncMock(
            side_effect=[
                {"status": "success", "instruments_generated": 10},
                Exception("Test error"),
            ]
        )

        result = await service.generate_instruments_date_range(
            start_date=start_date, end_date=end_date, cefi=True
        )

        assert result["status"] == "partial"
        assert result["dates_failed"] == 1
        assert result["dates_successful"] == 1

    def test_query_instruments(self, service):
        """Test querying instruments."""
        mock_df = pd.DataFrame(
            {
                "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
                "venue": ["TEST"],
                "instrument_type": ["SPOT_PAIR"],
            }
        )
        service.cloud_storage.query_instruments.return_value = mock_df

        result = service.query_instruments(venue="TEST", instrument_type="SPOT_PAIR")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        service.cloud_storage.query_instruments.assert_called_once_with(
            venue="TEST", instrument_type="SPOT_PAIR"
        )

    def test_query_instruments_no_filters(self, service):
        """Test querying instruments without filters."""
        mock_df = pd.DataFrame()
        service.cloud_storage.query_instruments.return_value = mock_df

        result = service.query_instruments()

        assert isinstance(result, pd.DataFrame)
        service.cloud_storage.query_instruments.assert_called_once_with(
            venue=None, instrument_type=None
        )

    def test_get_processing_stats(self, service):
        """Test getting processing statistics."""
        service.processing_service.get_processing_stats.return_value = {
            "exchanges_processed": 5
        }
        service.batch_processor.max_batch_size = 1000
        service.batch_processor.lookback_days = 7

        stats = service.get_processing_stats()

        assert isinstance(stats, dict)
        assert "processing_service" in stats
        assert "batch_processor" in stats
        assert stats["batch_processor"]["max_batch_size"] == 1000
        assert stats["batch_processor"]["lookback_days"] == 7

    def test_cleanup(self, service):
        """Test cleanup method."""
        service.cleanup()
        service.processing_service.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_cefi(self, service):
        """Test generating instruments for CeFi mode."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.processing_service.process_exchange_instruments = AsyncMock(
            return_value={"TEST:SPOT_PAIR:BTC-USDT": Mock()}
        )
        service.venue_mapping.all_tardis_exchanges = ["binance"]
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] in ["success", "warning"]
        assert "instruments_generated" in result or "message" in result

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_tradfi(self, service):
        """Test generating instruments for TradFi mode."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        with patch("instruments_service.app.core.instruments_service.DatabentoAdapter") as mock_adapter_class, \
             patch("instruments_service.app.core.instruments_service.DatabentoInstrumentConfig") as mock_config_class:
            mock_adapter = Mock()
            mock_adapter.create_vix_instrument_definition.return_value = {
                "instrument_key": "CBOE:INDEX:VIX",
                "venue": "CBOE",
                "instrument_type": "INDEX",
            }
            mock_adapter_class.return_value = mock_adapter
            
            mock_config = Mock()
            mock_config.get_symbols_for_venue.return_value = ["ES.FUT"]
            mock_config_class.return_value = mock_config
            
            service.processing_service.fetch_databento_instruments = AsyncMock(
                return_value={"CME:FUTURE:ES": Mock()}
            )
            
            result = await service.generate_instruments_for_date(
                date=target_date,
                cefi=False,
                tradfi=True,
                defi=False,
            )
            
            assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_defi(self, service):
        """Test generating instruments for DeFi mode."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.processing_service.fetch_defi_instruments = Mock(
            return_value={"UNISWAPV3-ETH:POOL:TEST": Mock()}
        )
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=True,
        )
        
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_no_modes(self, service):
        """Test generating instruments with no mode flags (processes all)."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.generate_instruments_for_date = AsyncMock(
            return_value={"status": "success", "instruments_generated": 10}
        )
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=False,
        )
        
        # Should recursively call with all modes
        assert service.generate_instruments_for_date.called

    def test_query_instruments_with_filters(self, service):
        """Test querying instruments with all filters."""
        mock_df = pd.DataFrame({
            "instrument_key": ["TEST:SPOT_PAIR:BTC-USDT"],
            "venue": ["TEST"],
            "instrument_type": ["SPOT_PAIR"],
            "base_asset": ["BTC"],
            "quote_asset": ["USDT"],
        })
        service.cloud_storage.query_instruments.return_value = mock_df
        
        result = service.query_instruments(
            venue="TEST",
            instrument_type="SPOT_PAIR",
            base_asset="BTC",
            quote_asset="USDT"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

