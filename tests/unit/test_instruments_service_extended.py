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
    async def test_generate_instruments_date_range_empty_range(self, service):
        """Test generating instruments for empty date range."""
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        service.batch_processor.get_required_periods.return_value = []
        
        result = await service.generate_instruments_date_range(
            start_date=start_date,
            end_date=end_date,
            cefi=True,
        )
        
        assert result["dates_processed"] == 0
        assert result["success_rate_percent"] == 0

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
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "TEST:SPOT_PAIR:BTC-USDT"
        mock_instrument.model_dump.return_value = {
            "instrument_key": "TEST:SPOT_PAIR:BTC-USDT",
            "venue": "TEST",
            "instrument_type": "SPOT_PAIR",
        }
        
        service.processing_service.process_exchange_instruments = AsyncMock(
            return_value={"TEST:SPOT_PAIR:BTC-USDT": mock_instrument}
        )
        service.venue_mapping.all_tardis_exchanges = ["binance"]
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] in ["success", "warning"]
        assert "instruments_generated" in result or "message" in result

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_cefi_with_exchanges(self, service):
        """Test generating instruments for CeFi mode with specific exchanges."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "TEST:SPOT_PAIR:BTC-USDT"
        mock_instrument.model_dump.return_value = {
            "instrument_key": "TEST:SPOT_PAIR:BTC-USDT",
            "venue": "TEST",
        }
        
        service.processing_service.process_exchange_instruments = AsyncMock(
            return_value={"TEST:SPOT_PAIR:BTC-USDT": mock_instrument}
        )
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            exchanges=["binance"],
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] in ["success", "warning"]
        assert result.get("exchanges_processed") == 1

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_cefi_exception(self, service):
        """Test generating instruments for CeFi mode with exception."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.processing_service.process_exchange_instruments = AsyncMock(
            side_effect=Exception("Test error")
        )
        service.venue_mapping.all_tardis_exchanges = ["binance"]
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        # Should handle exception gracefully
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_tradfi(self, service):
        """Test generating instruments for TradFi mode."""
        from datetime import datetime, timezone
        from instruments_service.models import InstrumentDefinition
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        with patch("instruments_service.app.core.instruments_service.DatabentoAdapter") as mock_adapter_class, \
             patch("instruments_service.app.core.instruments_service.DatabentoInstrumentConfig") as mock_config_class:
            mock_adapter = Mock()
            mock_vix_def = Mock()
            mock_vix_def.instrument_key = "CBOE:INDEX:VIX"
            mock_adapter.create_vix_instrument_definition.return_value = {
                "instrument_key": "CBOE:INDEX:VIX",
                "venue": "CBOE",
                "instrument_type": "INDEX",
                "symbol": "VIX",
            }
            mock_adapter_class.return_value = mock_adapter
            
            mock_config = Mock()
            mock_config.get_symbols_for_venue.return_value = ["ES.FUT"]
            mock_config_class.return_value = mock_config
            
            mock_instrument = Mock()
            mock_instrument.instrument_key = "CME:FUTURE:ES"
            mock_instrument.model_dump.return_value = {
                "instrument_key": "CME:FUTURE:ES",
                "venue": "CME",
            }
            service.processing_service.fetch_databento_instruments = AsyncMock(
                return_value={"CME:FUTURE:ES": mock_instrument}
            )
            service.cloud_storage.store_instruments = AsyncMock(return_value=True)
            
            result = await service.generate_instruments_for_date(
                date=target_date,
                cefi=False,
                tradfi=True,
                defi=False,
            )
            
            assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_tradfi_cboe_only(self, service):
        """Test generating instruments for TradFi mode with CBOE only."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        with patch("instruments_service.app.core.instruments_service.DatabentoAdapter") as mock_adapter_class, \
             patch("instruments_service.app.core.instruments_service.DatabentoInstrumentConfig") as mock_config_class, \
             patch("instruments_service.app.core.instruments_service.InstrumentDefinition") as mock_inst_def:
            mock_adapter = Mock()
            mock_adapter.create_vix_instrument_definition.return_value = {
                "instrument_key": "CBOE:INDEX:VIX",
                "venue": "CBOE",
                "instrument_type": "INDEX",
                "symbol": "VIX",
            }
            mock_adapter_class.return_value = mock_adapter
            
            mock_config = Mock()
            mock_config.get_symbols_for_venue.return_value = []
            mock_config_class.return_value = mock_config
            
            mock_vix_inst = Mock()
            mock_vix_inst.instrument_key = "CBOE:INDEX:VIX"
            mock_vix_inst.model_dump.return_value = {"instrument_key": "CBOE:INDEX:VIX"}
            mock_inst_def.return_value = mock_vix_inst
            
            service.cloud_storage.store_instruments = AsyncMock(return_value=True)
            
            result = await service.generate_instruments_for_date(
                date=target_date,
                cefi=False,
                tradfi=True,
                defi=False,
            )
            
            assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_tradfi_exception(self, service):
        """Test generating instruments for TradFi mode with exception."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        with patch("instruments_service.app.core.instruments_service.DatabentoInstrumentConfig") as mock_config_class:
            mock_config = Mock()
            mock_config.get_symbols_for_venue.side_effect = Exception("Config error")
            mock_config_class.return_value = mock_config
            
            result = await service.generate_instruments_for_date(
                date=target_date,
                cefi=False,
                tradfi=True,
                defi=False,
            )
            
            # Should handle exception gracefully
            assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_defi(self, service):
        """Test generating instruments for DeFi mode."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "UNISWAPV3-ETH:POOL:TEST"
        mock_instrument.model_dump.return_value = {"instrument_key": "UNISWAPV3-ETH:POOL:TEST"}
        service.processing_service.fetch_defi_instruments = Mock(
            return_value={"UNISWAPV3-ETH:POOL:TEST": mock_instrument}
        )
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=True,
        )
        
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_defi_exception(self, service):
        """Test generating instruments for DeFi mode with exception."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.processing_service.fetch_defi_instruments = Mock(
            side_effect=Exception("DeFi error")
        )
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=True,
        )
        
        # Should handle exception gracefully
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_defi_with_venues(self, service):
        """Test generating instruments for DeFi mode with venue filter."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "HYPERLIQUID:PERPETUAL:TEST"
        service.processing_service.fetch_defi_instruments = Mock(
            return_value={"HYPERLIQUID:PERPETUAL:TEST": mock_instrument}
        )
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=True,
            venues=["HYPERLIQUID"],
        )
        
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_defi_venues_string(self, service):
        """Test generating instruments for DeFi mode with venues as string."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "ASTER:PERPETUAL:TEST"
        service.processing_service.fetch_defi_instruments = Mock(
            return_value={"ASTER:PERPETUAL:TEST": mock_instrument}
        )
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=True,
            venues="ASTER",  # String instead of list
        )
        
        assert result["status"] in ["success", "warning"]

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_no_modes(self, service):
        """Test generating instruments with no mode flags (processes all)."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Mock all the processing methods
        service.processing_service.process_exchange_instruments = AsyncMock(return_value={})
        service.processing_service.fetch_databento_instruments = AsyncMock(return_value={})
        service.processing_service.fetch_defi_instruments = Mock(return_value={})
        service.venue_mapping.all_tardis_exchanges = []
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=False,
            tradfi=False,
            defi=False,
        )
        
        # Should recursively call with all modes
        assert result["status"] in ["success", "warning"]

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

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_storage_failure(self, service):
        """Test generating instruments when storage fails."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        mock_instrument = Mock()
        mock_instrument.instrument_key = "TEST:SPOT_PAIR:BTC-USDT"
        mock_instrument.model_dump.return_value = {"instrument_key": "TEST:SPOT_PAIR:BTC-USDT"}
        
        service.processing_service.process_exchange_instruments = AsyncMock(
            return_value={"TEST:SPOT_PAIR:BTC-USDT": mock_instrument}
        )
        service.venue_mapping.all_tardis_exchanges = ["binance"]
        service.cloud_storage.store_instruments = AsyncMock(return_value=False)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] == "error"
        assert "Storage failed" in result.get("message", "")

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_no_instruments(self, service):
        """Test generating instruments when no instruments are generated."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        service.processing_service.process_exchange_instruments = AsyncMock(return_value={})
        service.venue_mapping.all_tardis_exchanges = []
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] == "warning"
        assert result["instruments_generated"] == 0

    @pytest.mark.asyncio
    async def test_generate_instruments_for_date_dict_instruments(self, service):
        """Test generating instruments when instruments are dicts, not objects."""
        from datetime import datetime, timezone
        target_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        # Test with dict instruments (no model_dump method)
        service.processing_service.process_exchange_instruments = AsyncMock(
            return_value={"TEST:SPOT_PAIR:BTC-USDT": {"instrument_key": "TEST:SPOT_PAIR:BTC-USDT"}}
        )
        service.venue_mapping.all_tardis_exchanges = ["binance"]
        service.cloud_storage.store_instruments = AsyncMock(return_value=True)
        
        result = await service.generate_instruments_for_date(
            date=target_date,
            cefi=True,
            tradfi=False,
            defi=False,
        )
        
        assert result["status"] in ["success", "warning"]

