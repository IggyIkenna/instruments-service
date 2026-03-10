"""
Unit tests for various business logic modules in instruments-service.

Covers:
- ccxt_manual_fallback (7% → 100%)
- selective_validation get_venues_for_category (76% → higher)
- instruments_service.__init__ lazy imports (17% → higher)
- app/core/processors/derived_fields_populator (69% → higher)
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from instruments_service.app.core.processors.ccxt_manual_fallback import get_manual_ccxt_fallback
from instruments_service.app.core.processors.derived_fields_populator import populate_derived_fields
from instruments_service.app.core.selective_validation import get_venues_for_category

# ─── ccxt_manual_fallback ─────────────────────────────────────────────────────


class TestGetManualCcxtFallback:
    def test_hyperliquid_btc(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "BTC")
        assert result["tick_size"] == "1.0"
        assert result["contract_size"] == 1.0

    def test_hyperliquid_eth(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "ETH")
        assert result["tick_size"] == "0.1"

    def test_hyperliquid_sol(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "SOL")
        assert result["tick_size"] == "0.01"

    def test_hyperliquid_avax(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "AVAX")
        assert result["tick_size"] == "0.01"

    def test_hyperliquid_atom(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "ATOM")
        assert result["tick_size"] == "0.0001"

    def test_hyperliquid_unknown_asset_returns_empty(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "UNKNOWN_COIN")
        assert result == {}

    def test_aster_btc(self):
        result = get_manual_ccxt_fallback("ASTER", "BTC")
        assert result["tick_size"] == "0.1"
        assert result["min_size"] == "0.001"

    def test_aster_eth(self):
        result = get_manual_ccxt_fallback("ASTER", "ETH")
        assert result["tick_size"] == "0.01"

    def test_aster_sol(self):
        result = get_manual_ccxt_fallback("ASTER", "SOL")
        assert result["min_size"] == "0.01"

    def test_aster_unknown_asset_returns_empty(self):
        result = get_manual_ccxt_fallback("ASTER", "UNKNOWNCOIN")
        assert result == {}

    def test_unknown_venue_returns_empty(self):
        result = get_manual_ccxt_fallback("BINANCE", "BTC")
        assert result == {}

    def test_empty_venue_returns_empty(self):
        result = get_manual_ccxt_fallback("", "BTC")
        assert result == {}

    def test_hyperliquid_doge(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "DOGE")
        assert "tick_size" in result

    def test_hyperliquid_link(self):
        result = get_manual_ccxt_fallback("HYPERLIQUID", "LINK")
        assert result["min_size"] == "cost_min:10.0"


# ─── selective_validation get_venues_for_category ────────────────────────────


class TestGetVenuesForCategory:
    def _make_mapping(self, tardis=None, databento=None, defi=None):
        mapping = MagicMock()
        mapping.all_tardis_exchanges = tardis or ["binance", "bybit"]
        mapping.all_databento_venues = databento or ["CME", "ICE"]
        mapping.all_defi_venues = defi or ["UNISWAP_V3"]
        return mapping

    def test_cefi_returns_tardis_exchanges(self):
        mapping = self._make_mapping()
        result = get_venues_for_category("CEFI", mapping)
        assert result == ["binance", "bybit"]

    def test_tradfi_returns_databento_venues(self):
        mapping = self._make_mapping()
        result = get_venues_for_category("TRADFI", mapping)
        assert "CME" in result

    def test_defi_returns_defi_venues(self):
        mapping = self._make_mapping()
        result = get_venues_for_category("DEFI", mapping)
        assert "UNISWAP_V3" in result

    def test_sports_returns_league_registry_keys(self):
        mapping = self._make_mapping()
        result = get_venues_for_category("SPORTS", mapping)
        assert isinstance(result, list)
        # Should be sorted list of league IDs from LEAGUE_REGISTRY
        assert result == sorted(result)

    def test_unknown_category_raises(self):
        mapping = self._make_mapping()
        with pytest.raises(ValueError, match="Unknown category"):
            get_venues_for_category("UNKNOWN_CAT", mapping)


# ─── instruments_service.__init__ lazy imports ────────────────────────────────


class TestInstrumentsServiceInit:
    def test_version_attribute(self):
        import instruments_service

        assert instruments_service.__version__ == "0.1.0"

    def test_lazy_instrument_processing_service(self):
        import instruments_service

        cls = instruments_service.InstrumentProcessingService
        assert cls is not None

    def test_lazy_cloud_instrument_storage(self):
        import instruments_service

        cls = instruments_service.CloudInstrumentStorage
        assert cls is not None

    def test_lazy_instrument_batch_processor(self):
        import instruments_service

        cls = instruments_service.InstrumentBatchProcessor
        assert cls is not None

    def test_lazy_instrument_definition(self):
        import instruments_service

        cls = instruments_service.InstrumentDefinition
        assert cls is not None

    def test_lazy_instrument_key(self):
        import instruments_service

        cls = instruments_service.InstrumentKey
        assert cls is not None

    def test_lazy_venue(self):
        import instruments_service

        cls = instruments_service.Venue
        assert cls is not None

    def test_lazy_instrument_type(self):
        import instruments_service

        cls = instruments_service.InstrumentType
        assert cls is not None

    def test_attribute_error_for_unknown(self):
        import instruments_service

        with pytest.raises(AttributeError):
            _ = instruments_service.NonExistentAttribute


# ─── derived_fields_populator (async) ────────────────────────────────────────


def _make_derived_service(
    deribit_quotes=None,
    ccxt_service=None,
    enable_ccxt=True,
    tardis_to_venue=None,
    venue_to_ccxt=None,
):
    svc = MagicMock()

    mock_vm = MagicMock()
    mock_vm.tardis_to_venue = tardis_to_venue or {"DERIBIT": "DERIBIT"}
    mock_vm.venue_to_ccxt = venue_to_ccxt or {"BINANCE": "binance", "DERIBIT": "deribit"}
    svc.venue_mapping = mock_vm

    mock_ec = MagicMock()
    mock_ec.valid_quote_currencies = {"DERIBIT": deribit_quotes or ["BTC", "ETH", "USDC"]}
    svc.exchange_config = mock_ec

    mock_pc = MagicMock()
    mock_pc.enable_ccxt_integration = enable_ccxt
    svc.processing_config = mock_pc

    svc.ccxt_service = ccxt_service

    svc.parse_option_components.return_value = {
        "expiry_date": "2025-03-28T08:00:00Z",
        "strike_price": "80000",
        "option_type": "CALL",
    }
    svc.parse_expiry_from_symbol.return_value = "2025-06-27T08:00:00Z"

    return svc


class TestPopulateDerivedFields:
    def test_spot_pair_no_ccxt_returns_ccxt_symbol(self):
        svc = _make_derived_service(ccxt_service=None)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BINANCE:SPOT_PAIR:BTC-USDT",
                venue="BINANCE",
                inst_type="SPOT_PAIR",
                base_asset="BTC",
                quote_asset="USDT",
                symbol_id="BTCUSDT",
            )
        )
        assert result.get("ccxt_symbol") == "BTC/USDT"
        assert result.get("ccxt_exchange") == "binance"

    def test_perpetual_sets_underlying(self):
        svc = _make_derived_service(ccxt_service=None)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BINANCE:PERPETUAL:BTC-USDT@LIN",
                venue="BINANCE",
                inst_type="PERPETUAL",
                base_asset="BTC",
                quote_asset="USDT",
                symbol_id="BTCUSDT",
            )
        )
        assert result.get("underlying") == "BTC-USDT"

    def test_option_sets_expiry_strike_type(self):
        svc = _make_derived_service(ccxt_service=None)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="DERIBIT:OPTION:BTC-USD-250328-80000-CALL@INV",
                venue="DERIBIT",
                inst_type="OPTION",
                base_asset="BTC",
                quote_asset="USD",
                symbol_id="BTC-28MAR25-80000-C",
            )
        )
        assert result.get("expiry") is not None
        assert result.get("strike") == "80000"
        assert result.get("option_type") == "CALL"

    def test_future_parses_expiry(self):
        svc = _make_derived_service(ccxt_service=None)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BINANCE:FUTURE:BTC-USDT-250627@LIN",
                venue="BINANCE",
                inst_type="FUTURE",
                base_asset="BTC",
                quote_asset="USDT",
                symbol_id="BTCUSDT_250627",
                exchange="binance",
            )
        )
        assert result.get("expiry") is not None

    def test_deribit_usd_quote_sets_inverse(self):
        svc = _make_derived_service(ccxt_service=None)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="DERIBIT:PERPETUAL:BTC-USD@INV",
                venue="DERIBIT",
                inst_type="PERPETUAL",
                base_asset="BTC",
                quote_asset="USD",
                symbol_id="BTC-PERPETUAL",
            )
        )
        assert result.get("inverse") is True

    def test_ccxt_service_used_when_enabled(self):
        mock_ccxt = MagicMock()
        mock_ccxt.get_metadata.return_value = {
            "ccxt_symbol": "BTC/USDT",
            "ccxt_exchange": "binance",
            "tick_size": "0.01",
            "min_size": "0.001",
            "contract_size": None,
        }
        mock_ccxt.get_leverage_limits.return_value = {
            "max_position_size": 1000.0,
            "max_leverage": 100.0,
            "initial_margin_rate": 0.01,
            "maintenance_margin_rate": 0.005,
        }
        svc = _make_derived_service(ccxt_service=mock_ccxt, enable_ccxt=True)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BINANCE:PERPETUAL:BTC-USDT@LIN",
                venue="BINANCE",
                inst_type="PERPETUAL",
                base_asset="BTC",
                quote_asset="USDT",
                symbol_id="BTCUSDT",
            )
        )
        assert result.get("tick_size") == "0.01"
        assert result.get("max_leverage") == 100.0

    def test_ccxt_disabled_still_sets_symbol(self):
        svc = _make_derived_service(ccxt_service=None, enable_ccxt=False)
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BINANCE:SPOT_PAIR:ETH-USDT",
                venue="BINANCE",
                inst_type="SPOT_PAIR",
                base_asset="ETH",
                quote_asset="USDT",
                symbol_id="ETHUSDT",
            )
        )
        assert result.get("ccxt_symbol") == "ETH/USDT"

    def test_no_ccxt_service_no_ccxt_exchange_id(self):
        """When venue not in venue_to_ccxt, ccxt_exchange defaults to empty string."""
        svc = _make_derived_service(ccxt_service=None, venue_to_ccxt={"BINANCE": "binance"})
        result = asyncio.run(
            populate_derived_fields(
                svc,
                canonical_key="BYBIT:SPOT_PAIR:BTC-USDT",
                venue="BYBIT",  # not in venue_to_ccxt
                inst_type="SPOT_PAIR",
                base_asset="BTC",
                quote_asset="USDT",
                symbol_id="BTCUSDT",
            )
        )
        assert result.get("ccxt_exchange") == ""
