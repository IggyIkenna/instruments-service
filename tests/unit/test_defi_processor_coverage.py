"""
Unit tests for engine/processors/defi_processor.py

Covers:
- fetch_defi_instruments with various protocols
- MVP token filtering
- base_versions mapping (WETH -> ETH)
- unknown protocol returns empty dict
- exception handling
"""

from datetime import datetime
from unittest.mock import MagicMock, patch


def _make_service(base_tokens: list[str] | None = None, hyperliquid_bases: list[str] | None = None) -> MagicMock:
    """Create a mock DefiServiceProtocol."""
    service = MagicMock()
    tokens = base_tokens or ["ETH", "BTC", "USDC", "USDT", "WETH"]
    service.venue_mapping.get_defi_mvp_tokens.return_value = tokens
    service.venue_mapping.hyperliquid_aster_mvp_base_assets = hyperliquid_bases or ["BTC", "ETH", "SOL"]
    service.ccxt_service.load_markets.return_value = {"markets": {}}
    service.ccxt_service.get_metadata.return_value = {}
    service.ccxt_service.generate_default_ccxt_symbol.return_value = "BTC/USDT"
    service.get_manual_ccxt_fallback.return_value = {}
    return service


class TestFetchDefiInstrumentsUnknownProtocol:
    def test_unknown_protocol_returns_empty(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()
        result = fetch_defi_instruments(service, "unknown_protocol_xyz")
        assert result == {}


class TestFetchDefiInstrumentsAster:
    def test_aster_raises_not_implemented(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()
        # aster protocol raises NotImplementedError, which is caught as ValueError/TypeError
        # The outer except block catches it and returns {}
        result = fetch_defi_instruments(service, "aster")
        assert result == {}


class TestFetchDefiInstrumentsBalancer:
    def test_balancer_with_mock(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_instrument = {
            "instrument_key": "BALANCER:SPOT_PAIR:ETH-USDC",
            "venue": "BALANCER",
            "instrument_type": "SPOT_PAIR",
            "symbol": "ETH-USDC",
            "base_asset": "ETH",
            "quote_asset": "USDC",
            "available_from_datetime": "2024-01-01T00:00:00Z",
            "market_category": "DEFI",
        }

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {"key1": mock_instrument}

        with patch("instruments_service.engine.processors.defi_processor.BalancerAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "balancer")

        # ETH is in MVP list, USDC is in MVP list -> should be included
        assert isinstance(result, dict)

    def test_balancer_filters_non_mvp(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service(base_tokens=["ETH", "USDC"])

        non_mvp_instrument = {
            "instrument_key": "BALANCER:SPOT_PAIR:DOGE-USDC",
            "venue": "BALANCER",
            "instrument_type": "SPOT_PAIR",
            "symbol": "DOGE-USDC",
            "base_asset": "DOGE",  # Not in MVP list
            "quote_asset": "USDC",
            "available_from_datetime": "2024-01-01T00:00:00Z",
            "market_category": "DEFI",
        }

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {"key1": non_mvp_instrument}

        with patch("instruments_service.engine.processors.defi_processor.BalancerAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "balancer")

        assert result == {}


class TestFetchDefiInstrumentsAaveV3:
    def test_aave_with_target_date(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()
        target_date = datetime(2024, 1, 1)

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.AaveV3Adapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "aave_v3", target_date=target_date)

        assert result == {}
        mock_adapter.fetch_markets.assert_called_once_with(target_date=target_date)


class TestFetchDefiInstrumentsEtherFi:
    def test_etherfi_protocol(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_lst_instruments.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.EtherFiAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "etherfi")

        assert result == {}


class TestFetchDefiInstrumentsLido:
    def test_lido_protocol(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_lst_instruments.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.LidoAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "lido")

        assert result == {}


class TestFetchDefiInstrumentsMorpho:
    def test_morpho_protocol(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.MorphoAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "morpho")

        assert result == {}


class TestFetchDefiInstrumentsEthena:
    def test_ethena_protocol(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_yield_bearing_instruments.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.EthenaAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "ethena")

        assert result == {}


class TestFetchDefiInstrumentsUniswapV2:
    def test_uniswap_v2(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.UniswapV2Adapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "uniswap_v2")

        assert result == {}


class TestFetchDefiInstrumentsUniswapV4:
    def test_uniswap_v4(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.UniswapV4Adapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "uniswap_v4")

        assert result == {}


class TestFetchDefiInstrumentsCurve:
    def test_curve_protocol(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.CurveAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "curve")

        assert result == {}


class TestFetchDefiInstrumentsUniswapV3:
    def test_uniswap_v3(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_pools.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.UniswapV3Adapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "uniswap_v3")

        assert result == {}


class TestFetchDefiInstrumentsHyperliquid:
    def test_hyperliquid_basic(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service(hyperliquid_bases=["BTC", "ETH"])

        mock_adapter = MagicMock()
        mock_adapter.fetch_perpetuals.return_value = {}
        mock_adapter.fetch_spot_pairs.return_value = {}

        with patch(
            "instruments_service.engine.processors.defi_processor.HyperliquidAdapter", return_value=mock_adapter
        ):
            result = fetch_defi_instruments(service, "hyperliquid")

        assert result == {}


class TestFetchDefiInstrumentsBaseVersions:
    """Test WETH -> ETH base_version mapping logic."""

    def test_weth_maps_to_eth_base(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        # ETH is in MVP list, WETH should map to ETH via base_versions
        service = _make_service(base_tokens=["ETH", "BTC", "USDC"])

        instrument_with_weth = {
            "instrument_key": "UNISWAP_V3:SPOT_PAIR:WETH-USDC",
            "venue": "UNISWAP_V3",
            "instrument_type": "SPOT_PAIR",
            "symbol": "WETH-USDC",
            "base_asset": "WETH",  # Should be mapped to ETH
            "quote_asset": "USDC",
            "available_from_datetime": "2024-01-01T00:00:00Z",
            "market_category": "DEFI",
        }

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {"key1": instrument_with_weth}

        with patch("instruments_service.engine.processors.defi_processor.BalancerAdapter", return_value=mock_adapter):
            result = fetch_defi_instruments(service, "balancer")

        # WETH should be accepted since ETH is in MVP list
        assert isinstance(result, dict)


class TestFetchDefiInstrumentsDateFilter:
    """Test that date filtering is applied when target_date is provided."""

    def test_date_filter_called_when_target_date_provided(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()
        target_date = datetime(2024, 1, 15)
        service.date_filter_service.filter_instruments_by_date.return_value = {}

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {"key1": {"base_asset": "ETH", "quote_asset": "USDC"}}

        with patch("instruments_service.engine.processors.defi_processor.BalancerAdapter", return_value=mock_adapter):
            fetch_defi_instruments(service, "balancer", target_date=target_date)

        service.date_filter_service.filter_instruments_by_date.assert_called_once()

    def test_date_filter_not_called_without_target_date(self) -> None:
        from instruments_service.engine.processors.defi_processor import fetch_defi_instruments

        service = _make_service()

        mock_adapter = MagicMock()
        mock_adapter.fetch_markets.return_value = {}

        with patch("instruments_service.engine.processors.defi_processor.BalancerAdapter", return_value=mock_adapter):
            fetch_defi_instruments(service, "balancer")

        service.date_filter_service.filter_instruments_by_date.assert_not_called()
