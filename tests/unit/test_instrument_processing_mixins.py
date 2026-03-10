"""
Unit tests for instrument_processing_mixins pure methods.

Tests TardisIntegrationMixin._is_problematic_binance_instrument
and TardisIntegrationMixin._convert_to_tardis_symbol —
both are stateless, require no I/O, and have clear branching logic.
"""

from instruments_service.app.core.instrument_processing_mixins import TardisIntegrationMixin


class _ConcreteHandler(TardisIntegrationMixin):
    """Concrete subclass exposing the pure mixin methods for testing."""

    api_key = None


def _handler() -> _ConcreteHandler:
    return _ConcreteHandler()


# ─── _is_problematic_binance_instrument ──────────────────────────────────────


class TestIsProblematicBinanceInstrument:
    def test_normal_btcusdt_is_not_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("BTCUSDT") is False

    def test_ethusdt_is_not_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("ETHUSDT") is False

    def test_1000shib_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000SHIBUSDT") is True

    def test_1000pepe_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000PEPEUSDT") is True

    def test_1000x_prefix_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000XUSDT") is True

    def test_1000sats_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000SATSUSDT") is True

    def test_1mbabydoge_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1MBABYDOGEUSDT") is True

    def test_1000bonk_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000BONKUSDT") is True

    def test_1000lunc_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000LUNCUSDT") is True

    def test_usdttry_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("usdttry") is True

    def test_usdtzar_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("usdtzar") is True

    def test_usdtbrl_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("usdtbrl") is True

    def test_nftusdt_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("nftusdt") is True

    def test_defiusdt_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("defiusdt") is True

    def test_bullusdt_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("bullusdt") is True

    def test_bearusdt_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("bearusdt") is True

    def test_1inch_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1INCHUSDT") is True

    def test_general_1000_pattern_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000randomtoken") is True

    def test_general_1000000_pattern_is_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("1000000neiro") is True

    def test_solusdt_is_not_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("SOLUSDT") is False

    def test_adausdt_is_not_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("ADAUSDT") is False

    def test_lowercase_btcusdt_is_not_problematic(self):
        h = _handler()
        assert h._is_problematic_binance_instrument("btcusdt") is False


# ─── _convert_to_tardis_symbol ───────────────────────────────────────────────


class TestConvertToTardisSymbol:
    def test_binance_removes_dashes_lowercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("BTC-USDT", "binance")
        assert result == "btcusdt"

    def test_binance_futures_removes_dashes_lowercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("ETH-USDT", "binance-futures")
        assert result == "ethusdt"

    def test_deribit_lowercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("BTC-PERPETUAL", "deribit")
        assert result == "btc-perpetual"

    def test_upbit_uppercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("btc-krw", "upbit")
        assert result == "BTC-KRW"

    def test_coinbase_uppercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("btc-usd", "coinbase")
        assert result == "BTC-USD"

    def test_other_exchange_lowercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("BTC-USDT", "okx")
        assert result == "btc-usdt"

    def test_bybit_lowercased(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("BTCUSDT", "bybit")
        assert result == "btcusdt"

    def test_already_lowercase_binance(self):
        h = _handler()
        result = h._convert_to_tardis_symbol("btcusdt", "binance")
        assert result == "btcusdt"
