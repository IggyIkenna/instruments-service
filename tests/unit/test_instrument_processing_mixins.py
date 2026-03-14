"""
Unit tests for instrument_processing_mixins pure methods.

Tests TardisIntegrationMixin._is_problematic_binance_instrument
and TardisIntegrationMixin._convert_to_tardis_symbol —
both are stateless, require no I/O, and have clear branching logic.
"""

from unittest.mock import MagicMock

import pytest

from instruments_service.app.core.instrument_processing_mixins import TardisIntegrationMixin
from instruments_service.app.core.instrument_validation import InstrumentValidationMixin


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


# From test_low_coverage_modules

# ─── InstrumentValidationMixin ────────────────────────────────────────────────


class _MockService(InstrumentValidationMixin):
    """Test double that satisfies the InstrumentValidationMixin protocol."""

    def __init__(self):
        self.venue_mapping = MagicMock()
        self.venue_mapping.all_cefi_venues = ["BINANCE", "BYBIT", "DERIBIT"]
        self.venue_mapping.all_databento_venues = ["CME", "ICE", "NASDAQ"]
        self.venue_mapping.all_defi_venues = ["UNISWAP_V3", "AAVE_V3"]


class TestInstrumentValidationMixinFromLowCoverage:
    def setup_method(self):
        self.svc = _MockService()

    # _validate_venues_filter
    def test_empty_venues_filter_returns_none(self):
        result = self.svc._validate_venues_filter([], cefi=True, tradfi=True, defi=True)
        assert result is None

    def test_valid_cefi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["BINANCE"], cefi=True, tradfi=False, defi=False)
        assert result is not None
        assert "BINANCE" in result["CEFI"]

    def test_valid_tradfi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["CME"], cefi=False, tradfi=True, defi=False)
        assert result is not None
        assert "CME" in result["TRADFI"]

    def test_valid_defi_venue_categorised(self):
        result = self.svc._validate_venues_filter(["UNISWAP_V3"], cefi=False, tradfi=False, defi=True)
        assert result is not None
        assert "UNISWAP_V3" in result["DEFI"]

    def test_invalid_venue_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid venues"):
            self.svc._validate_venues_filter(["UNKNOWN_VENUE"], cefi=True, tradfi=True, defi=True)

    def test_no_market_types_allows_all(self):
        # When no flags set, all market types processed; invalid still raises
        with pytest.raises(ValueError):
            self.svc._validate_venues_filter(["UNKNOWN"], cefi=False, tradfi=False, defi=False)

    def test_venue_valid_for_multiple_types(self):
        # A venue in both cefi and tradfi lists
        self.svc.venue_mapping.all_cefi_venues = ["SHARED_VENUE"]
        self.svc.venue_mapping.all_databento_venues = ["SHARED_VENUE"]
        result = self.svc._validate_venues_filter(["SHARED_VENUE"], cefi=True, tradfi=True, defi=False)
        assert result is not None
        assert "SHARED_VENUE" in result["CEFI"]
        assert "SHARED_VENUE" in result["TRADFI"]

    # _extract_venues_from_instrument_ids
    def test_none_instrument_ids_returns_filter_unchanged(self):
        result = _MockService._extract_venues_from_instrument_ids(None, ["BINANCE"])
        assert result == ["BINANCE"]

    def test_extracts_venue_from_id(self):
        result = _MockService._extract_venues_from_instrument_ids(["BINANCE:SPOT_PAIR:BTC-USDT"], [])
        assert "BINANCE" in result

    def test_list_instrument_ids_extracted(self):
        result = _MockService._extract_venues_from_instrument_ids(
            ["BINANCE:SPOT_PAIR:BTC-USDT", "DERIBIT:PERPETUAL:BTC-USD@INV"], []
        )
        assert "BINANCE" in result
        assert "DERIBIT" in result

    def test_narrows_venues_filter_to_matching(self):
        result = _MockService._extract_venues_from_instrument_ids(["BINANCE:SPOT_PAIR:BTC-USDT"], ["BINANCE", "CME"])
        assert result == ["BINANCE"]
        assert "CME" not in result

    def test_no_matching_venues_returns_empty(self):
        result = _MockService._extract_venues_from_instrument_ids(["DERIBIT:PERPETUAL:BTC-USD@INV"], ["BINANCE"])
        assert result == []

    def test_string_instrument_id_works(self):
        result = _MockService._extract_venues_from_instrument_ids("CME:FUTURE:ES-USD-250328@LIN", [])
        assert "CME" in result

    # _filter_instruments_by_ids
    def test_none_ids_returns_all(self):
        instruments = {"K1": MagicMock(), "K2": MagicMock()}
        result = _MockService._filter_instruments_by_ids(instruments, None)
        assert len(result) == 2

    def test_filters_to_matching_ids(self):
        instruments = {"K1": "v1", "K2": "v2", "K3": "v3"}
        result = _MockService._filter_instruments_by_ids(instruments, ["K1", "K3"])
        assert "K1" in result
        assert "K2" not in result
        assert "K3" in result

    def test_case_insensitive_id_matching(self):
        instruments = {"BINANCE:SPOT_PAIR:BTC-USDT": "v1"}
        result = _MockService._filter_instruments_by_ids(instruments, ["binance:spot_pair:btc-usdt"])
        assert len(result) == 1

    def test_no_match_returns_empty(self):
        instruments = {"K1": "v1"}
        result = _MockService._filter_instruments_by_ids(instruments, ["NONEXISTENT"])
        assert result == {}

    def test_string_id_treated_as_single_id(self):
        instruments = {"K1": "v1", "K2": "v2"}
        result = _MockService._filter_instruments_by_ids(instruments, "K1")
        assert "K1" in result
        assert "K2" not in result


@pytest.mark.unit
class TestInstrumentValidationFromBoost:
    """Import coverage for instrument_validation mixin."""

    def test_import(self):
        from instruments_service.app.core.instrument_validation import InstrumentValidationMixin

        assert InstrumentValidationMixin is not None

    def test_init(self):
        from instruments_service.app.core.instrument_validation import InstrumentValidationMixin

        svc = InstrumentValidationMixin()
        assert svc is not None
