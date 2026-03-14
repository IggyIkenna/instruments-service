"""
Unit tests for canonical_key_generator module.

Tests generate_canonical_key for SPOT_PAIR, PERPETUAL, FUTURE, OPTION
instrument types across multiple venues with mocked service dependencies.
"""

from unittest.mock import MagicMock

import pytest

from instruments_service.engine.processors.canonical_key_generator import (
    SymbolInfo,
    generate_canonical_key,
)

# ─── Fixtures ────────────────────────────────────────────────────────────────


def _make_service(
    venue: str | None = "BINANCE",
    instrument_type: str | None = "SPOT_PAIR",
    parsed_base: str = "BTC",
    parsed_quote: str = "USDT",
    expiry: str | None = None,
    option_components: dict | None = None,
    deribit_quotes: list[str] | None = None,
    excluded_strategies: list[str] | None = None,
) -> MagicMock:
    svc = MagicMock()
    svc.normalize_venue.return_value = venue
    svc.normalize_instrument_type.return_value = instrument_type
    svc.parse_symbol_components.return_value = {"base_asset": parsed_base, "quote_asset": parsed_quote}
    svc.parse_expiry_from_symbol.return_value = expiry

    if option_components is None:
        option_components = {"expiry_date": "", "strike_price": "", "option_type": ""}
    svc.parse_option_components.return_value = option_components

    mock_exchange_config = MagicMock()
    mock_exchange_config.valid_quote_currencies = {"DERIBIT": deribit_quotes or ["BTC", "ETH", "USDC"]}
    svc.exchange_config = mock_exchange_config

    mock_data_config = MagicMock()
    mock_data_config.excluded_deribit_strategies = excluded_strategies or ["-FS-", "-C-P-", "combo"]
    svc.data_config = mock_data_config

    return svc


# ─── normalize_venue / instrument_type failures ───────────────────────────────


class TestGenerateCanonicalKeyEdgeCases:
    def test_returns_none_when_venue_is_none(self):
        svc = _make_service(venue=None)
        result = generate_canonical_key(svc, "unknown", "spot", "BTCUSDT", {})
        assert result is None

    def test_returns_none_when_instrument_type_is_none(self):
        svc = _make_service(instrument_type=None)
        result = generate_canonical_key(svc, "binance", "unknown_type", "BTCUSDT", {})
        assert result is None

    def test_returns_none_when_no_base_and_no_quote_parsed(self):
        svc = _make_service(parsed_base="", parsed_quote="")
        result = generate_canonical_key(svc, "binance", "spot", "???", {})
        assert result is None

    def test_returns_none_when_symbol_info_empty_and_parse_also_empty(self):
        svc = _make_service(parsed_base="", parsed_quote="")
        result = generate_canonical_key(svc, "binance", "spot", "BADTOKEN", {})
        assert result is None

    def test_raises_when_deribit_config_missing(self):
        svc = _make_service()
        svc.exchange_config.valid_quote_currencies = {}  # no DERIBIT key
        with pytest.raises(ValueError, match="valid_quote_currencies must have entry for DERIBIT"):
            generate_canonical_key(svc, "binance", "spot", "BTCUSDT", {"base_asset": "BTC", "quote_asset": "USDT"})

    def test_raises_when_deribit_config_not_list(self):
        svc = _make_service()
        svc.exchange_config.valid_quote_currencies = {"DERIBIT": "BTC"}  # str, not list
        with pytest.raises(TypeError, match="must be list"):
            generate_canonical_key(svc, "binance", "spot", "BTCUSDT", {"base_asset": "BTC", "quote_asset": "USDT"})


# ─── SPOT_PAIR ────────────────────────────────────────────────────────────────


class TestSpotPairKeys:
    def test_basic_spot_pair_from_symbol_info(self):
        svc = _make_service(venue="BINANCE", instrument_type="SPOT_PAIR")
        result = generate_canonical_key(svc, "binance", "spot", "BTCUSDT", {"base_asset": "btc", "quote_asset": "usdt"})
        assert result == "BINANCE:SPOT_PAIR:BTC-USDT"

    def test_spot_pair_uses_parsed_when_symbol_info_empty(self):
        svc = _make_service(venue="COINBASE", instrument_type="SPOT_PAIR", parsed_base="ETH", parsed_quote="USD")
        result = generate_canonical_key(svc, "coinbase", "spot", "ETH-USD", {})
        assert result == "COINBASE:SPOT_PAIR:ETH-USD"

    def test_spot_pair_returns_none_when_base_whitespace_only(self):
        svc = _make_service(venue="BINANCE", instrument_type="SPOT_PAIR", parsed_base="   ", parsed_quote="USDT")
        result = generate_canonical_key(svc, "binance", "spot", "BTCUSDT", {})
        assert result is None

    def test_spot_pair_returns_none_when_quote_empty_after_parse(self):
        svc = _make_service(venue="BINANCE", instrument_type="SPOT_PAIR", parsed_base="BTC", parsed_quote="")
        result = generate_canonical_key(svc, "binance", "spot", "BTC???", {})
        assert result is None


# ─── PERPETUAL ────────────────────────────────────────────────────────────────


class TestPerpetualKeys:
    def test_lin_perp_on_non_deribit(self):
        svc = _make_service(venue="BINANCE", instrument_type="PERPETUAL")
        result = generate_canonical_key(
            svc, "binance", "perpetual", "BTCUSDT", {"base_asset": "BTC", "quote_asset": "USDT"}
        )
        assert result == "BINANCE:PERPETUAL:BTC-USDT@LIN"

    def test_inv_perp_on_deribit_usd_quote(self):
        svc = _make_service(venue="DERIBIT", instrument_type="PERPETUAL")
        result = generate_canonical_key(
            svc, "deribit", "perpetual", "BTC-PERPETUAL", {"base_asset": "BTC", "quote_asset": "USD"}
        )
        assert result == "DERIBIT:PERPETUAL:BTC-USD@INV"

    def test_lin_perp_on_deribit_usdc_quote(self):
        svc = _make_service(venue="DERIBIT", instrument_type="PERPETUAL", deribit_quotes=["BTC", "ETH", "USDC"])
        result = generate_canonical_key(
            svc, "deribit", "perpetual", "BTC_USDC-PERPETUAL", {"base_asset": "BTC", "quote_asset": "USDC"}
        )
        assert result == "DERIBIT:PERPETUAL:BTC-USDC@LIN"

    def test_perpetual_returns_none_when_base_non_ascii(self):
        svc = _make_service(venue="BINANCE", instrument_type="PERPETUAL", parsed_base="\u4e2d\u6587", parsed_quote="")
        result = generate_canonical_key(svc, "binance", "perpetual", "???USDT", {})
        assert result is None

    def test_perp_uses_parsed_when_symbol_info_missing(self):
        svc = _make_service(venue="BYBIT", instrument_type="PERPETUAL", parsed_base="SOL", parsed_quote="USDT")
        result = generate_canonical_key(svc, "bybit", "perpetual", "SOLUSDT", {})
        assert result == "BYBIT:PERPETUAL:SOL-USDT@LIN"


# ─── FUTURE ───────────────────────────────────────────────────────────────────


class TestFutureKeys:
    def test_future_with_iso_expiry_in_symbol_info(self):
        svc = _make_service(venue="BINANCE", instrument_type="FUTURE")
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USDT", "expiry_date": "2025-03-28T08:00:00Z"}
        result = generate_canonical_key(svc, "binance", "future", "BTCUSDT_250328", info)
        assert result == "BINANCE:FUTURE:BTC-USDT-250328@LIN"

    def test_future_parses_expiry_from_symbol_when_missing_in_info(self):
        svc = _make_service(venue="BINANCE", instrument_type="FUTURE", expiry="2025-06-27T08:00:00Z")
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USDT"}
        result = generate_canonical_key(svc, "binance", "future", "BTCUSDT_250627", info)
        assert result == "BINANCE:FUTURE:BTC-USDT-250627@LIN"

    def test_future_returns_none_when_no_expiry(self):
        svc = _make_service(venue="BINANCE", instrument_type="FUTURE", expiry=None)
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USDT"}
        result = generate_canonical_key(svc, "binance", "future", "BTCUSD_NODATE", info)
        assert result is None

    def test_future_deribit_inv_flavor(self):
        svc = _make_service(venue="DERIBIT", instrument_type="FUTURE")
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USD", "expiry_date": "2025-03-28T08:00:00Z"}
        result = generate_canonical_key(svc, "deribit", "future", "BTC-28MAR25", info)
        assert result == "DERIBIT:FUTURE:BTC-USD-250328@INV"

    def test_future_expiry_long_string_uses_regex_extract(self):
        """If expiry_str > 6 chars after format, regex trims to 6-digit group."""
        svc = _make_service(venue="BINANCE", instrument_type="FUTURE", expiry=None)
        # Pass an expiry_date with a long ISO string; formatting will produce >6 chars
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USDT", "expiry_date": "2025-12-26T08:00:00Z"}
        result = generate_canonical_key(svc, "binance", "future", "BTCUSDT_251226", info)
        assert result == "BINANCE:FUTURE:BTC-USDT-251226@LIN"

    def test_future_non_iso_expiry_passthrough(self):
        svc = _make_service(venue="CME", instrument_type="FUTURE", expiry=None)
        info: SymbolInfo = {"base_asset": "ES", "quote_asset": "USD", "expiry_date": "250321"}
        result = generate_canonical_key(svc, "cme", "future", "ESH25", info)
        assert result == "CME:FUTURE:ES-USD-250321@LIN"


# ─── OPTION ───────────────────────────────────────────────────────────────────


class TestOptionKeys:
    def test_basic_option_key(self):
        svc = _make_service(venue="DERIBIT", instrument_type="OPTION")
        info: SymbolInfo = {
            "base_asset": "BTC",
            "quote_asset": "USD",
            "expiry_date": "2025-03-28T08:00:00Z",
            "strike_price": "80000",
            "option_type": "CALL",
        }
        result = generate_canonical_key(svc, "deribit", "option", "BTC-28MAR25-80000-C", info)
        assert result is not None
        assert "OPTION" in result
        assert "CALL" in result

    def test_option_returns_none_for_excluded_deribit_strategy(self):
        svc = _make_service(venue="DERIBIT", instrument_type="OPTION", excluded_strategies=["-FS-"])
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USD"}
        result = generate_canonical_key(svc, "deribit", "option", "BTC-28MAR25-80000-FS-C", info)
        assert result is None

    def test_option_parses_components_when_info_missing(self):
        svc = _make_service(
            venue="DERIBIT",
            instrument_type="OPTION",
            option_components={"expiry_date": "2025-03-28T08:00:00Z", "strike_price": "80000", "option_type": "CALL"},
        )
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USD"}
        result = generate_canonical_key(svc, "deribit", "option", "BTC-28MAR25-80000-C", info)
        assert result is not None
        assert "80000" in result

    def test_option_returns_none_when_missing_all_option_params(self):
        svc = _make_service(
            venue="DERIBIT",
            instrument_type="OPTION",
            option_components={"expiry_date": "", "strike_price": "", "option_type": ""},
        )
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USD"}
        result = generate_canonical_key(svc, "deribit", "option", "BTC-???-C", info)
        assert result is None

    def test_option_deribit_inv_flavor(self):
        svc = _make_service(venue="DERIBIT", instrument_type="OPTION")
        info: SymbolInfo = {
            "base_asset": "BTC",
            "quote_asset": "USD",
            "expiry_date": "250328",
            "strike_price": "80000",
            "option_type": "PUT",
        }
        result = generate_canonical_key(svc, "deribit", "option", "BTC-28MAR25-80000-P", info)
        assert result is not None
        assert "@INV" in result


# ─── Unhandled type ───────────────────────────────────────────────────────────


class TestUnhandledType:
    def test_unhandled_instrument_type_returns_none(self):
        svc = _make_service(venue="BINANCE", instrument_type="INDEX")
        info: SymbolInfo = {"base_asset": "BTC", "quote_asset": "USDT"}
        result = generate_canonical_key(svc, "binance", "index", "BTCINDEX", info)
        assert result is None


@pytest.mark.unit
class TestCanonicalKeyGeneratorFromBoost:
    """Import coverage for canonical_key_generator."""

    def test_import(self):
        from instruments_service.app.core.processors.canonical_key_generator import generate_canonical_key

        assert generate_canonical_key is not None
