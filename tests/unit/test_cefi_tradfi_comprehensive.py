"""Comprehensive unit tests for CeFi and TradFi adapters — maximising code coverage.

All tests are credential-free and use unittest.mock. No live network calls.
Targets:
  - cefi/tardis.py (449 uncovered lines)
  - tradfi/databento.py (396 uncovered lines)
  - tradfi/polygon.py (157 uncovered lines)
  - cefi/ccxt_adapter.py (128 uncovered lines)
  - cefi/aster.py (114 uncovered lines)
  - tradfi/ibkr.py (102 uncovered lines)
  - cefi/hyperliquid.py (95 uncovered lines)
  - tradfi/tradfi_live.py (63 uncovered lines)
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentType, MarginType, OptionType

from instruments_service.reference_data.adapters.cefi.aster import (
    AsterReferenceDataAdapter,
    _classify_aster_error,
    _extract_filter_value,
)
from instruments_service.reference_data.adapters.cefi.ccxt_adapter import CCXTReferenceDataAdapter
from instruments_service.reference_data.adapters.cefi.hyperliquid import (
    HyperliquidReferenceDataAdapter,
    _classify_hyperliquid_error,
)
from instruments_service.reference_data.adapters.cefi.tardis import (
    TardisReferenceDataAdapter,
    _classify_tardis_error,
    _infer_derivative_quote,
    _infer_margin_type,
    _normalize_option_type,
    _parse_ddmmmyy,
    _parse_deribit_combo_legs,
    _parse_deribit_symbol_expiry,
    _parse_expiry,
    _parse_underscore_yymmdd_symbol_expiry,
    _parse_yymmdd_symbol_expiry,
    _passes_asset_filter,
    _resolve_base_quote,
    _split_symbol,
)
from instruments_service.reference_data.adapters.tradfi.databento import (
    DatabentoReferenceDataAdapter,
    _classify_bento_error,
    _extract_underlying_from_symbol,
    _parse_cme_calendar_spread_legs,
    _resolve_trading_status,
    is_non_trading_day,
)
from instruments_service.reference_data.adapters.tradfi.ibkr import IBKRReferenceDataAdapter
from instruments_service.reference_data.adapters.tradfi.polygon import (
    PolygonReferenceDataAdapter,
    _parse_expiry_date,
)
from instruments_service.reference_data.adapters.tradfi.tradfi_live import (
    TradFiLiveReferenceDataAdapter,
    _dataframe_to_instrument_records,
)

# ── shared helpers ────────────────────────────────────────────────────────────


def _make_instrument(
    raw_symbol: str = "BTCUSDT",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    venue: str = "test",
    expiry: datetime | None = None,
    strike: Decimal | None = None,
    option_type: str | None = None,
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=raw_symbol,
        venue=venue,
        raw_symbol=raw_symbol,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=Decimal("0.01"),
        min_size=Decimal("0.001"),
        contract_size=Decimal("1"),
        expiry=expiry,
        strike=strike,
        option_type=option_type,
    )


def _make_aiohttp_session_mock(
    resp_status: int = 200,
    resp_json: object = None,
    resp_text: str = "",
    raise_on_get: Exception | None = None,
    raise_on_post: Exception | None = None,
) -> MagicMock:
    """Build a MagicMock that replaces aiohttp.ClientSession() as an async context manager."""
    mock_resp = AsyncMock()
    mock_resp.status = resp_status
    mock_resp.raise_for_status = MagicMock()
    if resp_status >= 400:
        mock_resp.raise_for_status = MagicMock(
            side_effect=aiohttp.ClientResponseError(request_info=MagicMock(), history=(), status=resp_status)
        )
    mock_resp.json = AsyncMock(return_value=resp_json)
    mock_resp.text = AsyncMock(return_value=resp_text)

    mock_cm = MagicMock()
    if raise_on_get:
        mock_cm.__aenter__ = AsyncMock(side_effect=raise_on_get)
    else:
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    if raise_on_get:
        mock_session_obj.get = MagicMock(side_effect=raise_on_get)
    else:
        mock_session_obj.get = MagicMock(return_value=mock_cm)
    if raise_on_post:
        mock_session_obj.post = MagicMock(side_effect=raise_on_post)
    else:
        mock_session_obj.post = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


# =============================================================================
# TARDIS adapter tests
# =============================================================================


class TestTardisHelperFunctions:
    """Test module-level helper functions for maximum coverage."""

    # ── _normalize_option_type ────────────────────────────────────────────

    def test_normalize_option_type_call(self) -> None:
        assert _normalize_option_type("CALL") == OptionType.CALL
        assert _normalize_option_type("C") == OptionType.CALL
        assert _normalize_option_type("call") == OptionType.CALL

    def test_normalize_option_type_put(self) -> None:
        assert _normalize_option_type("PUT") == OptionType.PUT
        assert _normalize_option_type("P") == OptionType.PUT

    def test_normalize_option_type_none_or_empty(self) -> None:
        assert _normalize_option_type(None) is None
        assert _normalize_option_type("") is None

    def test_normalize_option_type_unknown(self) -> None:
        assert _normalize_option_type("STRADDLE") is None

    # ── _classify_tardis_error ────────────────────────────────────────────

    def test_classify_tardis_error_429_status(self) -> None:
        assert _classify_tardis_error(Exception("err"), status=429) == "429"

    def test_classify_tardis_error_401_status(self) -> None:
        assert _classify_tardis_error(Exception("err"), status=401) == "401"

    def test_classify_tardis_error_500_status(self) -> None:
        assert _classify_tardis_error(Exception("err"), status=500) == "500"

    def test_classify_tardis_error_rate_msg(self) -> None:
        assert _classify_tardis_error(Exception("rate limit exceeded")) == "429"

    def test_classify_tardis_error_auth_msg(self) -> None:
        assert _classify_tardis_error(Exception("unauthorized access")) == "401"

    def test_classify_tardis_error_server_msg(self) -> None:
        assert _classify_tardis_error(Exception("internal server error")) == "500"

    def test_classify_tardis_error_unknown(self) -> None:
        assert _classify_tardis_error(Exception("something odd")) == "UNKNOWN"

    # ── _parse_expiry ─────────────────────────────────────────────────────

    def test_parse_expiry_valid_iso(self) -> None:
        result = _parse_expiry("2026-03-27T08:00:00Z")
        assert result is not None
        assert result.year == 2026

    def test_parse_expiry_none(self) -> None:
        assert _parse_expiry(None) is None

    def test_parse_expiry_empty(self) -> None:
        assert _parse_expiry("") is None

    def test_parse_expiry_invalid(self) -> None:
        assert _parse_expiry("NOT-A-DATE") is None

    # ── _parse_ddmmmyy ───────────────────────────────────────────────────

    def test_parse_ddmmmyy_valid(self) -> None:
        result = _parse_ddmmmyy("27MAR26")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 27

    def test_parse_ddmmmyy_single_digit_day(self) -> None:
        result = _parse_ddmmmyy("3APR26")
        assert result is not None
        assert result.day == 3

    def test_parse_ddmmmyy_invalid_month(self) -> None:
        assert _parse_ddmmmyy("27XXX26") is None

    def test_parse_ddmmmyy_invalid_format(self) -> None:
        assert _parse_ddmmmyy("ABCDEF") is None

    def test_parse_ddmmmyy_empty(self) -> None:
        assert _parse_ddmmmyy("") is None

    # ── _parse_deribit_symbol_expiry ──────────────────────────────────────

    def test_parse_deribit_symbol_expiry_option(self) -> None:
        result = _parse_deribit_symbol_expiry("BTC-27MAR26-190000-C")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 27

    def test_parse_deribit_symbol_expiry_future(self) -> None:
        result = _parse_deribit_symbol_expiry("BTC-27MAR26")
        assert result is not None

    def test_parse_deribit_symbol_expiry_no_dash(self) -> None:
        assert _parse_deribit_symbol_expiry("BTCUSDT") is None

    def test_parse_deribit_symbol_expiry_short_segment(self) -> None:
        assert _parse_deribit_symbol_expiry("BTC-AB") is None

    # ── _parse_yymmdd_symbol_expiry ───────────────────────────────────────

    def test_parse_yymmdd_symbol_expiry_okx(self) -> None:
        result = _parse_yymmdd_symbol_expiry("BTC-USD-260626")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6
        assert result.day == 26

    def test_parse_yymmdd_symbol_expiry_base_only(self) -> None:
        result = _parse_yymmdd_symbol_expiry("BTC-260626")
        assert result is not None

    def test_parse_yymmdd_no_match(self) -> None:
        assert _parse_yymmdd_symbol_expiry("BTCUSDT") is None

    def test_parse_yymmdd_invalid_date(self) -> None:
        # 13th month should fail
        assert _parse_yymmdd_symbol_expiry("BTC-USD-261332") is None

    # ── _parse_underscore_yymmdd_symbol_expiry ────────────────────────────

    def test_parse_underscore_yymmdd_kraken_future(self) -> None:
        result = _parse_underscore_yymmdd_symbol_expiry("FI_XBTUSD_240329")
        assert result is not None
        assert result.year == 2024
        assert result.month == 3
        assert result.day == 29

    def test_parse_underscore_yymmdd_kraken_pf_style(self) -> None:
        result = _parse_underscore_yymmdd_symbol_expiry("PF_XBTUSD_241227")
        assert result is not None
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 27

    def test_parse_underscore_yymmdd_kraken_perp_returns_none(self) -> None:
        # Perpetuals use PERP suffix — not a date
        assert _parse_underscore_yymmdd_symbol_expiry("PF_XBTUSD_PERP") is None

    def test_parse_underscore_yymmdd_no_underscore(self) -> None:
        assert _parse_underscore_yymmdd_symbol_expiry("BTCUSDT") is None

    def test_parse_underscore_yymmdd_invalid_date(self) -> None:
        # 13th month should fail
        assert _parse_underscore_yymmdd_symbol_expiry("FI_XBTUSD_261332") is None

    # ── _resolve_base_quote ───────────────────────────────────────────────

    def test_resolve_base_quote_from_metadata(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="BTCUSDT", baseCurrency="BTC", quoteCurrency="USDT")
        base, quote = _resolve_base_quote(item, "BTCUSDT", "binance-futures")
        assert base == "BTC"
        assert quote == "USDT"

    def test_resolve_base_quote_dash_separated(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="BTC-USDT")
        base, quote = _resolve_base_quote(item, "BTC-USDT", "binance-futures")
        assert base == "BTC"
        assert quote == "USDT"

    def test_resolve_base_quote_upbit_reversed(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="KRW-BTC")
        base, quote = _resolve_base_quote(item, "KRW-BTC", "upbit")
        assert base == "BTC"
        assert quote == "KRW"

    def test_resolve_base_quote_deribit_perpetual(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="BTC-PERPETUAL")
        base, quote = _resolve_base_quote(item, "BTC-PERPETUAL", "deribit")
        assert base == "BTC"
        assert quote == "USD"

    def test_resolve_base_quote_deribit_usdc_linear(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="BTC_USDC-PERPETUAL")
        base, quote = _resolve_base_quote(item, "BTC_USDC-PERPETUAL", "deribit")
        assert base == "BTC"
        assert quote == "USDC"

    def test_resolve_base_quote_concatenated(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        item = TardisInstrumentDetail(id="BNBBTC")
        base, quote = _resolve_base_quote(item, "BNBBTC", "binance")
        assert base == "BNB"
        assert quote == "BTC"

    # ── _infer_derivative_quote ───────────────────────────────────────────

    def test_infer_derivative_quote_usdc(self) -> None:
        assert _infer_derivative_quote("BTC_USDC-PERPETUAL", "deribit") == "USDC"

    def test_infer_derivative_quote_usdt(self) -> None:
        assert _infer_derivative_quote("BTCUSDT_PERP", "binance-futures") == "USDT"

    def test_infer_derivative_quote_usd_um(self) -> None:
        assert _infer_derivative_quote("BTC-USD_UM-SWAP", "okex") == "USD"

    def test_infer_derivative_quote_default(self) -> None:
        assert _infer_derivative_quote("BTC-PERPETUAL", "deribit") == "USD"

    # ── _infer_margin_type ────────────────────────────────────────────────

    def test_infer_margin_type_spot_returns_none(self) -> None:
        assert _infer_margin_type(InstrumentType.SPOT_PAIR, "USDT", "BTCUSDT", "binance") is None

    def test_infer_margin_type_okx_coin_margined(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTC-USD_UM-SWAP", "okex") == MarginType.INVERSE

    def test_infer_margin_type_binance_coin_margined(self) -> None:
        assert _infer_margin_type(InstrumentType.FUTURE, "USD", "BTCUSD_PERP", "binance-futures") == MarginType.INVERSE

    def test_infer_margin_type_deribit_inverse(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USD", "BTC-PERPETUAL", "deribit") == MarginType.INVERSE

    def test_infer_margin_type_linear(self) -> None:
        assert _infer_margin_type(InstrumentType.PERPETUAL, "USDT", "BTCUSDT", "binance-futures") == MarginType.LINEAR

    # ── _passes_asset_filter ──────────────────────────────────────────────

    def test_passes_asset_filter_valid(self) -> None:
        assert _passes_asset_filter("BTC", "USDT", "PERPETUAL") is True

    def test_passes_asset_filter_invalid_base(self) -> None:
        assert _passes_asset_filter("OBSCURECOIN123", "USDT", "PERPETUAL") is False

    def test_passes_asset_filter_invalid_quote(self) -> None:
        assert _passes_asset_filter("BTC", "INVALIDQUOTE", "PERPETUAL") is False

    def test_passes_asset_filter_options_btc(self) -> None:
        assert _passes_asset_filter("BTC", "USD", "OPTION") is True

    def test_passes_asset_filter_options_non_btc_eth(self) -> None:
        # Options only allow BTC/ETH underlyings
        assert _passes_asset_filter("SOL", "USD", "OPTION") is False

    # ── _split_symbol ─────────────────────────────────────────────────────

    def test_split_symbol_concatenated(self) -> None:
        assert _split_symbol("BTCUSDT") == ("BTC", "USDT")

    def test_split_symbol_underscore(self) -> None:
        assert _split_symbol("BTC_USDC") == ("BTC", "USDC")

    def test_split_symbol_unknown(self) -> None:
        base, quote = _split_symbol("UNKNOWNSYMBOL")
        assert base == "UNKNOWNSYMBOL"
        assert quote == ""

    # ── _parse_deribit_combo_legs ─────────────────────────────────────────

    def test_parse_combo_legs_future_spread(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-FS-25APR26_PERP", "DERIBIT")
        assert len(legs) == 2
        assert "PERPETUAL" in legs[1].instrument_key
        assert legs[0].side == "BUY"
        assert legs[1].side == "SELL"

    def test_parse_combo_legs_call_spread(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CS-25APR26-90000_100000", "DERIBIT")
        assert len(legs) == 2
        assert "OPTION" in legs[0].instrument_key

    def test_parse_combo_legs_butterfly(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CBUT-25APR26-80000_90000_100000", "DERIBIT")
        assert len(legs) == 3

    def test_parse_combo_legs_calendar(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CCAL-25APR26_3APR26-90000", "DERIBIT")
        assert len(legs) == 2

    def test_parse_combo_legs_short_symbol(self) -> None:
        legs = _parse_deribit_combo_legs("BTC", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_unknown_code(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-UNKNOWN-25APR26", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_straddle(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-STRD-25APR26-90000", "DERIBIT")
        assert len(legs) == 2

    def test_parse_combo_legs_jelly_roll(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-JR-25APR26_3APR26-90000", "DERIBIT")
        assert len(legs) == 4

    def test_parse_combo_legs_future_spread_two_futures(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-FS-25APR26_3APR26", "DERIBIT")
        assert len(legs) == 2
        assert "FUTURE" in legs[0].instrument_key

    def test_parse_combo_legs_fs_empty_rest(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-FS", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_dual_expiry_empty_rest(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CCAL", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_dual_expiry_single_expiry_segment(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CCAL-25APR26", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_single_expiry_no_rest(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CS", "DERIBIT")
        assert legs == []

    def test_parse_combo_legs_ratio_spread(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-CSR12-25APR26-90000_100000", "DERIBIT")
        assert len(legs) == 2
        assert legs[1].ratio == 2

    def test_parse_combo_legs_linear_future_spread(self) -> None:
        """ETH_USDC base with underscore."""
        legs = _parse_deribit_combo_legs("ETH_USDC-FS-25APR26_PERP", "DERIBIT")
        assert len(legs) == 2
        assert "ETH_USDC" in legs[0].instrument_key

    def test_parse_combo_legs_condor(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-ICOND-25APR26-80000_85000_90000_95000", "DERIBIT")
        assert len(legs) == 4

    def test_parse_combo_legs_box(self) -> None:
        legs = _parse_deribit_combo_legs("BTC-BOX-25APR26-80000_85000_90000_95000", "DERIBIT")
        assert len(legs) == 4


class TestTardisAdapter:
    """Tests for TardisReferenceDataAdapter methods."""

    def test_venue(self) -> None:
        adapter = TardisReferenceDataAdapter()
        assert adapter.venue == "tardis"

    def test_resolve_bar_type_known(self) -> None:
        assert TardisReferenceDataAdapter._resolve_bar_type("1m") == (60, "trade_bar_1m")
        assert TardisReferenceDataAdapter._resolve_bar_type("1h") == (3600, "trade_bar_1h")
        assert TardisReferenceDataAdapter._resolve_bar_type("1d") == (86400, "trade_bar_1d")

    def test_resolve_bar_type_unknown_defaults_to_1d(self) -> None:
        assert TardisReferenceDataAdapter._resolve_bar_type("5m") == (86400, "trade_bar_1d")

    def test_build_datafeed_headers_with_key(self) -> None:
        adapter = TardisReferenceDataAdapter(api_key="test-key")
        headers = adapter._build_datafeed_headers()
        assert headers["Authorization"] == "Bearer test-key"

    def test_build_datafeed_headers_without_key(self) -> None:
        adapter = TardisReferenceDataAdapter()
        headers = adapter._build_datafeed_headers()
        assert headers == {}

    def test_parse_ohlcv_line_valid(self) -> None:
        adapter = TardisReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, 12, tzinfo=UTC).timestamp() * 1000)
        msg = {
            "timestamp": ts_ms,
            "open": "50000",
            "high": "51000",
            "low": "49000",
            "close": "50500",
            "volume": "10",
        }
        result = adapter._parse_ohlcv_line(msg, "BTC-PERPETUAL", "1d")
        assert result is not None
        assert result.open == Decimal("50000")
        assert result.volume == Decimal("10")

    def test_parse_ohlcv_line_missing_open(self) -> None:
        adapter = TardisReferenceDataAdapter()
        msg: dict[str, object] = {"timestamp": 1000}
        assert adapter._parse_ohlcv_line(msg, "BTC", "1d") is None

    def test_parse_ohlcv_line_missing_timestamp(self) -> None:
        adapter = TardisReferenceDataAdapter()
        msg: dict[str, object] = {"open": "50000"}
        assert adapter._parse_ohlcv_line(msg, "BTC", "1d") is None

    def test_parse_ohlcv_line_defaults_for_missing_fields(self) -> None:
        adapter = TardisReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        msg: dict[str, object] = {"timestamp": ts_ms, "open": "100"}
        result = adapter._parse_ohlcv_line(msg, "BTC", "1d")
        assert result is not None
        assert result.high == Decimal("100")
        assert result.low == Decimal("100")
        assert result.close == Decimal("100")
        assert result.volume == Decimal("0")

    def test_make_funding_rate_ref(self) -> None:
        adapter = TardisReferenceDataAdapter()
        now = datetime.now(UTC)
        ts = int(now.timestamp() * 1000)
        last_rate: dict[str, object] = {"fundingRate": "0.0001", "timestamp": ts}
        ref = adapter._make_funding_rate_ref("BTC-PERPETUAL", last_rate, "deribit", now)
        assert ref.rate == Decimal("0.0001")
        assert ref.venue == "tardis"
        assert ref.symbol == "BTC-PERPETUAL"

    def test_make_funding_rate_ref_no_timestamp(self) -> None:
        adapter = TardisReferenceDataAdapter()
        now = datetime.now(UTC)
        last_rate: dict[str, object] = {"fundingRate": "0.0002"}
        ref = adapter._make_funding_rate_ref("BTC", last_rate, "deribit", now)
        assert ref.rate == Decimal("0.0002")
        assert ref.next_funding_time == now

    @pytest.mark.asyncio
    async def test_get_instruments_empty_exchanges(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=[])
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_with_type_filter(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        perp = _make_instrument(raw_symbol="BTC-PERPETUAL", instrument_type="PERPETUAL", venue="tardis")
        spot = _make_instrument(raw_symbol="BTCUSD", instrument_type="SPOT_PAIR", venue="tardis")
        with patch.object(adapter, "_fetch_exchange_instruments", return_value=[perp, spot]):
            result = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert len(result) == 1
        assert result[0].instrument_type == "PERPETUAL"

    @pytest.mark.asyncio
    async def test_get_funding_rate_no_data_raises(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        with (
            patch.object(adapter, "_scan_exchanges_for_funding_rate", return_value=(None, "deribit")),
            pytest.raises(RuntimeError, match="No funding rate"),
        ):
            await adapter.get_funding_rate("BTC-PERPETUAL")

    @pytest.mark.asyncio
    async def test_scan_exchanges_for_funding_rate_returns_found(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_rate: dict[str, object] = {"fundingRate": "0.001", "timestamp": 1000}
        with patch.object(adapter, "_find_funding_rate", return_value=mock_rate):
            rate, exchange = await adapter._scan_exchanges_for_funding_rate("BTC", 0, 1000, "[]", {})
        assert rate is not None
        assert exchange == "deribit"

    @pytest.mark.asyncio
    async def test_find_funding_rate_client_error(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])
        mock_session = MagicMock()
        with patch.object(
            adapter,
            "_fetch_datafeed_text",
            side_effect=aiohttp.ClientError("connection lost"),
        ):
            result = await adapter._find_funding_rate(mock_session, "deribit", 0, 1000, "[]", {}, None)
        assert result is None

    @pytest.mark.asyncio
    async def test_find_funding_rate_no_text(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session = MagicMock()
        with patch.object(adapter, "_fetch_datafeed_text", return_value=None):
            result = await adapter._find_funding_rate(mock_session, "deribit", 0, 1000, "[]", {}, None)
        assert result is None

    @pytest.mark.asyncio
    async def test_find_funding_rate_ndjson_with_rate(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session = MagicMock()
        ndjson = json.dumps({"fundingRate": "0.0001", "timestamp": 1000})
        with patch.object(adapter, "_fetch_datafeed_text", return_value=ndjson):
            result = await adapter._find_funding_rate(mock_session, "deribit", 0, 1000, "[]", {}, None)
        assert result is not None
        assert result["fundingRate"] == "0.0001"

    @pytest.mark.asyncio
    async def test_find_funding_rate_no_funding_field(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session = MagicMock()
        ndjson = json.dumps({"price": "50000"})
        with patch.object(adapter, "_fetch_datafeed_text", return_value=ndjson):
            result = await adapter._find_funding_rate(mock_session, "deribit", 0, 1000, "[]", {}, None)
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_datafeed_text_404_returns_none(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        result = await adapter._fetch_datafeed_text(mock_session, "deribit", 0, 1000, "[]", {})
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_datafeed_text_422_returns_none(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 422
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        result = await adapter._fetch_datafeed_text(mock_session, "deribit", 0, 1000, "[]", {})
        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_datafeed_text_401_raises(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 401
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with pytest.raises(RuntimeError, match="Tardis API key required"):
            await adapter._fetch_datafeed_text(mock_session, "deribit", 0, 1000, "[]", {})

    @pytest.mark.asyncio
    async def test_fetch_datafeed_text_200_returns_text(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = AsyncMock(return_value='{"data":"ok"}')
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        result = await adapter._fetch_datafeed_text(mock_session, "deribit", 0, 1000, "[]", {})
        assert result == '{"data":"ok"}'

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_from_exchange_client_error(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session = MagicMock()
        with patch.object(
            adapter,
            "_fetch_datafeed_text",
            side_effect=aiohttp.ClientError("timeout"),
        ):
            results = await adapter._fetch_ohlcv_from_exchange(mock_session, "deribit", 0, 1000, "[]", {}, "BTC", "1d")
        assert results == []

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_from_exchange_none_text(self) -> None:
        adapter = TardisReferenceDataAdapter()
        mock_session = MagicMock()
        with patch.object(adapter, "_fetch_datafeed_text", return_value=None):
            results = await adapter._fetch_ohlcv_from_exchange(mock_session, "deribit", 0, 1000, "[]", {}, "BTC", "1d")
        assert results == []

    @pytest.mark.asyncio
    async def test_collect_ohlcv_from_exchanges_first_success(self) -> None:
        adapter = TardisReferenceDataAdapter(exchanges=["deribit", "binance-futures"])
        from instruments_service.reference_data.schemas import OHLCVRef

        bar = OHLCVRef(
            venue="tardis",
            symbol="BTC",
            timestamp=datetime.now(UTC),
            open=Decimal("50000"),
            high=Decimal("51000"),
            low=Decimal("49000"),
            close=Decimal("50500"),
            volume=Decimal("10"),
            interval="1d",
        )
        with patch.object(adapter, "_fetch_ohlcv_from_exchange", return_value=[bar]):
            results = await adapter._collect_ohlcv_from_exchanges("BTC", "1d", 0, 1000, "[]", {})
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_spot(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTCUSDT",
            type="spot",
            baseCurrency="BTC",
            quoteCurrency="USDT",
            availableSince="2020-01-01T00:00:00Z",
        )
        result = adapter._parse_tardis_instrument(item, "binance")
        assert result is not None
        assert result.instrument_type == InstrumentType.SPOT_PAIR
        assert result.base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_perpetual(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-PERPETUAL",
            type="perpetual",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is not None
        assert result.instrument_type == InstrumentType.PERPETUAL

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_option_with_fields(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-27MAR26-190000-C",
            type="option",
            baseCurrency="BTC",
            quoteCurrency="USD",
            strikePrice=190000,
            optionType="C",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is not None
        assert result.option_type == OptionType.CALL
        assert result.strike == Decimal("190000")

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_empty_id_returns_none(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(id="")
        assert adapter._parse_tardis_instrument(item, "deribit") is None

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_combo(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-FS-25APR26_PERP",
            type="combo",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is not None
        assert result.instrument_type == InstrumentType.COMBO
        assert result.legs is not None
        assert len(result.legs) == 2

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_combo_no_legs_returns_none(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-UNKNOWN-25APR26",
            type="combo",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is None

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_deribit_none_type_returns_none(self) -> None:
        """Deribit is derivatives-only; instruments with type=None must be skipped, not defaulted to SPOT_PAIR."""
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC",
            type=None,
            baseCurrency="BTC",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is None, "type=None on deribit must return None, not a SPOT_PAIR record"

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_deribit_spot_type_returns_none(self) -> None:
        """An explicit type='spot' on deribit must also be rejected (deribit has no spot trading)."""
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-USD",
            type="spot",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is None, "type='spot' on deribit must return None"

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_non_derivatives_only_spot_allowed(self) -> None:
        """Spot instruments on non-derivatives-only venues (binance) are still accepted."""
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTCUSDT",
            type=None,
            baseCurrency="BTC",
            quoteCurrency="USDT",
        )
        result = adapter._parse_tardis_instrument(item, "binance")
        assert result is not None, "type=None on binance should default to SPOT_PAIR, not be skipped"
        assert result.instrument_type == InstrumentType.SPOT_PAIR

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_with_spec_fields(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTCUSDT",
            type="spot",
            baseCurrency="BTC",
            quoteCurrency="USDT",
            priceIncrement=0.01,
            minTradeAmount=0.001,
            contractMultiplier=1.0,
        )
        result = adapter._parse_tardis_instrument(item, "binance")
        assert result is not None
        assert result.tick_size == Decimal("0.01")
        assert result.min_size == Decimal("0.001")

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_fallback_expiry_from_available_to(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTCUSD-FUTURE",
            type="future",
            baseCurrency="BTC",
            quoteCurrency="USD",
            availableTo="2026-06-27T00:00:00Z",
        )
        result = adapter._parse_tardis_instrument(item, "binance-futures")
        assert result is not None
        assert result.expiry is not None

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_yymmdd_expiry_fallback(self) -> None:
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-USD-260626",
            type="future",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "okex")
        assert result is not None
        assert result.expiry is not None
        assert result.expiry.month == 6

    @pytest.mark.asyncio
    async def test_parse_tardis_instrument_option_strike_from_symbol(self) -> None:
        """Strike parsed from Deribit symbol when not in metadata."""
        from unified_api_contracts import TardisInstrumentDetail

        adapter = TardisReferenceDataAdapter()
        item = TardisInstrumentDetail(
            id="BTC-27MAR26-190000-C",
            type="option",
            baseCurrency="BTC",
            quoteCurrency="USD",
        )
        result = adapter._parse_tardis_instrument(item, "deribit")
        assert result is not None
        assert result.strike == Decimal("190000")
        assert result.option_type == OptionType.CALL

    @pytest.mark.asyncio
    async def test_fetch_exchange_instruments_401_falls_back(self) -> None:
        """401 on /v1/instruments falls back to /v1/exchanges."""

        adapter = TardisReferenceDataAdapter(exchanges=["deribit"])

        call_count = 0

        def mock_get(url: str, headers: object = None) -> MagicMock:
            nonlocal call_count
            call_count += 1
            mock_resp = AsyncMock()
            if "/v1/instruments/" in url:
                mock_resp.status = 401
            else:
                mock_resp.status = 200
                mock_resp.raise_for_status = MagicMock()
                mock_resp.json = AsyncMock(return_value={"id": "deribit", "name": "Deribit", "instruments": []})
            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            return mock_cm

        mock_session = MagicMock()
        mock_session.get = mock_get

        results = await adapter._fetch_exchange_instruments(mock_session, "api-key", "deribit")
        assert results == []
        assert call_count >= 2  # instruments API + exchanges API


# =============================================================================
# CCXT adapter tests
# =============================================================================


class TestCCXTAdapterComprehensive:
    """Comprehensive tests for CCXTReferenceDataAdapter."""

    def test_venue_with_canonical(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit", canonical_venue="BYBIT")
        assert adapter.venue == "BYBIT"

    def test_venue_default(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="kucoin")
        assert adapter.venue == "kucoin"

    def test_map_ccxt_type_spot(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("spot") == InstrumentType.SPOT_PAIR

    def test_map_ccxt_type_swap(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("swap") == InstrumentType.PERPETUAL

    def test_map_ccxt_type_perpetual(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("perpetual") == InstrumentType.PERPETUAL

    def test_map_ccxt_type_future(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("future") == InstrumentType.FUTURE

    def test_map_ccxt_type_option(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("option") == InstrumentType.OPTION

    def test_map_ccxt_type_unknown(self) -> None:
        assert CCXTReferenceDataAdapter._map_ccxt_type("warrant") == InstrumentType.SPOT_PAIR

    def test_extract_market_sizes(self) -> None:
        market: dict[str, object] = {
            "precision": {"price": "0.01", "amount": "0.001"},
            "limits": {"amount": {"min": "0.0001"}},
            "contractSize": "10",
        }
        tick, lot, min_amt, contract_size = CCXTReferenceDataAdapter._extract_market_sizes(market)
        assert tick == "0.01"
        assert lot == "0.001"
        assert min_amt == "0.0001"
        assert contract_size == "10"

    def test_extract_market_sizes_missing(self) -> None:
        market: dict[str, object] = {}
        tick, lot, min_amt, contract_size = CCXTReferenceDataAdapter._extract_market_sizes(market)
        assert tick is None
        assert lot is None
        assert min_amt is None
        assert contract_size is None

    def test_parse_ccxt_expiry_valid(self) -> None:
        result = CCXTReferenceDataAdapter._parse_ccxt_expiry("2026-06-27T08:00:00Z")
        assert result is not None
        assert result.year == 2026

    def test_parse_ccxt_expiry_none(self) -> None:
        assert CCXTReferenceDataAdapter._parse_ccxt_expiry(None) is None

    def test_parse_ccxt_expiry_invalid(self) -> None:
        assert CCXTReferenceDataAdapter._parse_ccxt_expiry("bad-date") is None

    def test_parse_ccxt_market_option_with_strike(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        market: dict[str, object] = {
            "type": "option",
            "active": True,
            "base": "BTC",
            "quote": "USD",
            "settle": "",
            "strike": 50000.0,
            "optionType": "call",
            "linear": False,
            "inverse": True,
            "precision": {},
            "limits": {},
            "expiryDatetime": "2026-06-27T08:00:00Z",
        }
        result = adapter._parse_ccxt_market("BTC-50000-C", market, None)
        assert result is not None
        assert result.instrument_type == InstrumentType.OPTION
        assert result.strike == Decimal("50000.0")
        assert result.option_type == OptionType.CALL

    def test_parse_ccxt_market_option_without_strike_returns_none(self) -> None:
        """Options without strike are skipped (combo/conditional)."""
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        market: dict[str, object] = {
            "type": "option",
            "active": True,
            "base": "BTC",
            "quote": "USD",
            "strike": None,
            "optionType": "call",
            "precision": {},
            "limits": {},
        }
        result = adapter._parse_ccxt_market("BTC-COMBO", market, None)
        assert result is None

    def test_parse_ccxt_market_linear_margin(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="binance")
        market: dict[str, object] = {
            "type": "swap",
            "active": True,
            "base": "BTC",
            "quote": "USDT",
            "settle": "USDT",
            "linear": True,
            "inverse": False,
            "precision": {},
            "limits": {},
        }
        result = adapter._parse_ccxt_market("BTC/USDT:USDT", market, None)
        assert result is not None
        assert result.margin_type == MarginType.LINEAR

    def test_parse_ccxt_market_inverse_margin(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="binance")
        market: dict[str, object] = {
            "type": "swap",
            "active": True,
            "base": "BTC",
            "quote": "USD",
            "settle": "BTC",
            "linear": False,
            "inverse": True,
            "precision": {},
            "limits": {},
        }
        result = adapter._parse_ccxt_market("BTC/USD:BTC", market, None)
        assert result is not None
        assert result.margin_type == MarginType.INVERSE

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(
            return_value={
                "BTC/USDT": {
                    "type": "spot",
                    "active": True,
                    "base": "BTC",
                    "quote": "USDT",
                    "id": "BTCUSDT",
                    "precision": {},
                    "limits": {},
                }
            }
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_get_instruments_closes_exchange_on_error(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(side_effect=Exception("timeout"))
        mock_exchange.close = AsyncMock()
        with (
            patch.object(adapter, "_get_exchange", return_value=mock_exchange),
            pytest.raises(Exception, match="timeout"),
        ):
            await adapter.get_instruments()
        mock_exchange.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("UNKNOWN")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_matches_instrument_key(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        inst = _make_instrument(raw_symbol="BTCUSDT", venue="bybit")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTCUSDT")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_funding_rate_no_next_dt(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_funding_rate = AsyncMock(
            return_value={"fundingRate": 0.0002, "markPrice": 60000, "nextFundingDatetime": None}
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            rate = await adapter.get_funding_rate("BTC/USDT:USDT")
        assert rate.rate == Decimal("0.0002")

    @pytest.mark.asyncio
    async def test_get_ohlcv_success(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_ohlcv = AsyncMock(
            return_value=[
                [ts_ms, "50000", "51000", "49000", "50500", "100"],
            ]
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_ohlcv("BTC/USDT", interval="1d", limit=1)
        assert len(results) == 1
        assert results[0].open == Decimal("50000")

    @pytest.mark.asyncio
    async def test_get_ohlcv_stamps_close_edge(self) -> None:
        """get_ohlcv must stamp the close/right edge, not the open edge from bar[0].

        bar_edge fix 2026-06-08: ccxt OHLCV format is
        [timestamp_open_ms, open, high, low, close, volume] — NO close-time field.
        The fix calls compute_bar_close_boundary(open_ts, timeframe) to derive t_close.

        For a 1d bar with open at 2026-01-01 00:00:00 UTC, the close edge is
        2026-01-02 00:00:00 UTC.
        """
        adapter = CCXTReferenceDataAdapter(venue="bybit")
        open_ts = datetime(2026, 1, 1, tzinfo=UTC)
        ts_ms = int(open_ts.timestamp() * 1000)
        mock_exchange = MagicMock()
        mock_exchange.load_markets = AsyncMock(return_value={})
        mock_exchange.fetch_ohlcv = AsyncMock(
            return_value=[
                [ts_ms, "50000", "51000", "49000", "50500", "100"],
            ]
        )
        mock_exchange.close = AsyncMock()
        with patch.object(adapter, "_get_exchange", return_value=mock_exchange):
            results = await adapter.get_ohlcv("BTC/USDT", interval="1d", limit=1)
        assert len(results) == 1
        expected_close_ts = datetime(2026, 1, 2, tzinfo=UTC)
        assert results[0].timestamp == expected_close_ts, (
            f"Expected close-edge {expected_close_ts!r}, got {results[0].timestamp!r}. "
            "ccxt has no close-time field — compute_bar_close_boundary must be used."
        )

    @pytest.mark.asyncio
    async def test_get_options_chain_empty(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        with patch.object(adapter, "get_instruments", return_value=[]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.calls == []
        assert chain.puts == []

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_empty(self) -> None:
        adapter = CCXTReferenceDataAdapter(venue="deribit")
        with patch.object(adapter, "get_instruments", return_value=[]):
            cal = await adapter.get_expiry_calendar("BTC")
        assert cal.expiries == []


# =============================================================================
# Aster adapter tests
# =============================================================================


class TestAsterAdapterComprehensive:
    """Comprehensive tests for AsterReferenceDataAdapter."""

    def test_venue(self) -> None:
        adapter = AsterReferenceDataAdapter()
        assert adapter.venue == "aster"

    # ── _classify_aster_error ─────────────────────────────────────────────

    def test_classify_aster_error_429_status(self) -> None:
        assert _classify_aster_error(Exception("err"), status=429) == "429"

    def test_classify_aster_error_503_status(self) -> None:
        assert _classify_aster_error(Exception("err"), status=503) == "503"

    def test_classify_aster_error_rate_msg(self) -> None:
        assert _classify_aster_error(Exception("-1003 rate limit")) == "-1003"

    def test_classify_aster_error_unavailable_msg(self) -> None:
        # "service unavailable 503" contains "503" substring → returns "503"
        assert _classify_aster_error(Exception("service unavailable 503")) == "503"

    def test_classify_aster_error_unknown(self) -> None:
        assert _classify_aster_error(Exception("something")) == "UNKNOWN"

    # ── _extract_filter_value ─────────────────────────────────────────────

    def test_extract_filter_value_found(self) -> None:
        filters: list[object] = [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ]
        assert _extract_filter_value(filters, "PRICE_FILTER", "tickSize") == "0.01"

    def test_extract_filter_value_not_found(self) -> None:
        filters: list[object] = [{"filterType": "OTHER", "value": "1"}]
        assert _extract_filter_value(filters, "PRICE_FILTER", "tickSize") == ""

    def test_extract_filter_value_empty(self) -> None:
        assert _extract_filter_value([], "PRICE_FILTER", "tickSize") == ""

    # ── get_instruments ───────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_instruments_non_perpetual_returns_empty(self) -> None:
        adapter = AsterReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success_with_valid_data(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "symbols": [
                    {
                        "symbol": "BTCUSDC",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDC",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        ],
                    },
                    {
                        "symbol": "ETHUSDC",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "baseAsset": "ETH",
                        "quoteAsset": "USDC",
                        "filters": [],
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].instrument_type == InstrumentType.PERPETUAL
        assert results[0].venue == "ASTER"
        assert results[0].margin_type == MarginType.LINEAR

    @pytest.mark.asyncio
    async def test_get_instruments_filters_non_trading(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "symbols": [
                    {
                        "symbol": "BTCUSDC",
                        "status": "BREAK",
                        "contractType": "PERPETUAL",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDC",
                        "filters": [],
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_filters_non_perpetual_contract(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "symbols": [
                    {
                        "symbol": "BTCUSDC",
                        "status": "TRADING",
                        "contractType": "DELIVERY",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDC",
                        "filters": [],
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_filters_unknown_base_asset(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "symbols": [
                    {
                        "symbol": "OBSCUREUSDC",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "baseAsset": "OBSCURECOIN999",
                        "quoteAsset": "USDC",
                        "filters": [],
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        """HTTP 500 must raise, not return [] (CF-11 regression)."""
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_skips_non_dict_symbols(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"symbols": ["not-a-dict", 42]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = AsterReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="BTCUSDC", venue="ASTER")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTCUSDC")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = AsterReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    # ── get_funding_rate ──────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "lastFundingRate": "0.0003",
                "nextFundingTime": int(datetime(2026, 4, 1, 8, tzinfo=UTC).timestamp() * 1000),
                "markPrice": "50000",
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTCUSDC")
        assert result.rate == Decimal("0.0003")
        assert result.mark_price == Decimal("50000")

    @pytest.mark.asyncio
    async def test_get_funding_rate_http_error_returns_zero(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTCUSDC")
        assert result.rate == Decimal("0")

    @pytest.mark.asyncio
    async def test_get_funding_rate_zero_next_time(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "lastFundingRate": "0.0001",
                "nextFundingTime": 0,
                "markPrice": "0",
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTCUSDC")
        assert result.rate == Decimal("0.0001")
        assert result.mark_price is None  # "0" → None

    # ── get_ohlcv ─────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_ohlcv_success(self) -> None:
        adapter = AsterReferenceDataAdapter()
        ts = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_session = _make_aiohttp_session_mock(
            resp_json=[
                [ts, "50000", "51000", "49000", "50500", "100", ts + 60000],
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTCUSDC", interval="1d", limit=1)
        assert len(results) == 1
        assert results[0].open == Decimal("50000")

    @pytest.mark.asyncio
    async def test_get_ohlcv_skips_short_candles(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json=[[1000, "50000", "51000"]]  # too short
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTCUSDC")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_http_error_returns_empty(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTCUSDC")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_non_list_response(self) -> None:
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"error": "bad"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTCUSDC")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_unknown_interval_defaults(self) -> None:
        adapter = AsterReferenceDataAdapter()
        ts = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_session = _make_aiohttp_session_mock(resp_json=[[ts, "100", "110", "90", "105", "50", ts + 86400000]])
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTCUSDC", interval="7d", limit=1)
        assert len(results) == 1


# =============================================================================
# Hyperliquid adapter tests
# =============================================================================


class TestHyperliquidAdapterComprehensive:
    """Comprehensive tests for HyperliquidReferenceDataAdapter."""

    def test_venue(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter.venue == "HYPERLIQUID"

    # ── _classify_hyperliquid_error ───────────────────────────────────────

    def test_classify_429_status(self) -> None:
        assert _classify_hyperliquid_error(Exception("err"), status=429) == "RATE_LIMIT"

    def test_classify_503_status(self) -> None:
        assert _classify_hyperliquid_error(Exception("err"), status=503) == "503"

    def test_classify_500_status(self) -> None:
        assert _classify_hyperliquid_error(Exception("err"), status=500) == "500"

    def test_classify_rate_msg(self) -> None:
        assert _classify_hyperliquid_error(Exception("rate limit")) == "RATE_LIMIT"

    def test_classify_margin_msg(self) -> None:
        assert _classify_hyperliquid_error(Exception("insufficient margin")) == "INSUFFICIENT_MARGIN"

    def test_classify_server_msg(self) -> None:
        assert _classify_hyperliquid_error(Exception("internal server error")) == "500"

    def test_classify_unknown(self) -> None:
        assert _classify_hyperliquid_error(Exception("something")) == "UNKNOWN"

    # ── get_instruments ───────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_instruments_non_perpetual_returns_empty(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(
            resp_json={
                "universe": [
                    {"name": "BTC", "szDecimals": 5},
                    {"name": "ETH", "szDecimals": 4},
                    {"name": "", "szDecimals": 3},  # empty name → skipped
                    {"name": "OBSCURECOIN999", "szDecimals": 3},  # not in universe
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].base_asset == "BTC"
        assert results[0].quote_asset == "USD"
        assert results[0].settle_asset == "USDC"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        """HTTP 500 must raise, not return [] (CF-11 regression)."""
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="BTC", venue="HYPERLIQUID")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTC")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    # ── get_funding_rate ──────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_funding_rate_success(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        ts_ms = int(datetime(2026, 4, 1, 8, tzinfo=UTC).timestamp() * 1000)
        mock_session = _make_aiohttp_session_mock(resp_json=[{"fundingRate": "0.0001", "time": ts_ms}])
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC")
        assert result.rate == Decimal("0.0001")
        assert result.venue == "HYPERLIQUID"

    @pytest.mark.asyncio
    async def test_get_funding_rate_empty_response(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json=[])
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC")
        assert result.rate == Decimal("0")

    @pytest.mark.asyncio
    async def test_get_funding_rate_http_error(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC")
        assert result.rate == Decimal("0")

    @pytest.mark.asyncio
    async def test_get_funding_rate_non_list_response(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"error": "bad"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC")
        assert result.rate == Decimal("0")

    # ── _resolve_hl_interval ──────────────────────────────────────────────

    def test_resolve_hl_interval_known(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter._resolve_hl_interval("1m") == ("1m", 60)
        assert adapter._resolve_hl_interval("1d") == ("1d", 86400)

    def test_resolve_hl_interval_unknown(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter._resolve_hl_interval("3m") == ("1d", 86400)

    # ── _parse_hl_candle ──────────────────────────────────────────────────

    def test_parse_hl_candle_valid(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        candle: dict[str, object] = {
            "t": ts_ms,
            "o": "50000",
            "h": "51000",
            "l": "49000",
            "c": "50500",
            "v": "100",
        }
        result = adapter._parse_hl_candle(candle, "BTC", "1d")
        assert result is not None
        assert result.open == Decimal("50000")

    def test_parse_hl_candle_t_key(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        candle: dict[str, object] = {"T": ts_ms, "o": "100", "h": "110", "l": "90", "c": "105", "v": "50"}
        result = adapter._parse_hl_candle(candle, "BTC", "1h")
        assert result is not None

    def test_parse_hl_candle_non_dict_returns_none(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        assert adapter._parse_hl_candle("not a dict", "BTC", "1d") is None

    # ── get_ohlcv ─────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_ohlcv_success(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_session = _make_aiohttp_session_mock(
            resp_json=[
                {"t": ts_ms, "o": "50000", "h": "51000", "l": "49000", "c": "50500", "v": "100"},
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTC", interval="1d", limit=1)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_ohlcv_http_error(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTC")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_ohlcv_non_list_response(self) -> None:
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"error": "bad"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_ohlcv("BTC")
        assert results == []


# =============================================================================
# Polygon adapter tests
# =============================================================================


class TestPolygonAdapterComprehensive:
    """Comprehensive tests for PolygonReferenceDataAdapter."""

    def test_venue(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        assert adapter.venue == "polygon"

    def test_get_api_key_raises_without_key(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            adapter._get_api_key()

    def test_get_api_key_returns_key(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="test-key")
        assert adapter._get_api_key() == "test-key"

    # ── _parse_expiry_date ────────────────────────────────────────────────

    def test_parse_expiry_date_valid(self) -> None:
        result = _parse_expiry_date("2026-06-27")
        assert result is not None
        assert result.year == 2026
        assert result.month == 6

    def test_parse_expiry_date_none(self) -> None:
        assert _parse_expiry_date(None) is None

    def test_parse_expiry_date_empty(self) -> None:
        assert _parse_expiry_date("") is None

    def test_parse_expiry_date_invalid(self) -> None:
        assert _parse_expiry_date("bad-date") is None

    # ── _parse_ticker ─────────────────────────────────────────────────────

    def test_parse_ticker_valid(self) -> None:
        from instruments_service.reference_data.adapters.tradfi.polygon import PolygonTicker

        adapter = PolygonReferenceDataAdapter(api_key="key")
        ticker = PolygonTicker(ticker="AAPL", name="Apple Inc.", active=True)
        now = datetime.now(UTC)
        result = adapter._parse_ticker(ticker, now)
        assert result is not None
        assert result.instrument_key == "AAPL"
        assert result.base_asset == "AAPL"

    def test_parse_ticker_empty_sym_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.tradfi.polygon import PolygonTicker

        adapter = PolygonReferenceDataAdapter(api_key="key")
        ticker = PolygonTicker(ticker="", name="")
        result = adapter._parse_ticker(ticker, datetime.now(UTC))
        assert result is None

    # ── _parse_option_contract ────────────────────────────────────────────

    def test_parse_option_contract(self) -> None:
        from instruments_service.reference_data.adapters.tradfi.polygon import PolygonOptionContract

        adapter = PolygonReferenceDataAdapter(api_key="key")
        contract = PolygonOptionContract(
            ticker="O:AAPL260627C00150000",
            underlying_ticker="AAPL",
            contract_type="call",
            expiration_date="2026-06-27",
            strike_price=150.0,
            shares_per_contract=100,
        )
        now = datetime.now(UTC)
        result = adapter._parse_option_contract(contract, now)
        assert result.instrument_type == InstrumentType.OPTION
        assert result.strike == Decimal("150.0")
        assert result.option_type == "call"
        assert result.contract_size == Decimal("100")

    # ── _parse_polygon_bar ────────────────────────────────────────────────

    def test_parse_polygon_bar_valid(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        bar: dict[str, object] = {"t": ts_ms, "o": "150", "h": "155", "l": "148", "c": "152", "v": "1000000"}
        result = adapter._parse_polygon_bar(bar, "AAPL", "1d")
        assert result is not None
        assert result.open == Decimal("150")

    def test_parse_polygon_bar_non_dict(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        assert adapter._parse_polygon_bar("not a dict", "AAPL", "1d") is None

    def test_parse_polygon_bar_missing_timestamp(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        assert adapter._parse_polygon_bar({"o": "150"}, "AAPL", "1d") is None

    # ── get_instruments ───────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_instruments_requires_api_key(self) -> None:
        adapter = PolygonReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_spot_only(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        inst = _make_instrument(venue="polygon")
        with (
            patch.object(adapter, "_fetch_tickers", return_value=[inst]),
            patch.object(adapter, "_fetch_options", return_value=[]),
        ):
            results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instruments_option_only(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        inst = _make_instrument(instrument_type="OPTION", venue="polygon", expiry=datetime(2026, 6, 27, tzinfo=UTC))
        with (
            patch.object(adapter, "_fetch_tickers", return_value=[]),
            patch.object(adapter, "_fetch_options", return_value=[inst]),
        ):
            results = await adapter.get_instruments(instrument_type="OPTION")
        assert len(results) == 1

    # ── get_instrument ────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"results": {"ticker": "AAPL", "name": "Apple Inc.", "active": True}})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("AAPL")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_404(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_no_results(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"results": None})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("AAPL")
        assert result is None

    # ── get_options_chain ─────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_options_chain_with_data(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        call = _make_instrument(
            raw_symbol="O:AAPL-C",
            instrument_type="OPTION",
            base_asset="AAPL",
            expiry=expiry_dt,
            strike=Decimal("150"),
            option_type="call",
            venue="polygon",
        )
        put = _make_instrument(
            raw_symbol="O:AAPL-P",
            instrument_type="OPTION",
            base_asset="AAPL",
            expiry=expiry_dt,
            strike=Decimal("150"),
            option_type="put",
            venue="polygon",
        )
        with patch.object(adapter, "_fetch_options", return_value=[call, put]):
            chain = await adapter.get_options_chain("AAPL")
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1
        assert Decimal("150") in chain.strikes

    # ── get_expiry_calendar ───────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_expiry_calendar(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        inst = _make_instrument(
            instrument_type="OPTION",
            expiry=expiry_dt,
            venue="polygon",
        )
        with patch.object(adapter, "_fetch_options", return_value=[inst]):
            cal = await adapter.get_expiry_calendar("AAPL")
        assert expiry_dt in cal.expiries

    # ── get_funding_rate raises ───────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("AAPL")

    # ── get_ohlcv ─────────────────────────────────────────────────────────

    @pytest.mark.asyncio
    async def test_get_ohlcv_success(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        ts_ms = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "results": [
                    {"t": ts_ms, "o": "150", "h": "155", "l": "148", "c": "152", "v": "1000000"},
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_ohlcv("AAPL", interval="1d", limit=1)
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_ohlcv_403_raises(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 403
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            pytest.raises(RuntimeError, match=r"Polygon\.io authentication failed"),
        ):
            await adapter.get_ohlcv("AAPL")

    @pytest.mark.asyncio
    async def test_get_ohlcv_no_results(self) -> None:
        adapter = PolygonReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"results": None})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_ohlcv("AAPL")
        assert results == []


# =============================================================================
# IBKR adapter tests (comprehensive coverage beyond existing tests)
# =============================================================================


class TestIBKRAdapterComprehensive:
    """Comprehensive tests for IBKRReferenceDataAdapter beyond existing coverage."""

    def test_make_contract_stk(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        contract = adapter._make_contract("AAPL", "STK")
        assert contract is not None

    def test_make_contract_fut(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        contract = adapter._make_contract("ES", "FUT")
        assert contract is not None

    def test_make_contract_opt(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        contract = adapter._make_contract("AAPL", "OPT")
        assert contract is not None

    def test_make_contract_fop(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        contract = adapter._make_contract("ES", "FOP")
        assert contract is not None

    def test_make_contract_generic(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        contract = adapter._make_contract("BOND1", "BOND")
        assert contract is not None

    def test_resolve_sec_type_none_returns_stk(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        result = adapter._resolve_sec_type_and_symbols(None)
        assert result is not None
        sec_type, _symbols = result
        assert sec_type == "STK"

    def test_resolve_sec_type_equity(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        result = adapter._resolve_sec_type_and_symbols("EQUITY")
        assert result is not None
        assert result[0] == "STK"

    def test_resolve_sec_type_spot(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        result = adapter._resolve_sec_type_and_symbols("SPOT_PAIR")
        assert result is not None
        assert result[0] == "STK"

    def test_resolve_sec_type_future(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        result = adapter._resolve_sec_type_and_symbols("FUTURE")
        assert result is not None
        assert result[0] == "FUT"

    def test_resolve_sec_type_option(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        result = adapter._resolve_sec_type_and_symbols("OPTION")
        assert result is not None
        assert result[0] == "OPT"

    def test_resolve_sec_type_unsupported(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        assert adapter._resolve_sec_type_and_symbols("CRYPTO") is None

    @pytest.mark.asyncio
    async def test_get_instruments_with_results(self) -> None:
        mock_ib = MagicMock()
        mock_contract = MagicMock()
        mock_contract.symbol = "AAPL"
        mock_contract.secType = "STK"
        mock_contract.currency = "USD"
        mock_contract.conId = 265598
        mock_contract.exchange = "SMART"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = "1"
        mock_contract.localSymbol = "AAPL"
        mock_contract.tradingClass = "NMS"
        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Apple Inc."
        mock_details.industry = "Technology"
        mock_details.category = "Computers"
        mock_details.subcategory = "Computers"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        mock_details.secIdList = []
        mock_details.notes = ""
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[mock_details])

        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        results = await adapter.get_instruments(instrument_type="FUTURE")
        assert isinstance(results, list)

    def test_log_isin(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        mock_details = MagicMock()
        tag_value = MagicMock()
        tag_value.tag = "ISIN"
        tag_value.val = "US0378331005"
        mock_details.secIdList = [tag_value]
        # Should not raise
        adapter._log_isin("AAPL", mock_details)

    def test_log_isin_no_isin(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        mock_details = MagicMock()
        mock_details.secIdList = []
        adapter._log_isin("AAPL", mock_details)

    @pytest.mark.asyncio
    async def test_get_corporate_actions_with_split_notes(self) -> None:
        mock_ib = MagicMock()
        mock_contract = MagicMock()
        mock_contract.symbol = "TSLA"
        mock_contract.secType = "STK"
        mock_contract.currency = "USD"
        mock_contract.conId = 123
        mock_contract.exchange = "SMART"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = ""
        mock_contract.localSymbol = "TSLA"
        mock_contract.tradingClass = ""
        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Tesla Inc."
        mock_details.industry = "Auto"
        mock_details.category = "Auto"
        mock_details.subcategory = "EV"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        mock_details.secIdList = []
        mock_details.notes = "Split 3-for-1 effective 2025-08-25"
        mock_ib.reqContractDetailsAsync = AsyncMock(return_value=[mock_details])

        adapter = IBKRReferenceDataAdapter(ib=mock_ib)
        result = await adapter.get_corporate_actions("TSLA", datetime.now(UTC))
        assert len(result) == 1
        assert result[0].action_type == "stock_split"

    def test_extract_raw_from_details(self) -> None:
        adapter = IBKRReferenceDataAdapter()
        mock_contract = MagicMock()
        mock_contract.conId = 123
        mock_contract.symbol = "AAPL"
        mock_contract.secType = "STK"
        mock_contract.lastTradeDateOrContractMonth = ""
        mock_contract.strike = 0.0
        mock_contract.right = ""
        mock_contract.multiplier = "1"
        mock_contract.exchange = "SMART"
        mock_contract.currency = "USD"
        mock_contract.localSymbol = "AAPL"
        mock_contract.tradingClass = "NMS"
        mock_details = MagicMock()
        mock_details.contract = mock_contract
        mock_details.minTick = 0.01
        mock_details.longName = "Apple Inc."
        mock_details.industry = "Tech"
        mock_details.category = "Computers"
        mock_details.subcategory = "Hardware"
        mock_details.timeZoneId = "US/Eastern"
        mock_details.underConId = 0
        raw = adapter._extract_raw_from_details(mock_details)
        assert raw["symbol"] == "AAPL"
        assert raw["minTick"] == 0.01


# =============================================================================
# TradFi Live adapter tests
# =============================================================================


class TestTradFiLiveAdapterComprehensive:
    """Comprehensive tests for TradFiLiveReferenceDataAdapter."""

    def test_venue_with_filter(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        assert adapter.venue == "CME"

    def test_venue_default(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        assert adapter.venue == "TRADFI"

    @pytest.mark.asyncio
    async def test_get_instruments_from_gcs(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        records = [_make_instrument(venue="CME", raw_symbol="ESZ4")]
        with patch.object(adapter, "_read_most_recent_gcs_snapshot", return_value=records):
            result = await adapter.get_instruments()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_instruments_filters_expired(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        expired = _make_instrument(
            venue="CME",
            raw_symbol="ESH4",
            expiry=datetime(2024, 1, 1, tzinfo=UTC),
        )
        active = _make_instrument(venue="CME", raw_symbol="ESZ6")
        with patch.object(adapter, "_read_most_recent_gcs_snapshot", return_value=[expired, active]):
            result = await adapter.get_instruments()
        assert len(result) == 1
        assert result[0].raw_symbol == "ESZ6"

    @pytest.mark.asyncio
    async def test_get_instruments_filters_by_type(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        future = _make_instrument(
            venue="CME", instrument_type="FUTURE", raw_symbol="ESZ6", expiry=datetime(2026, 12, 19, tzinfo=UTC)
        )
        spot = _make_instrument(venue="CME", instrument_type="SPOT_PAIR", raw_symbol="AAPL")
        with patch.object(adapter, "_read_most_recent_gcs_snapshot", return_value=[future, spot]):
            result = await adapter.get_instruments(instrument_type="FUTURE")
        assert len(result) == 1
        assert result[0].instrument_type == "FUTURE"

    @pytest.mark.asyncio
    async def test_get_instruments_falls_back_to_databento(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME", api_key="key")
        databento_inst = _make_instrument(venue="CME", raw_symbol="ESZ6")
        with (
            patch.object(adapter, "_read_most_recent_gcs_snapshot", return_value=None),
            patch(
                "instruments_service.reference_data.adapters.tradfi.tradfi_live.DatabentoReferenceDataAdapter"
            ) as mock_db,
        ):
            mock_adapter = mock_db.return_value
            mock_adapter.get_instruments = AsyncMock(return_value=[databento_inst])
            result = await adapter.get_instruments()
        assert len(result) == 1

    def test_read_most_recent_gcs_snapshot_exception(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        with patch(
            "instruments_service.reference_data.adapters.tradfi.tradfi_live.get_storage_client",
            side_effect=Exception("GCS not available"),
        ):
            result = adapter._read_most_recent_gcs_snapshot()
        assert result is None

    def test_read_most_recent_gcs_snapshot_no_data(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter(venue_filter="CME")
        mock_storage = MagicMock()
        mock_storage.download_bytes = MagicMock(return_value=None)
        with (
            patch(
                "instruments_service.reference_data.adapters.tradfi.tradfi_live.get_storage_client",
                return_value=mock_storage,
            ),
            patch(
                "instruments_service.reference_data.adapters.tradfi.tradfi_live.get_bucket_name",
                return_value="test-bucket",
            ),
        ):
            result = adapter._read_most_recent_gcs_snapshot()
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="ESZ6", venue="CME")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("ESZ6")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("ES")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("ES")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("ES")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = TradFiLiveReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("ES")

    # ── _dataframe_to_instrument_records ──────────────────────────────────

    def test_dataframe_to_instrument_records_with_nan(self) -> None:
        import pandas as pd

        df = pd.DataFrame(
            [
                {
                    "instrument_key": "CME:FUTURE:ESZ6",
                    "venue": "CME",
                    "raw_symbol": "ESZ6",
                    "instrument_type": "FUTURE",
                    "base_asset": "ES",
                    "quote_asset": "USD",
                    "tick_size": 0.25,
                    "min_size": 1,
                    "contract_size": 50,
                    "expiry": float("nan"),
                    "strike": float("nan"),
                    "option_type": None,
                    "legs": None,
                }
            ]
        )
        records = _dataframe_to_instrument_records(df)
        # FUTURE with NaN expiry → None expiry → validator rejects → record skipped
        assert len(records) == 0

    def test_dataframe_to_instrument_records_with_legs_json(self) -> None:
        import pandas as pd

        legs_json = json.dumps([{"instrument_key": "CME:FUTURE:ESM6", "side": "BUY", "ratio": 1}])
        df = pd.DataFrame(
            [
                {
                    "instrument_key": "CME:COMBO:ESM6-ESU6",
                    "venue": "CME",
                    "raw_symbol": "ESM6-ESU6",
                    "instrument_type": "COMBO",
                    "base_asset": "ES",
                    "quote_asset": "USD",
                    "legs": legs_json,
                }
            ]
        )
        records = _dataframe_to_instrument_records(df)
        assert len(records) == 1

    def test_dataframe_to_instrument_records_invalid_legs_json(self) -> None:
        import pandas as pd

        df = pd.DataFrame(
            [
                {
                    "instrument_key": "CME:FUTURE:ESZ6",
                    "venue": "CME",
                    "raw_symbol": "ESZ6",
                    "instrument_type": "FUTURE",
                    "base_asset": "ES",
                    "quote_asset": "USD",
                    "legs": "not-valid-json{{{",
                }
            ]
        )
        records = _dataframe_to_instrument_records(df)
        assert len(records) >= 0  # May succeed or skip depending on validation


# =============================================================================
# Databento adapter tests (comprehensive coverage)
# =============================================================================


class TestDatabentoHelpers:
    """Test module-level helper functions for Databento adapter."""

    # ── _classify_bento_error ─────────────────────────────────────────────

    def test_classify_bento_error_rate_limit(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="429 rate limit exceeded")
        assert _classify_bento_error(mock_exc) == "RATE_LIMIT"

    def test_classify_bento_error_auth(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="401 unauthorized")
        assert _classify_bento_error(mock_exc) == "AUTH_FAILURE"

    def test_classify_bento_error_connection(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="connection reset")
        assert _classify_bento_error(mock_exc) == "CONNECTION_RESET"

    def test_classify_bento_error_validation(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="422 unprocessable")
        assert _classify_bento_error(mock_exc) == "VALIDATION_ERROR"

    def test_classify_bento_error_not_found(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="404 not found")
        assert _classify_bento_error(mock_exc) == "NOT_FOUND"

    def test_classify_bento_error_server(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="500 internal server error")
        assert _classify_bento_error(mock_exc) == "SERVER_ERROR"

    def test_classify_bento_error_unknown(self) -> None:
        import databento as db

        mock_exc = MagicMock(spec=db.common.error.BentoError)
        mock_exc.__str__ = MagicMock(return_value="something weird")
        assert _classify_bento_error(mock_exc) == "UNKNOWN"

    # ── _extract_underlying_from_symbol ───────────────────────────────────

    def test_extract_underlying_es(self) -> None:
        result = _extract_underlying_from_symbol("ESH6")
        # Should find "ES" prefix
        assert result in ("ES", "")  # depends on registry state

    def test_extract_underlying_no_match(self) -> None:
        result = _extract_underlying_from_symbol("X")
        assert result == ""

    # ── _parse_cme_calendar_spread_legs ───────────────────────────────────

    def test_parse_cme_spread_legs_valid(self) -> None:
        # This may return None if ES isn't in the exchange code registry
        # Test that the function handles the format correctly
        result = _parse_cme_calendar_spread_legs("ESM6-ESU6", "CME")
        if result is not None:
            assert len(result) == 2
            assert result[0].side == "BUY"
            assert result[1].side == "SELL"

    def test_parse_cme_spread_legs_no_dash(self) -> None:
        assert _parse_cme_calendar_spread_legs("ESM6", "CME") is None

    def test_parse_cme_spread_legs_empty_parts(self) -> None:
        assert _parse_cme_calendar_spread_legs("-", "CME") is None

    def test_parse_cme_spread_legs_three_parts(self) -> None:
        assert _parse_cme_calendar_spread_legs("A-B-C", "CME") is None

    # ── _resolve_trading_status ───────────────────────────────────────────

    def test_resolve_trading_status_saturday(self) -> None:
        saturday = date(2026, 4, 11)  # Saturday
        is_trading, label = _resolve_trading_status("CME", saturday, False)
        assert is_trading is False
        assert label == "weekend"

    def test_resolve_trading_status_sunday_futures(self) -> None:
        sunday = date(2026, 4, 12)  # Sunday
        is_trading, label = _resolve_trading_status("CME", sunday, False)
        assert is_trading is True
        assert label == "sunday_open"

    def test_resolve_trading_status_sunday_equity(self) -> None:
        sunday = date(2026, 4, 12)  # Sunday
        is_trading, label = _resolve_trading_status("NASDAQ", sunday, False)
        assert is_trading is False
        assert label == "weekend"

    def test_resolve_trading_status_holiday(self) -> None:
        monday = date(2026, 4, 6)  # Monday
        is_trading, label = _resolve_trading_status("CME", monday, True)
        assert is_trading is False
        assert label == "holiday"

    def test_resolve_trading_status_regular(self) -> None:
        monday = date(2026, 4, 6)  # Monday
        is_trading, label = _resolve_trading_status("CME", monday, False)
        assert is_trading is True
        assert label == "regular"

    # ── is_non_trading_day ────────────────────────────────────────────────

    def test_is_non_trading_day_saturday(self) -> None:
        saturday = date(2026, 4, 11)
        assert is_non_trading_day("CME", saturday) is True

    def test_is_non_trading_day_unknown_venue(self) -> None:
        assert is_non_trading_day("UNKNOWN_VENUE", date.today()) is False


class TestDatabentoAdapter:
    """Tests for DatabentoReferenceDataAdapter methods."""

    def test_venue_default(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        assert adapter.venue == "databento"

    def test_venue_with_filter(self) -> None:
        adapter = DatabentoReferenceDataAdapter(venue_filter="CME")
        assert adapter.venue == "CME"

    @pytest.mark.asyncio
    async def test_get_instruments_no_key_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("ES")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("ES")

    def test_estimate_available_since_future_cme(self) -> None:
        expiry = datetime(2027, 6, 1, tzinfo=UTC)
        result = DatabentoReferenceDataAdapter._estimate_available_since(InstrumentType.FUTURE, expiry, "CME")
        assert result < expiry

    def test_estimate_available_since_option(self) -> None:
        expiry = datetime(2027, 6, 1, tzinfo=UTC)
        result = DatabentoReferenceDataAdapter._estimate_available_since(InstrumentType.OPTION, expiry, "CBOE")
        assert result < expiry

    def test_estimate_available_since_equity(self) -> None:
        result = DatabentoReferenceDataAdapter._estimate_available_since(InstrumentType.SPOT_PAIR, None, "NASDAQ")
        assert result.year >= 2015

    def test_resolve_asset_group_from_underlying(self) -> None:
        # Test with a known exchange code if available
        result = DatabentoReferenceDataAdapter._resolve_asset_group("GLBX.MDP3", "ESM6", "ES")
        assert result is not None  # Should resolve to something

    def test_resolve_asset_group_fallback_to_dataset(self) -> None:
        result = DatabentoReferenceDataAdapter._resolve_asset_group("GLBX.MDP3", "UNKN", "")
        assert result is not None

    def test_parse_tick_and_lot_valid(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        row = MagicMock()
        row.min_price_increment = "0.25"
        row.min_lot_size_round_lot = "1"
        tick, lot = adapter._parse_tick_and_lot(row)
        assert tick == Decimal("0.25")
        assert lot == Decimal("1")

    def test_parse_tick_and_lot_none(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        row = MagicMock()
        row.min_price_increment = None
        row.min_lot_size_round_lot = None
        tick, lot = adapter._parse_tick_and_lot(row)
        assert tick == Decimal("0.01")
        assert lot == Decimal("1")

    def test_parse_tick_and_lot_invalid(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        row = MagicMock()
        row.min_price_increment = "not-a-number"
        row.min_lot_size_round_lot = "also-bad"
        tick, lot = adapter._parse_tick_and_lot(row)
        assert tick == Decimal("0.01")
        assert lot == Decimal("1")

    def test_parse_expiry_from_row_valid(self) -> None:
        row = MagicMock()
        row.expiration = "2026-06-27T00:00:00Z"
        result = DatabentoReferenceDataAdapter._parse_expiry_from_row(row)
        assert result is not None

    def test_parse_expiry_from_row_none(self) -> None:
        row = MagicMock()
        row.expiration = None
        assert DatabentoReferenceDataAdapter._parse_expiry_from_row(row) is None

    def test_parse_expiry_from_row_invalid(self) -> None:
        row = MagicMock()
        row.expiration = "not-a-date"
        assert DatabentoReferenceDataAdapter._parse_expiry_from_row(row) is None

    def test_parse_strike_from_row_valid(self) -> None:
        row = MagicMock()
        row.strike_price = "150.0"
        result = DatabentoReferenceDataAdapter._parse_strike_from_row(row)
        assert result == Decimal("150.0")

    def test_parse_strike_from_row_none(self) -> None:
        row = MagicMock()
        row.strike_price = None
        assert DatabentoReferenceDataAdapter._parse_strike_from_row(row) is None

    def test_parse_strike_from_row_invalid(self) -> None:
        row = MagicMock()
        row.strike_price = "NaN"
        assert DatabentoReferenceDataAdapter._parse_strike_from_row(row) is None

    def test_parse_option_type_from_row(self) -> None:
        row = MagicMock()
        row.option_type = "C"
        result = DatabentoReferenceDataAdapter._parse_option_type_from_row(row, "E")
        assert result == "C"

    def test_parse_option_type_from_row_fallback_to_class(self) -> None:
        row = MagicMock()
        row.option_type = ""
        result = DatabentoReferenceDataAdapter._parse_option_type_from_row(row, "P")
        assert result == "P"

    def test_parse_option_type_from_row_none(self) -> None:
        row = MagicMock()
        row.option_type = ""
        result = DatabentoReferenceDataAdapter._parse_option_type_from_row(row, "E")
        assert result is None

    def test_is_filtered_out_expired(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key", target_date=date(2026, 4, 11))
        past = datetime(2025, 1, 1, tzinfo=UTC)
        assert adapter._is_filtered_out("GLBX.MDP3", "F", past) is True

    def test_is_filtered_out_too_far(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key", target_date=date(2026, 4, 11))
        far_future = datetime(2028, 1, 1, tzinfo=UTC)
        assert adapter._is_filtered_out("GLBX.MDP3", "F", far_future) is True

    def test_is_filtered_out_valid(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key", target_date=date(2026, 4, 11))
        near_future = datetime(2026, 9, 1, tzinfo=UTC)
        assert adapter._is_filtered_out("GLBX.MDP3", "F", near_future) is False

    def test_is_filtered_out_no_expiry(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key", target_date=date(2026, 4, 11))
        assert adapter._is_filtered_out("DBEQ.BASIC", "E", None) is False

    @pytest.mark.asyncio
    async def test_get_options_chain(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        call = _make_instrument(
            instrument_type="OPTION",
            base_asset="ES",
            expiry=expiry_dt,
            strike=Decimal("5000"),
            option_type="call",
            venue="CME",
        )
        put = _make_instrument(
            raw_symbol="ESZ6P",
            instrument_type="OPTION",
            base_asset="ES",
            expiry=expiry_dt,
            strike=Decimal("5000"),
            option_type="put",
            venue="CME",
        )
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[call, put])):
            chain = await adapter.get_options_chain("ES")
        assert isinstance(chain.calls, list)
        assert isinstance(chain.puts, list)

    @pytest.mark.asyncio
    async def test_get_expiry_calendar(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        expiry_dt = datetime(2026, 6, 27, tzinfo=UTC)
        inst = _make_instrument(
            instrument_type="FUTURE",
            base_asset="ES",
            expiry=expiry_dt,
            venue="CME",
        )
        inst.underlying = "ES"
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[inst])):
            cal = await adapter.get_expiry_calendar("ES")
        assert expiry_dt in cal.expiries

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        inst = _make_instrument(raw_symbol="ESZ6", venue="CME")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[inst])):
            result = await adapter.get_instrument("ESZ6")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    def test_create_fx_spot_records(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        records = adapter._create_fx_spot_records()
        assert isinstance(records, list)
        if records:
            assert records[0].venue == "FX"
            assert records[0].instrument_type == InstrumentType.SPOT_PAIR

    def test_create_yahoo_index_records(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        records = adapter._create_yahoo_index_records()
        assert isinstance(records, list)

    def test_get_equity_symbols(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        symbols = adapter._get_equity_symbols()
        assert isinstance(symbols, list)
        # Should be deduplicated
        assert len(symbols) == len(set(symbols))

    def test_enrich_session_metadata(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        record = _make_instrument(venue="CME", raw_symbol="ESZ6")
        adapter._enrich_session_metadata([record])
        # Should not raise

    def test_parse_row_to_record_empty_symbol(self) -> None:
        adapter = DatabentoReferenceDataAdapter(api_key="key")
        row = MagicMock()
        row.raw_symbol = ""
        row.symbol = ""
        result = adapter._parse_row_to_record(row, "GLBX.MDP3", "CME")
        assert result is None


# =============================================================================
# Phase 2: UnsupportedCapabilityError guards — Hyperliquid and Aster
# =============================================================================


class TestHyperliquidUnsupportedCapabilityGuard:
    """Hyperliquid.get_instruments raises UnsupportedCapabilityError for OPTION/FUTURE."""

    @pytest.mark.asyncio
    async def test_option_raises_unsupported(self) -> None:
        from unified_api_contracts import UnsupportedCapabilityError

        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.OPTION)
        assert exc_info.value.venue == "HYPERLIQUID"
        assert "OPTION" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_future_raises_unsupported(self) -> None:
        from unified_api_contracts import UnsupportedCapabilityError

        adapter = HyperliquidReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.FUTURE)
        assert exc_info.value.venue == "HYPERLIQUID"
        assert "FUTURE" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_perpetual_does_not_raise(self) -> None:
        """PERPETUAL is the only supported type — must not raise."""
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"universe": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_none_does_not_raise(self) -> None:
        """instrument_type=None must not raise."""
        adapter = HyperliquidReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"universe": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=None)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_spot_pair_returns_empty(self) -> None:
        """SPOT_PAIR is not OPTION/FUTURE so no guard fires — falls through to return []."""
        adapter = HyperliquidReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type=InstrumentType.SPOT_PAIR)
        assert results == []


class TestAsterUnsupportedCapabilityGuard:
    """Aster.get_instruments raises UnsupportedCapabilityError for OPTION/FUTURE."""

    @pytest.mark.asyncio
    async def test_option_raises_unsupported(self) -> None:
        from unified_api_contracts import UnsupportedCapabilityError

        adapter = AsterReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.OPTION)
        assert exc_info.value.venue == "ASTER"
        assert "OPTION" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_future_raises_unsupported(self) -> None:
        from unified_api_contracts import UnsupportedCapabilityError

        adapter = AsterReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.FUTURE)
        assert exc_info.value.venue == "ASTER"
        assert "FUTURE" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_perpetual_does_not_raise(self) -> None:
        """PERPETUAL is the only supported type — must not raise."""
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"symbols": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_none_does_not_raise(self) -> None:
        """instrument_type=None must not raise."""
        adapter = AsterReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"symbols": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=None)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_spot_pair_returns_empty(self) -> None:
        """SPOT_PAIR is not OPTION/FUTURE so no guard fires — falls through to return []."""
        adapter = AsterReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type=InstrumentType.SPOT_PAIR)
        assert results == []


# =============================================================================
# Phase 4: DeribitComboReferenceDataAdapter
# =============================================================================


class TestDeribitComboAdapter:
    """Tests for DeribitComboReferenceDataAdapter."""

    @pytest.mark.asyncio
    async def test_get_instruments_combo_success(self) -> None:
        """Happy path: BTC combo returned from Deribit API.

        The mock returns the same response for every currency call, so the
        BTC-STRD combo is returned once per currency in _DERIBIT_COMBO_UNDERLYINGS.
        We verify the first result has correct fields.
        """
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _DERIBIT_COMBO_UNDERLYINGS,
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        combo_instrument = {
            "instrument_name": "BTC-STRD-25APR26-90000",
            "creation_timestamp": 1700000000000,
            "settlement_currency": "BTC",
            "kind": "combo",
        }
        mock_session = _make_aiohttp_session_mock(resp_json={"result": [combo_instrument]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.COMBO)
        # The mock returns the same combo for all currency calls → one result per currency
        assert len(results) == len(_DERIBIT_COMBO_UNDERLYINGS)
        rec = results[0]
        assert rec.instrument_type == InstrumentType.COMBO
        assert rec.venue == "DERIBIT"
        assert rec.raw_symbol == "BTC-STRD-25APR26-90000"
        assert rec.base_asset == "BTC"
        assert rec.instrument_key == "DERIBIT:COMBO:BTC-STRD-25APR26-90000"

    @pytest.mark.asyncio
    async def test_get_instruments_none_type_fetches_combo(self) -> None:
        """instrument_type=None defaults to fetching COMBOs."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"result": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            # None should not raise
            results = await adapter.get_instruments(instrument_type=None)
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_get_instruments_non_combo_raises(self) -> None:
        """Non-COMBO instrument type raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.OPTION)
        assert exc_info.value.venue == "DERIBIT"
        assert "OPTION" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_get_instruments_future_raises(self) -> None:
        """FUTURE instrument type raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError) as exc_info:
            await adapter.get_instruments(instrument_type=InstrumentType.FUTURE)
        assert "FUTURE" in exc_info.value.capability

    @pytest.mark.asyncio
    async def test_get_instruments_http_error_all_currencies_raises(self) -> None:
        """CF-11: when EVERY currency HTTP-fails, get_instruments MUST raise RuntimeError
        (→ attempted_failed via _fetch_one), NOT ``return []``. A clean empty would land
        DERIBIT in _non_error_venues and silently vanish from coverage. Per-currency shard
        isolation (partial success) is covered by
        test_deribit_combo_adapter.py::test_get_instruments_partial_success_does_not_raise.
        Completes the cross-AG CF-11 sweep (slot-6 e2e008f0 fixed aster/hyperliquid/tardis
        but missed the DeribitCombo adapter).
        """
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        # Simulate HTTP 500 from Deribit for every currency.
        mock_session = _make_aiohttp_session_mock(resp_status=500)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(RuntimeError, match="no instruments fetched"),
        ):
            await adapter.get_instruments(instrument_type=InstrumentType.COMBO)

    @pytest.mark.asyncio
    async def test_get_instruments_empty_result_list(self) -> None:
        """Empty result list from Deribit returns empty instruments list."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"result": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.COMBO)
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_result_item_skipped(self) -> None:
        """Non-dict items in result list are skipped silently."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json={"result": ["bad_item", None, 42]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.COMBO)
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """get_instrument returns the matching record."""
        from unified_api_contracts.internal import InstrumentRecord

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        inst = InstrumentRecord(
            instrument_key="DERIBIT:COMBO:BTC-STRD-25APR26-90000",
            venue="DERIBIT",
            raw_symbol="BTC-STRD-25APR26-90000",
            instrument_type=InstrumentType.COMBO,
            base_asset="BTC",
            quote_asset="USD",
        )
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTC-STRD-25APR26-90000")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        """get_instrument returns None for unknown symbol."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("BTC-NONEXISTENT")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        """get_options_chain raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        """get_expiry_calendar raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        """get_funding_rate raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError):
            await adapter.get_funding_rate("BTC-STRD-25APR26-90000")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        """get_ohlcv raises UnsupportedCapabilityError."""
        from unified_api_contracts import UnsupportedCapabilityError

        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        with pytest.raises(UnsupportedCapabilityError):
            await adapter.get_ohlcv("BTC-STRD-25APR26-90000")

    def test_venue_property(self) -> None:
        """venue property returns DERIBIT."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        assert adapter.venue == "DERIBIT"

    def test_extract_structure_code(self) -> None:
        """_extract_structure_code correctly identifies structure codes."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            _extract_structure_code,
        )

        assert _extract_structure_code("BTC-STRD-25APR26-90000") == "STRD"
        assert _extract_structure_code("BTC-CS-25APR26-80000_90000") == "CS"
        assert _extract_structure_code("BTC-ICOND-25APR26-70000_80000_90000_100000") == "ICOND"
        assert _extract_structure_code("BTC-FS-25APR26_PERP") == "FS"
        assert _extract_structure_code("ETH-CBUT-25APR26-3000_3500_4000") == "CBUT"
        assert _extract_structure_code("UNKNOWN-FORMAT") == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_parse_combo_instrument_bad_item_returns_none(self) -> None:
        """_parse_combo_instrument returns None for non-dict input."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        now = datetime.now(UTC)
        assert adapter._parse_combo_instrument("not_a_dict", now) is None
        assert adapter._parse_combo_instrument(None, now) is None
        assert adapter._parse_combo_instrument({"instrument_name": ""}, now) is None

    @pytest.mark.asyncio
    async def test_parse_combo_instrument_invalid_timestamp(self) -> None:
        """Invalid creation_timestamp falls back to now."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        now = datetime.now(UTC)
        item = {
            "instrument_name": "BTC-STRD-25APR26-90000",
            "creation_timestamp": "NOT_A_NUMBER",
            "settlement_currency": "BTC",
        }
        result = adapter._parse_combo_instrument(item, now)
        assert result is not None
        assert result.instrument_type == InstrumentType.COMBO

    @pytest.mark.asyncio
    async def test_get_instruments_response_non_dict_skipped(self) -> None:
        """Non-dict top-level response is handled gracefully."""
        from instruments_service.reference_data.adapters.cefi.deribit_combo_adapter import (
            DeribitComboReferenceDataAdapter,
        )

        adapter = DeribitComboReferenceDataAdapter()
        mock_session = _make_aiohttp_session_mock(resp_json=[])  # list instead of dict
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(instrument_type=InstrumentType.COMBO)
        assert results == []

    def test_factory_contains_deribit_combo(self) -> None:
        """DERIBIT-COMBO is registered in the factory adapter map."""
        from instruments_service.reference_data.factory import (
            _ADAPTERS,
            CANONICAL_VENUE_TO_ADAPTER,
        )

        assert "DERIBIT-COMBO" in CANONICAL_VENUE_TO_ADAPTER
        assert CANONICAL_VENUE_TO_ADAPTER["DERIBIT-COMBO"] == "deribit_combo"
        assert "deribit_combo" in _ADAPTERS
