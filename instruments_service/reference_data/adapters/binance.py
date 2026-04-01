"""Binance reference data adapter — REST only, no WebSocket."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import (
    BinanceFuturesExchangeInfo,
    BinanceInstrumentInfo,
    BinanceOptionInstrumentInfo,
    classify_venue_error,
)
from unified_api_contracts.internal import (
    InstrumentRecord,
    InstrumentStatus,
    InstrumentType,
    MarginType,
    OptionType,
)
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_FAPI_BASE = "https://fapi.binance.com"
_EAPI_BASE = "https://eapi.binance.com"
_SPOT_BASE = "https://api.binance.com"


def _classify_binance_error(exc: Exception, status: int | None = None) -> str:
    """Map a Binance HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    # Binance uses numeric error codes in response body: -1003, -1021, etc.
    if "-1003" in msg or "429" in str(status or "") or "rate" in msg or "too many" in msg:
        return "-1003"
    if "-1021" in msg or "timestamp" in msg:
        return "-1021"
    if "-1006" in msg or "connection" in msg:
        return "-1006"
    if "-2011" in msg or "unknown order" in msg:
        return "-2011"
    if "-1013" in msg or "lot size" in msg:
        return "-1013"
    if "-1000" in msg or (status is not None and status >= 500):
        return "-1000"
    return "UNKNOWN"


_BINANCE_SPOT_VENUE = "BINANCE-SPOT"
_BINANCE_FUTURES_VENUE = "BINANCE-FUTURES"


class BinanceReferenceDataAdapter(BaseReferenceDataAdapter):
    """Binance reference data: spot, USDT-M futures, coin-M futures, options.

    Canonical venue assignment per instrument type:
      SPOT            → BINANCE-SPOT
      PERP / FUTURE   → BINANCE-FUTURES
      OPTION          → BINANCE-FUTURES (options are traded on the derivatives platform)
    """

    @property
    def venue(self) -> str:
        return _BINANCE_SPOT_VENUE  # default for the property (spot context)

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        results: list[InstrumentRecord] = []
        async with aiohttp.ClientSession() as session:
            if instrument_type in (None, "SPOT_PAIR", InstrumentType.SPOT_PAIR):
                results.extend(await self._fetch_spot(session))
            if instrument_type in (None, "PERPETUAL", "FUTURE", InstrumentType.PERPETUAL, InstrumentType.FUTURE):
                results.extend(await self._fetch_futures(session))
            if instrument_type in (None, "OPTION", InstrumentType.OPTION):
                results.extend(await self._fetch_options(session))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        async with aiohttp.ClientSession() as session:
            url = f"{_EAPI_BASE}/eapi/v1/exchangeInfo"
            async with session.get(url) as resp:
                data = BinanceFuturesExchangeInfo.model_validate(await resp.json())
        now = datetime.now(UTC)
        symbols: list[BinanceOptionInstrumentInfo] = data.optionSymbols or []
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for sym in symbols:
            if sym.underlying != underlying:
                continue
            if sym.quoteAsset != "USDT":
                continue
            inst = self._parse_option_symbol(sym)
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            strikes.add(inst.strike or Decimal(0))
            if inst.option_type == OptionType.CALL:
                calls.append(inst)
            else:
                puts.append(inst)
        target_expiry = expiry or (calls[0].expiry if calls else now)
        return CanonicalOptionsChain(
            venue=self.venue,
            underlying=underlying,
            expiry=target_expiry or now,
            strikes=sorted(strikes),
            calls=calls,
            puts=puts,
            fetched_at=now,
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        async with aiohttp.ClientSession() as session:
            url = f"{_FAPI_BASE}/fapi/v1/exchangeInfo"
            async with session.get(url) as resp:
                data = BinanceFuturesExchangeInfo.model_validate(await resp.json())
        expiries: list[datetime] = []
        symbols: list[BinanceInstrumentInfo] = data.symbols or []
        for sym in symbols:
            if sym.baseAsset != underlying.upper():
                continue
            contract_type = sym.contractType or ""
            if contract_type not in ("CURRENT_QUARTER", "NEXT_QUARTER", "PERPETUAL"):
                continue
            delivery_ts = sym.deliveryDate
            if delivery_ts and int(str(delivery_ts)) > 0:
                expiries.append(datetime.fromtimestamp(int(str(delivery_ts)) / 1000, tz=UTC))
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(set(expiries)),
            updated_at=datetime.now(UTC),
        )

    async def _fetch_spot(self, session: aiohttp.ClientSession) -> list[InstrumentRecord]:
        url = f"{_SPOT_BASE}/api/v3/exchangeInfo"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = BinanceFuturesExchangeInfo.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_binance_error(exc)
            classification = classify_venue_error("binance", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Binance spot exchangeInfo failed: %s (classified: %s, action: %s)",
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "binance",
                    "endpoint": "spot/exchangeInfo",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        results: list[InstrumentRecord] = []
        for sym in data.symbols or []:
            if sym.status != "TRADING":
                continue
            filters: list[dict[str, object]] = sym.filters or []
            tick = self._filter_size(filters, "PRICE_FILTER", "tickSize")
            lot = self._filter_size(filters, "LOT_SIZE", "stepSize")
            base = sym.baseAsset or ""
            quote = sym.quoteAsset or ""
            results.append(
                InstrumentRecord(
                    instrument_key=f"{_BINANCE_SPOT_VENUE}:SPOT_PAIR:{base}~{quote}",
                    venue=_BINANCE_SPOT_VENUE,
                    raw_symbol=sym.symbol,
                    instrument_type=InstrumentType.SPOT_PAIR,
                    base_asset=base,
                    quote_asset=quote,
                    status=InstrumentStatus.ACTIVE,
                    tick_size=Decimal(tick),
                    min_size=Decimal(lot),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
                )
            )
        return results

    async def _fetch_futures(self, session: aiohttp.ClientSession) -> list[InstrumentRecord]:
        url = f"{_FAPI_BASE}/fapi/v1/exchangeInfo"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = BinanceFuturesExchangeInfo.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_binance_error(exc)
            classification = classify_venue_error("binance", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Binance futures exchangeInfo failed: %s (classified: %s, action: %s)",
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "binance",
                    "endpoint": "futures/exchangeInfo",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        results: list[InstrumentRecord] = []
        for sym in data.symbols or []:
            if sym.status != "TRADING":
                continue
            contract_type = sym.contractType or ""
            inst_type = InstrumentType.PERPETUAL if contract_type == "PERPETUAL" else InstrumentType.FUTURE
            # Determine margin type from contract type
            margin_asset = (sym.marginAsset or "").upper() if hasattr(sym, "marginAsset") else ""
            margin_type = MarginType.INVERSE if margin_asset in ("BTC", "ETH") else MarginType.LINEAR
            filters: list[dict[str, object]] = sym.filters or []
            tick = self._filter_size(filters, "PRICE_FILTER", "tickSize")
            lot = self._filter_size(filters, "LOT_SIZE", "stepSize")
            delivery_ts = sym.deliveryDate
            expiry: datetime | None = None
            if delivery_ts and int(str(delivery_ts)) > 0:
                expiry = datetime.fromtimestamp(int(str(delivery_ts)) / 1000, tz=UTC)
            base_asset = sym.baseAsset or ""
            quote_asset = sym.quoteAsset or ""
            inst_type_key = inst_type.value
            results.append(
                InstrumentRecord(
                    instrument_key=f"{_BINANCE_FUTURES_VENUE}:{inst_type_key}:{base_asset}~{quote_asset}",
                    venue=_BINANCE_FUTURES_VENUE,
                    raw_symbol=sym.symbol,
                    instrument_type=inst_type,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    status=InstrumentStatus.ACTIVE,
                    tick_size=Decimal(tick),
                    min_size=Decimal(lot),
                    contract_size=Decimal(str(sym.contractSize or 1)),
                    expiry=expiry,
                    strike=None,
                    option_type=None,
                    margin_type=margin_type,
                    available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
                )
            )
        return results

    async def _fetch_options(self, session: aiohttp.ClientSession) -> list[InstrumentRecord]:
        url = f"{_EAPI_BASE}/eapi/v1/exchangeInfo"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = BinanceFuturesExchangeInfo.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_binance_error(exc)
            classification = classify_venue_error("binance", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Binance options exchangeInfo failed: %s (classified: %s, action: %s)",
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "binance",
                    "endpoint": "options/exchangeInfo",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        symbols: list[BinanceOptionInstrumentInfo] = data.optionSymbols or []
        return [self._parse_option_symbol(s) for s in symbols if s.expiryDate]

    def _parse_option_symbol(self, sym: BinanceOptionInstrumentInfo) -> InstrumentRecord:
        expiry_ts = sym.expiryDate
        expiry: datetime | None = None
        if expiry_ts:
            expiry = datetime.fromtimestamp(int(str(expiry_ts)) / 1000, tz=UTC)
        symbol_val = sym.symbol or ""
        underlying = sym.underlying or ""
        side = (sym.side or "").upper()
        opt_type = OptionType.CALL if side == "CALL" else OptionType.PUT
        return InstrumentRecord(
            instrument_key=f"{_BINANCE_FUTURES_VENUE}:OPTION:{symbol_val}",
            venue=_BINANCE_FUTURES_VENUE,
            raw_symbol=symbol_val,
            instrument_type=InstrumentType.OPTION,
            base_asset=underlying,
            quote_asset="USDT",
            status=InstrumentStatus.ACTIVE,
            tick_size=Decimal(str(sym.priceScale or "0.01")),
            min_size=Decimal(str(sym.quantityScale or "0.001")),
            contract_size=Decimal("1"),
            expiry=expiry,
            strike=Decimal(str(sym.strikePrice or "0")),
            option_type=opt_type,
            available_from_datetime=datetime(2017, 7, 14, tzinfo=UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch current funding rate from Binance USDT-M futures (public, no auth required).

        Uses GET /fapi/v1/premiumIndex which returns markPrice and nextFundingTime.
        """
        url = f"{_FAPI_BASE}/fapi/v1/premiumIndex"
        now = datetime.now(UTC)
        params_fr = {"symbol": symbol}
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params_fr) as resp,
            ):
                resp.raise_for_status()
                raw_data: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_binance_error(exc)
            classification = classify_venue_error("binance", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Binance premiumIndex failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "binance",
                    "endpoint": "premiumIndex",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return FundingRateRef(
                venue=self.venue,
                symbol=symbol,
                rate=Decimal("0"),
                next_funding_time=now,
                mark_price=None,
                updated_at=now,
            )
        data: dict[str, object] = raw_data if isinstance(raw_data, dict) else {}
        next_funding_time_ms = int(str(data.get("nextFundingTime", 0)))
        next_funding_time = datetime.fromtimestamp(next_funding_time_ms / 1000, tz=UTC) if next_funding_time_ms else now
        rate_raw = data.get("lastFundingRate", "0") or "0"
        mark_price_raw = data.get("markPrice")
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=Decimal(str(mark_price_raw)) if mark_price_raw else None,
            updated_at=now,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Binance USDT-M futures klines endpoint (public, no auth).

        Uses GET /fapi/v1/klines. Each bar: [openTime, open, high, low, close, volume, ...].
        """
        url = f"{_FAPI_BASE}/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_binance_error(exc)
            classification = classify_venue_error("binance", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Binance klines failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "binance",
                    "endpoint": "klines",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        raw: list[object] = raw_json if isinstance(raw_json, list) else []
        results: list[OHLCVRef] = []
        for bar_raw in raw:
            bar = bar_raw if isinstance(bar_raw, list) else []
            open_time_ms = int(str(bar[0]))
            ts = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
            results.append(
                OHLCVRef(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(bar[1])),
                    high=Decimal(str(bar[2])),
                    low=Decimal(str(bar[3])),
                    close=Decimal(str(bar[4])),
                    volume=Decimal(str(bar[5])),
                    interval=interval,
                )
            )
        return results

    @staticmethod
    def _filter_size(filters: list[dict[str, object]], filter_type: str, field_name: str) -> str:
        for f in filters:
            if f.get("filterType") == filter_type:
                return str(f.get(field_name, "0.001"))
        return "0.001"
