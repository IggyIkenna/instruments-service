"""Bybit reference data adapter — REST only, no WebSocket."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    BybitInstrumentInfo,
    BybitInstrumentsResponse,
    classify_venue_error,
)
from unified_api_contracts.internal import FeeScheduleEntry, InstrumentRecord, MarginType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import CanonicalExpiryCalendar, CanonicalOptionsChain, FundingRateRef, OHLCVRef

logger = logging.getLogger(__name__)

_BASE = "https://api.bybit.com"
_BYBIT_SPOT_VENUE = "BYBIT-SPOT"
_BYBIT_FUTURES_VENUE = "BYBIT-FUTURES"


def _classify_bybit_error(exc: Exception, status: int | None = None) -> str:
    """Map a Bybit HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if "10006" in msg or "429" in str(status or "") or "rate" in msg:
        return "10006"
    if "10000" in msg or (status is not None and status >= 500):
        return "10000"
    if "10001" in msg or "parameter" in msg:
        return "10001"
    if "10019" in msg:
        return "10019"
    if "33004" in msg or "api key" in msg:
        return "33004"
    return "UNKNOWN"


class BybitReferenceDataAdapter(BaseReferenceDataAdapter):
    """Bybit reference data: spot, linear perp/futures, options."""

    @property
    def venue(self) -> str:
        return _BYBIT_SPOT_VENUE  # default for the property (spot context)

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        results: list[InstrumentRecord] = []
        async with aiohttp.ClientSession() as session:
            if instrument_type in (None, "spot"):
                results.extend(await self._fetch_category(session, "spot"))
            if instrument_type in (None, "perp", "future"):
                results.extend(await self._fetch_linear_inverse(session))
            if instrument_type in (None, "option"):
                results.extend(await self._fetch_options(session))
        return results

    async def _fetch_linear_inverse(self, session: aiohttp.ClientSession) -> list[InstrumentRecord]:
        """Fetch linear and inverse derivatives, deduplicating by instrument_key."""
        seen: set[str] = set()
        results: list[InstrumentRecord] = []
        for category in ("inverse", "linear"):
            for record in await self._fetch_category(session, category):
                if record.instrument_key not in seen:
                    seen.add(record.instrument_key)
                    results.append(record)
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
        instruments = await self.get_instruments(instrument_type="option")
        calls: list[InstrumentRecord] = []
        puts: list[InstrumentRecord] = []
        strikes: set[Decimal] = set()
        for inst in instruments:
            if inst.base_asset != underlying.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            if inst.option_type == "call":
                calls.append(inst)
            else:
                puts.append(inst)
        now = datetime.now(UTC)
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
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        instruments = await self.get_instruments(instrument_type=instrument_type)
        expiry_set: set[datetime] = set()
        for inst in instruments:
            if inst.base_asset == underlying.upper() and inst.expiry:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Bybit v5 funding history endpoint (public).

        Uses GET /v5/market/funding/history with limit=1 to get the most recent rate.
        """
        url = f"{_BASE}/v5/market/funding/history"
        params = {"category": "linear", "symbol": symbol, "limit": "1"}
        now = datetime.now(UTC)
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params) as resp,
            ):
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_bybit_error(exc)
            classification = classify_venue_error("bybit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Bybit funding/history failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "bybit",
                    "endpoint": "funding/history",
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
        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        result_obj: object = raw_dict.get("result")
        result: dict[str, object] = result_obj if isinstance(result_obj, dict) else {}
        list_obj: object = result.get("list")
        list_raw: list[object] = list_obj if isinstance(list_obj, list) else []
        entry_obj: object = list_raw[0] if list_raw else None
        entry: dict[str, object] = entry_obj if isinstance(entry_obj, dict) else {}
        rate_raw = entry.get("fundingRate", "0") or "0"
        funding_ts_raw = entry.get("fundingRateTimestamp", "0") or "0"
        funding_ts = int(str(funding_ts_raw))
        next_funding_time = datetime.fromtimestamp(funding_ts / 1000, tz=UTC) if funding_ts else now
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    _MARGIN_TYPE_MAP: ClassVar[dict[str, MarginType]] = {
        "LinearPerpetual": MarginType.LINEAR,
        "LinearFutures": MarginType.LINEAR,
        "InversePerpetual": MarginType.INVERSE,
        "InverseFutures": MarginType.INVERSE,
    }

    _BYBIT_INTERVAL_MAP: ClassVar[dict[str, str]] = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "4h": "240",
        "6h": "360",
        "12h": "720",
        "1d": "D",
        "1w": "W",
        "1M": "M",
    }

    def _parse_bybit_bar(self, bar: object, symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a single Bybit kline array into OHLCVRef. Returns None if invalid."""
        if not isinstance(bar, list) or len(bar) < 6:
            return None
        bar_list: list[object] = bar
        ts = datetime.fromtimestamp(int(str(bar_list[0])) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(bar_list[1])),
            high=Decimal(str(bar_list[2])),
            low=Decimal(str(bar_list[3])),
            close=Decimal(str(bar_list[4])),
            volume=Decimal(str(bar_list[5])),
            interval=interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Bybit v5 kline endpoint (public).

        Uses GET /v5/market/kline. Bars returned newest-first; reversed to oldest-first.
        """
        bybit_interval = self._BYBIT_INTERVAL_MAP.get(interval, "D")
        url = f"{_BASE}/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": bybit_interval,
            "limit": str(limit),
        }
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(url, params=params) as resp,
            ):
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_bybit_error(exc)
            classification = classify_venue_error("bybit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Bybit kline failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "bybit",
                    "endpoint": "kline",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        result_obj: object = raw_dict.get("result")
        result: dict[str, object] = result_obj if isinstance(result_obj, dict) else {}
        list_obj: object = result.get("list")
        list_raw: list[object] = list_obj if isinstance(list_obj, list) else []
        return [ref for bar in reversed(list_raw) if (ref := self._parse_bybit_bar(bar, symbol, interval)) is not None]

    async def _fetch_category(self, session: aiohttp.ClientSession, category: str) -> list[InstrumentRecord]:
        url = f"{_BASE}/v5/market/instruments-info"
        params = {"category": category, "limit": "1000"}
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = BybitInstrumentsResponse.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_bybit_error(exc)
            classification = classify_venue_error("bybit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Bybit instruments-info failed for category %s: %s (classified: %s, action: %s)",
                category,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "bybit",
                    "endpoint": "instruments-info",
                    "category": category,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        symbol_list: list[BybitInstrumentInfo] = data.result.instruments
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        for sym in symbol_list:
            if sym.status not in ("Trading", "PreLaunch"):
                continue
            record = self._parse_category_symbol(sym, category, now)
            if record is not None:
                results.append(record)
        return results

    def _parse_category_symbol(self, sym: BybitInstrumentInfo, category: str, now: datetime) -> InstrumentRecord | None:
        if not sym.symbol:
            return None
        contract_type = sym.contractType or ""
        if category == "spot":
            inst_type = "spot"
            margin_type: MarginType | None = None
        elif contract_type in ("LinearPerpetual", "InversePerpetual"):
            inst_type = "perp"
            margin_type = self._MARGIN_TYPE_MAP.get(contract_type)
        else:
            inst_type = "future"
            margin_type = self._MARGIN_TYPE_MAP.get(contract_type)
        expiry = self._parse_delivery_time(sym.deliveryTime)
        settle_coin = sym.settleCoin
        base_coin = sym.baseCoin or ""
        quote_coin = sym.quoteCoin or ""
        tick_size, lot_size, min_order = self._parse_bybit_filters(sym.priceFilter, sym.lotSizeFilter)
        canonical = _BYBIT_SPOT_VENUE if inst_type == "spot" else _BYBIT_FUTURES_VENUE
        return InstrumentRecord(
            instrument_key=f"{canonical}:{inst_type.upper()}:{base_coin}~{quote_coin}",
            venue=canonical,
            symbol=f"{base_coin}/{quote_coin}",
            raw_symbol=sym.symbol,
            instrument_type=inst_type,
            base_asset=base_coin,
            quote_asset=quote_coin,
            tick_size=tick_size,
            lot_size=lot_size,
            min_order_size=min_order,
            contract_size=Decimal("1"),
            settlement_asset=str(settle_coin) if settle_coin else None,
            expiry=expiry,
            strike=None,
            option_type=None,
            margin_type=margin_type,
            is_active=True,
            updated_at=now,
        )

    def _parse_delivery_time(self, expiry_ts: object) -> datetime | None:
        """Convert Bybit delivery timestamp (ms) to UTC datetime."""
        if expiry_ts and str(expiry_ts) != "0":
            return datetime.fromtimestamp(int(str(expiry_ts)) / 1000, tz=UTC)
        return None

    def _parse_bybit_filters(
        self,
        price_filter: dict[str, object] | None,
        lot_filter: dict[str, object] | None,
    ) -> tuple[Decimal, Decimal, Decimal]:
        """Extract tick_size, lot_size, min_order_size from Bybit filter dicts."""
        tick_size = Decimal(
            str(price_filter.get("tickSize", "0.01")) if price_filter and price_filter.get("tickSize") else "0.01"
        )
        lot_size = Decimal(
            str(lot_filter.get("qtyStep", "0.01")) if lot_filter and lot_filter.get("qtyStep") else "0.001"
        )
        min_order = Decimal(
            str(lot_filter.get("minOrderQty", "0.01")) if lot_filter and lot_filter.get("minOrderQty") else "0.001"
        )
        return tick_size, lot_size, min_order

    async def _fetch_options(self, session: aiohttp.ClientSession) -> list[InstrumentRecord]:
        underlyings = ["BTC", "ETH", "SOL"]
        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        for base in underlyings:
            url = f"{_BASE}/v5/market/instruments-info"
            params = {"category": "option", "baseCoin": base, "limit": "1000"}
            try:
                async with session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    data = BybitInstrumentsResponse.model_validate(await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_bybit_error(exc)
                classification = classify_venue_error("bybit", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Bybit options instruments-info failed for %s: %s (classified: %s, action: %s)",
                    base,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "bybit",
                        "endpoint": "options/instruments-info",
                        "base": base,
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                continue
            symbol_list: list[BybitInstrumentInfo] = data.result.instruments
            for sym in symbol_list:
                record = self._parse_option_symbol(sym, base, now)
                if record is not None:
                    results.append(record)
        return results

    def _parse_option_symbol(self, sym: BybitInstrumentInfo, base: str, now: datetime) -> InstrumentRecord | None:
        if not sym.symbol:
            return None
        price_filter: dict[str, object] | None = sym.priceFilter
        lot_filter: dict[str, object] | None = sym.lotSizeFilter
        expiry_ts = sym.deliveryTime
        expiry: datetime | None = None
        if expiry_ts and str(expiry_ts) != "0":
            expiry = datetime.fromtimestamp(int(str(expiry_ts)) / 1000, tz=UTC)
        opt_type_raw = (sym.optionsType or "").lower()
        opt_type = "call" if opt_type_raw == "call" else "put"
        strike_raw = sym.strikePrice
        return InstrumentRecord(
            instrument_key=f"{_BYBIT_FUTURES_VENUE}:OPTION:{sym.symbol}",
            venue=_BYBIT_FUTURES_VENUE,
            symbol=sym.symbol,
            raw_symbol=sym.symbol,
            instrument_type="option",
            base_asset=base,
            quote_asset="USDC",
            tick_size=Decimal(
                str(price_filter.get("tickSize", "0.01")) if price_filter and price_filter.get("tickSize") else "0.01"
            ),
            lot_size=Decimal(
                str(lot_filter.get("qtyStep", "0.01")) if lot_filter and lot_filter.get("qtyStep") else "0.01"
            ),
            min_order_size=Decimal(
                str(lot_filter.get("minOrderQty", "0.01")) if lot_filter and lot_filter.get("minOrderQty") else "0.01"
            ),
            contract_size=Decimal("1"),
            settlement_asset="USDC",
            expiry=expiry,
            strike=Decimal(str(strike_raw)) if strike_raw else None,
            option_type=opt_type,
            is_active=True,
            updated_at=now,
        )

    async def get_exchange_fee_schedule(
        self,
        client_id: str,
    ) -> FeeScheduleEntry | None:
        """Fetch Bybit account fee tier via GET /v5/account/fee-rate.

        Requires signed authentication (API key + HMAC-SHA256 signature).
        Stub until Bybit authenticated client is wired — logs WARNING and
        returns None so callers fall back to default fee schedule.
        """
        logger.warning(
            "BybitReferenceDataAdapter.get_exchange_fee_schedule: "
            "authenticated fee-rate fetch not yet implemented for client_id=%s; "
            "endpoint: GET /v5/account/fee-rate (requires signed request). "
            "Returning None — caller should use default FeeScheduleEntry.",
            client_id,
        )
        return None
