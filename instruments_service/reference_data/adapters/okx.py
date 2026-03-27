"""OKX reference data adapter — REST only, no WebSocket."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    OKXInstrumentInfo,
    OKXInstrumentsResponse,
    classify_venue_error,
)
from unified_api_contracts.internal import FeeScheduleEntry, InstrumentRecord, MarginType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.okx.com"
_OKX_SPOT_VENUE = "OKX-SPOT"
_OKX_FUTURES_VENUE = "OKX-FUTURES"

_INST_TYPE_MAP = {
    "spot": "SPOT",
    "perp": "SWAP",
    "future": "FUTURES",
    "option": "OPTION",
}


def _classify_okx_error(exc: Exception, status: int | None = None) -> str:
    """Map an OKX HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if "50011" in msg or "429" in str(status or "") or "rate" in msg:
        return "50011"
    if "50004" in msg or "websocket" in msg:
        return "50004"
    if "50026" in msg or "busy" in msg:
        return "50026"
    if "51008" in msg or "insufficient" in msg:
        return "51008"
    if status is not None and status >= 500:
        return "50026"
    return "UNKNOWN"


class OKXReferenceDataAdapter(BaseReferenceDataAdapter):
    """OKX reference data: spot, swap (perp), futures, options."""

    @property
    def venue(self) -> str:
        return _OKX_SPOT_VENUE  # default for the property (spot context)

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        results: list[InstrumentRecord] = []
        inst_types = list(_INST_TYPE_MAP.keys()) if instrument_type is None else [instrument_type]

        async with aiohttp.ClientSession() as session:
            for itype in inst_types:
                okx_type = _INST_TYPE_MAP.get(itype, "SPOT")
                if itype == "option":
                    for uly in ["BTC-USD", "ETH-USD", "SOL-USD"]:
                        results.extend(await self._fetch_instruments(session, okx_type, uly=uly))
                else:
                    results.extend(await self._fetch_instruments(session, okx_type))
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
            if not inst.raw_symbol.startswith(underlying.upper() + "-USD"):
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            if inst.option_type == "C":
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
        """Fetch latest funding rate from OKX public endpoint.

        Uses GET /api/v5/public/funding-rate-history with limit=1.
        OKX instId for USDT perpetuals: BTC-USDT-SWAP format.
        """
        url = f"{_BASE}/api/v5/public/funding-rate-history"
        params = {"instId": symbol, "limit": "1"}
        now = datetime.now(UTC)
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_okx_error(exc)
            classification = classify_venue_error("okx", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "OKX funding-rate-history failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "okx",
                    "endpoint": "funding-rate-history",
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
        data_obj: object = raw_dict.get("data")
        data_list: list[object] = data_obj if isinstance(data_obj, list) else []
        entry_obj: object = data_list[0] if data_list else {}
        entry: dict[str, object] = entry_obj if isinstance(entry_obj, dict) else {}
        rate_raw = entry.get("fundingRate", "0") or "0"
        next_ts_raw = entry.get("nextFundingTime", "0") or "0"
        next_ts = int(str(next_ts_raw))
        next_funding_time = datetime.fromtimestamp(next_ts / 1000, tz=UTC) if next_ts else now
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    _OKX_BAR_MAP: ClassVar[dict[str, str]] = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "1w": "1W",
        "1M": "1M",
    }

    def _parse_okx_candle_row(self, row_raw: object, symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a single OKX candle array into OHLCVRef. Returns None if invalid."""
        if not isinstance(row_raw, list) or len(row_raw) < 6:
            return None
        row: list[object] = row_raw
        ts = datetime.fromtimestamp(int(str(row[0])) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
            interval=interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from OKX historical candles endpoint (public).

        Uses GET /api/v5/market/history-candles.
        OKX bar strings: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M.
        """
        bar = self._OKX_BAR_MAP.get(interval, "1D")
        url = f"{_BASE}/api/v5/market/history-candles"
        params = {"instId": symbol, "bar": bar, "limit": str(limit)}
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_okx_error(exc)
            classification = classify_venue_error("okx", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "OKX history-candles failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "okx",
                    "endpoint": "history-candles",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        data_obj: object = raw_dict.get("data")
        data_list: list[object] = data_obj if isinstance(data_obj, list) else []
        return [
            bar_ref
            for row_raw in reversed(data_list)
            if (bar_ref := self._parse_okx_candle_row(row_raw, symbol, interval)) is not None
        ]

    async def _fetch_instruments(
        self,
        session: aiohttp.ClientSession,
        inst_type: str,
        uly: str | None = None,
    ) -> list[InstrumentRecord]:
        url = f"{_BASE}/api/v5/public/instruments"
        params: dict[str, str] = {"instType": inst_type}
        if uly:
            params["uly"] = uly
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = OKXInstrumentsResponse.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_okx_error(exc)
            classification = classify_venue_error("okx", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "OKX instruments failed for instType=%s: %s (classified: %s, action: %s)",
                inst_type,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "okx",
                    "endpoint": "instruments",
                    "inst_type": inst_type,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        items: list[OKXInstrumentInfo] = data.data
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        for sym in items:
            if sym.state != "live":
                continue
            record = self._parse_okx_instrument(sym, now)
            if record is not None:
                results.append(record)
        return results

    def _parse_okx_instrument(self, sym: OKXInstrumentInfo, now: datetime) -> InstrumentRecord | None:
        if not sym.instId:
            return None
        if sym.instType == "SPOT":
            mapped_type = "spot"
        elif sym.instType == "SWAP":
            mapped_type = "perp"
        elif sym.instType == "FUTURES":
            mapped_type = "future"
        else:
            mapped_type = "option"
        exp_time = sym.expTime
        expiry: datetime | None = None
        if exp_time and str(exp_time) != "":
            expiry = datetime.fromtimestamp(int(str(exp_time)) / 1000, tz=UTC)
        settle_ccy = sym.settleCcy
        opt_type = str(sym.optType).lower() if sym.optType else None
        strike_val = sym.stk
        base_ccy = sym.baseCcy or ""
        quote_ccy = sym.quoteCcy or ""
        margin_type = self._get_margin_type(sym.instType, sym.ctType, sym.settleCcy, base_ccy)
        canonical = _OKX_SPOT_VENUE if mapped_type == "spot" else _OKX_FUTURES_VENUE
        return InstrumentRecord(
            instrument_key=f"{canonical}:{mapped_type.upper()}:{base_ccy}~{quote_ccy}",
            venue=canonical,
            symbol=f"{base_ccy}/{quote_ccy}",
            raw_symbol=sym.instId,
            instrument_type=mapped_type,
            base_asset=base_ccy,
            quote_asset=quote_ccy,
            tick_size=Decimal(str(sym.tickSz or "0.01")),
            lot_size=Decimal(str(sym.lotSz or "0.001")),
            min_order_size=Decimal(str(sym.minSz or "0.001")),
            contract_size=Decimal(str(sym.ctVal)) if sym.ctVal else Decimal("1"),
            settlement_asset=str(settle_ccy) if settle_ccy else None,
            expiry=expiry,
            strike=Decimal(str(strike_val)) if strike_val else None,
            option_type=opt_type,
            margin_type=margin_type,
            is_active=True,
            updated_at=now,
        )

    def _get_margin_type(
        self,
        inst_type: str,
        ct_type: str | None,
        settle_ccy: str | None,
        base_ccy: str,
    ) -> MarginType | None:
        """Derive MarginType from OKX instrument fields.

        SPOT/MARGIN: no margin type.
        Derivatives: ctType="linear" → LINEAR, ctType="inverse" → INVERSE.
        Fallback: settleCcy == baseCcy → INVERSE, else LINEAR.
        """
        if inst_type in ("SPOT", "MARGIN"):
            return None
        if ct_type == "linear":
            return MarginType.LINEAR
        if ct_type == "inverse":
            return MarginType.INVERSE
        # Fallback: coin-margined if settlement currency equals base asset
        if settle_ccy and base_ccy and settle_ccy.upper() == base_ccy.upper():
            return MarginType.INVERSE
        return MarginType.LINEAR

    async def get_exchange_fee_schedule(
        self,
        client_id: str,
    ) -> FeeScheduleEntry | None:
        """Fetch OKX account fee tier via GET /api/v5/account/trade-fee.

        Requires authenticated headers (OK-ACCESS-KEY, OK-ACCESS-SIGN, etc.).
        Stub until OKX authenticated client is wired — logs WARNING and
        returns None so callers fall back to default fee schedule.
        """
        logger.warning(
            "OKXReferenceDataAdapter.get_exchange_fee_schedule: "
            "authenticated fee-rate fetch not yet implemented for client_id=%s; "
            "endpoint: GET /api/v5/account/trade-fee (requires HMAC auth headers). "
            "Returning None — caller should use default FeeScheduleEntry.",
            client_id,
        )
        return None
