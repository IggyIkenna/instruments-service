"""Deribit reference data adapter — options, futures, perpetuals."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    DeribitGetInstrumentResponse,
    DeribitGetInstrumentsResponse,
    DeribitInstrumentInfoFull,
    classify_venue_error,
)
from unified_api_contracts.internal import (
    AssetClass,
    InstrumentRecord,
    InstrumentStatus,
    InstrumentType,
    MarginType,
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

_BASE = "https://www.deribit.com/api/v2"
_DERIBIT_VENUE = "DERIBIT"  # Deribit is derivatives-only; single canonical name


def _classify_deribit_error(exc: Exception, status: int | None = None) -> str:
    """Map a Deribit HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if "10028" in msg or "10040" in msg or "429" in str(status or "") or "rate" in msg or "too many" in msg:
        return "10028"
    if "13009" in msg or "invalid token" in msg:
        return "13009"
    if "13010" in msg or "token revoked" in msg:
        return "13010"
    if "11044" in msg or "not enough funds" in msg:
        return "11044"
    if status is not None and status >= 500:
        return "10028"
    return "UNKNOWN"


class DeribitReferenceDataAdapter(BaseReferenceDataAdapter):
    """Deribit reference data: options, futures, perpetuals for BTC/ETH."""

    @property
    def venue(self) -> str:
        return _DERIBIT_VENUE

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        currencies = ["BTC", "ETH", "SOL", "USDC"]
        if instrument_type in (None, "perp", "future"):
            kinds = ["future"]
        elif instrument_type == "option":
            kinds = ["option"]
        else:
            kinds = ["future", "option"]

        results: list[InstrumentRecord] = []
        async with aiohttp.ClientSession() as session:
            for currency in currencies:
                for kind in kinds:
                    url = f"{_BASE}/public/get_instruments"
                    params = {"currency": currency, "kind": kind, "expired": "false"}
                    try:
                        async with session.get(url, params=params) as resp:
                            resp.raise_for_status()
                            data = DeribitGetInstrumentsResponse.model_validate(await resp.json())
                    except aiohttp.ClientError as exc:
                        error_code = _classify_deribit_error(exc)
                        classification = classify_venue_error("deribit", error_code)
                        action = classification.action.value if classification else "fail"
                        retry_safe = classification.retry_safe if classification else False
                        logger.error(
                            "Deribit get_instruments failed %s/%s: %s (classified: %s, action: %s)",
                            currency,
                            kind,
                            exc,
                            error_code,
                            action,
                        )
                        log_event(
                            "ADAPTER_FETCH_FAILED",
                            details={
                                "venue": "deribit",
                                "endpoint": "get_instruments",
                                "currency": currency,
                                "kind": kind,
                                "error": str(exc),
                                "error_code": error_code,
                                "action": action,
                                "retry_safe": retry_safe,
                            },
                        )
                        continue
                    for inst_data in data.result:
                        results.append(self._parse_instrument(inst_data))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        async with aiohttp.ClientSession() as session:
            url = f"{_BASE}/public/get_instrument"
            try:
                async with session.get(url, params={"instrument_name": symbol}) as resp:
                    resp.raise_for_status()
                    data = DeribitGetInstrumentResponse.model_validate(await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_deribit_error(exc)
                classification = classify_venue_error("deribit", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Deribit get_instrument failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "deribit",
                        "endpoint": "get_instrument",
                        "symbol": symbol,
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                return None
        if not data.result:
            return None
        return self._parse_instrument(data.result)

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
            if inst.base != underlying.upper():
                continue
            if expiry and inst.expiry and inst.expiry.date() != expiry.date():
                continue
            if inst.strike:
                strikes.add(inst.strike)
            # Deribit option_key format: "BTC-31DEC24-50000-C" (last segment = C/P)
            option_side = inst.instrument_key.split("-")[-1].upper()
            if option_side == "C":
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
            if inst.base == underlying.upper() and inst.expiry and str(inst.instrument_type).lower() == instrument_type:
                expiry_set.add(inst.expiry)
        return CanonicalExpiryCalendar(
            venue=self.venue,
            instrument_type=instrument_type,
            underlying=underlying,
            expiries=sorted(expiry_set),
            updated_at=datetime.now(UTC),
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Deribit public endpoint.

        Uses GET /public/get_funding_rate_history with an 8-hour window.
        Returns the most recent entry. Deribit perpetuals settle funding every 8h.
        """
        url = f"{_BASE}/public/get_funding_rate_history"
        now = datetime.now(UTC)
        end_ts = int(now.timestamp() * 1000)
        start_ts = end_ts - 8 * 3600 * 1000
        params = {
            "instrument_name": symbol,
            "start_timestamp": str(start_ts),
            "end_timestamp": str(end_ts),
        }
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_deribit_error(exc)
            classification = classify_venue_error("deribit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Deribit get_funding_rate_history failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "deribit",
                    "endpoint": "get_funding_rate_history",
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
        result_list: list[object] = result_obj if isinstance(result_obj, list) else []
        entry_obj: object = result_list[-1] if result_list else {}
        entry: dict[str, object] = entry_obj if isinstance(entry_obj, dict) else {}
        rate_raw = entry.get("interest8h", entry.get("funding", "0")) or "0"
        ts_raw = entry.get("timestamp", int(now.timestamp() * 1000))
        next_funding_time = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    _DERIBIT_RESOLUTION_MAP: ClassVar[dict[str, str]] = {
        "1m": "1",
        "3m": "3",
        "5m": "5",
        "10m": "10",
        "15m": "15",
        "30m": "30",
        "1h": "60",
        "2h": "120",
        "3h": "180",
        "6h": "360",
        "1d": "1D",
    }
    _DERIBIT_INTERVAL_SECONDS: ClassVar[dict[str, int]] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "10m": 600,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "3h": 10800,
        "6h": 21600,
        "1d": 86400,
    }

    def _extract_deribit_ohlcv_arrays(
        self, result: dict[str, object]
    ) -> tuple[list[object], list[object], list[object], list[object], list[object], list[object]]:
        """Extract ticks, opens, highs, lows, closes, volumes arrays from Deribit result dict."""

        def _to_list(v: object) -> list[object]:
            return v if isinstance(v, list) else []

        return (
            _to_list(result.get("ticks")),
            _to_list(result.get("open")),
            _to_list(result.get("high")),
            _to_list(result.get("low")),
            _to_list(result.get("close")),
            _to_list(result.get("volume")),
        )

    def _build_ohlcv_bars(
        self,
        ticks: list[object],
        opens: list[object],
        highs: list[object],
        lows: list[object],
        closes: list[object],
        volumes: list[object],
        symbol: str,
        interval: str,
    ) -> list[OHLCVRef]:
        """Build OHLCVRef list from Deribit parallel tick/open/high/low/close/volume arrays."""
        results: list[OHLCVRef] = []
        for i, tick in enumerate(ticks):
            if i >= len(opens) or i >= len(closes):
                break
            ts = datetime.fromtimestamp(int(str(tick)) / 1000, tz=UTC)
            results.append(
                OHLCVRef(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(opens[i])),
                    high=Decimal(str(highs[i])) if i < len(highs) else Decimal(str(opens[i])),
                    low=Decimal(str(lows[i])) if i < len(lows) else Decimal(str(opens[i])),
                    close=Decimal(str(closes[i])),
                    volume=Decimal(str(volumes[i])) if i < len(volumes) else Decimal("0"),
                    interval=interval,
                )
            )
        return results

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Deribit public TradingView chart data endpoint.

        Uses GET /public/get_tradingview_chart_data.
        Deribit resolution strings: 1, 3, 5, 10, 15, 30, 60, 120, 180, 360, 1D.
        """
        resolution = self._DERIBIT_RESOLUTION_MAP.get(interval, "1D")
        bar_seconds = self._DERIBIT_INTERVAL_SECONDS.get(interval, 86400)
        now = datetime.now(UTC)
        end_ts = int(now.timestamp())
        start_ts = end_ts - limit * bar_seconds
        url = f"{_BASE}/public/get_tradingview_chart_data"
        params = {
            "instrument_name": symbol,
            "resolution": resolution,
            "start_timestamp": str(start_ts * 1000),
            "end_timestamp": str(end_ts * 1000),
        }
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_deribit_error(exc)
            classification = classify_venue_error("deribit", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Deribit get_tradingview_chart_data failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "deribit",
                    "endpoint": "get_tradingview_chart_data",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        result_obj = raw_dict.get("result")
        result: dict[str, object] = result_obj if isinstance(result_obj, dict) else {}
        ticks, opens, highs, lows, closes, volumes = self._extract_deribit_ohlcv_arrays(result)
        return self._build_ohlcv_bars(ticks, opens, highs, lows, closes, volumes, symbol, interval)

    def _parse_instrument(self, data: DeribitInstrumentInfoFull) -> InstrumentRecord:
        expiry_ts = data.expiration_timestamp
        expiry: datetime | None = None
        if expiry_ts:
            expiry = datetime.fromtimestamp(int(str(expiry_ts)) / 1000, tz=UTC)
        kind = data.kind or ""
        inst_name = (data.instrument_name or "").lower()
        if kind == "option":
            inst_type = InstrumentType.OPTION
        elif "perpetual" in inst_name:
            inst_type = InstrumentType.PERP
        else:
            inst_type = InstrumentType.FUTURES
        instrument_name = data.instrument_name or ""
        base_currency = data.base_currency or ""
        quote_currency = data.quote_currency or "USD"
        # Inverse if settlement currency matches the base coin (coin-margined)
        margin_type: MarginType | None = None
        if inst_type in (InstrumentType.PERP, InstrumentType.FUTURES):
            settle_ccy = (data.settlement_currency or "").upper()
            base_ccy = base_currency.upper()
            margin_type = MarginType.INVERSE if settle_ccy == base_ccy else MarginType.LINEAR
        # Canonical type string — always lowercase to match InstrumentType enum values.
        # Key uses uppercase for readability; type field stores the canonical enum value.
        inst_type_str: str = inst_type.value  # e.g. "perp", "futures", "option"
        return InstrumentRecord(
            instrument_key=f"{_DERIBIT_VENUE}:{inst_type_str.upper()}:{instrument_name}",
            venue=_DERIBIT_VENUE,
            asset_class=AssetClass.CRYPTO,
            instrument_type=inst_type_str,
            base=base_currency,
            quote=quote_currency,
            tick_size=Decimal(str(data.tick_size or "0.01")),
            lot_size=Decimal(str(data.min_trade_amount or "0.1")),
            contract_size=Decimal(str(data.contract_size or "1")),
            expiry=expiry,
            strike=Decimal(str(data.strike)) if data.strike else None,
            option_type=str(data.option_type).lower() if data.option_type else None,
            underlying=base_currency if kind == "option" else None,
            margin_type=margin_type,
            status=InstrumentStatus.ACTIVE,
        )
