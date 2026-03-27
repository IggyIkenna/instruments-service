"""Aster reference data adapter — REST only, no WebSocket.

Aster is an on-chain perpetual futures exchange (CLOB model, Binance Futures-compatible API).
Base URL: https://www.aster.exchange (fapi subdomain deprecated)
API docs: https://github.com/asterdex/api-docs
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    CEFI_ACCEPTED_QUOTE_ASSETS,
    CEFI_BASE_ASSET_UNIVERSE,
    AsterExchangeInfo,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.aster.exchange"


def _classify_aster_error(exc: Exception, status: int | None = None) -> str:
    """Map an Aster HTTP/network error to a UAC error code for classification."""
    if status == 429:
        return "429"
    if status == 503:
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg or "-1003" in msg:
        return "-1003"
    if "503" in msg or "unavailable" in msg:
        return "503"
    if "429" in msg:
        return "429"
    return "UNKNOWN"


def _extract_filter_value(filters: list[object], filter_type: str, field: str) -> str:
    """Extract a value from Binance-style symbol filters list."""
    for f in filters:
        if isinstance(f, dict) and f.get("filterType") == filter_type:
            val = f.get(field)
            if val is not None:
                return str(val)
    return ""


class AsterReferenceDataAdapter(BaseReferenceDataAdapter):
    """Aster reference data: perpetual futures from exchangeInfo REST endpoint.

    Aster uses a Binance Futures-compatible REST API.  Instruments are
    fetched from GET /fapi/v1/exchangeInfo and filtered to PERPETUAL contracts
    with status TRADING.
    """

    @property
    def venue(self) -> str:
        return "aster"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active perpetual instruments from Aster exchangeInfo."""
        if instrument_type not in (None, "perp"):
            return []

        url = f"{_BASE}/fapi/v1/exchangeInfo"
        try:
            async with aiohttp.ClientSession() as session, session.get(url) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
                    "endpoint": "exchangeInfo",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        data = AsterExchangeInfo.model_validate(raw)
        symbols: list[object] = data.symbols or []
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []

        for sym_raw in symbols:
            if not isinstance(sym_raw, dict):
                continue
            contract_type = sym_raw.get("contractType")
            status = sym_raw.get("status")
            if contract_type != "PERPETUAL" or status != "TRADING":
                continue

            base_asset: str = str(sym_raw.get("baseAsset", ""))
            quote_asset: str = str(sym_raw.get("quoteAsset", "USDC"))
            raw_symbol: str = str(sym_raw.get("symbol", ""))
            if not base_asset or not raw_symbol:
                continue
            if base_asset.upper() not in CEFI_BASE_ASSET_UNIVERSE:
                continue
            if quote_asset.upper() not in CEFI_ACCEPTED_QUOTE_ASSETS:
                continue

            filters: list[object] = sym_raw.get("filters", [])
            tick_str = _extract_filter_value(filters, "PRICE_FILTER", "tickSize")
            lot_str = _extract_filter_value(filters, "LOT_SIZE", "stepSize")

            tick_size = Decimal(tick_str) if tick_str else Decimal("0.01")
            lot_size = Decimal(lot_str) if lot_str else Decimal("0.001")

            results.append(
                InstrumentRecord(
                    instrument_key=f"ASTER:PERPETUAL:{raw_symbol}",
                    venue="ASTER",
                    symbol=f"{base_asset}/{quote_asset}",
                    raw_symbol=raw_symbol,
                    instrument_type="perp",
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                    tick_size=tick_size,
                    lot_size=lot_size,
                    min_order_size=lot_size,
                    contract_size=Decimal("1"),
                    settlement_asset="USDC",
                    expiry=None,
                    strike=None,
                    option_type=None,
                    is_active=True,
                    updated_at=now,
                )
            )

        logger.info("Aster: fetched %d perpetual instruments", len(results))
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
        raise NotImplementedError("Aster does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Aster perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Aster premiumIndex endpoint.

        GET /fapi/v1/premiumIndex?symbol=<symbol>
        Returns lastFundingRate and nextFundingTime.
        """
        url = f"{_BASE}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        now = datetime.now(UTC)

        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster premiumIndex request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
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

        entry: dict[str, object] = raw if isinstance(raw, dict) else {}
        rate_raw = entry.get("lastFundingRate", "0") or "0"
        next_time_raw = entry.get("nextFundingTime", 0)
        mark_raw = entry.get("markPrice", "0") or "0"

        next_funding_time = (
            datetime.fromtimestamp(int(str(next_time_raw)) / 1000, tz=UTC) if int(str(next_time_raw)) > 0 else now
        )

        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=Decimal(str(mark_raw)) if mark_raw != "0" else None,
            updated_at=now,
        )

    _INTERVAL_SECONDS: ClassVar[dict[str, int]] = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "8h": 28800,
        "12h": 43200,
        "1d": 86400,
        "1w": 604800,
    }

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Aster klines endpoint (Binance Futures-compatible).

        GET /fapi/v1/klines?symbol=<symbol>&interval=<interval>&limit=<limit>
        """
        aster_interval = interval if interval in self._INTERVAL_SECONDS else "1d"
        bar_seconds = self._INTERVAL_SECONDS.get(aster_interval, 86400)

        now = datetime.now(UTC)
        end_ts = int(now.timestamp() * 1000)
        start_ts = end_ts - limit * bar_seconds * 1000

        url = f"{_BASE}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": aster_interval,
            "startTime": str(start_ts),
            "endTime": str(end_ts),
            "limit": str(limit),
        }

        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw_ohlcv: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_aster_error(exc)
            classification = classify_venue_error("aster", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Aster klines request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "aster",
                    "endpoint": "klines",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        candles: list[object] = raw_ohlcv if isinstance(raw_ohlcv, list) else []
        results: list[OHLCVRef] = []

        for candle_raw in candles:
            if not isinstance(candle_raw, list) or len(candle_raw) < 6:
                continue
            # Binance kline format: [openTime, open, high, low, close, volume, ...]
            ts = datetime.fromtimestamp(int(str(candle_raw[0])) / 1000, tz=UTC)
            results.append(
                OHLCVRef(
                    venue=self.venue,
                    symbol=symbol,
                    timestamp=ts,
                    open=Decimal(str(candle_raw[1])),
                    high=Decimal(str(candle_raw[2])),
                    low=Decimal(str(candle_raw[3])),
                    close=Decimal(str(candle_raw[4])),
                    volume=Decimal(str(candle_raw[5])),
                    interval=aster_interval,
                )
            )

        return results
