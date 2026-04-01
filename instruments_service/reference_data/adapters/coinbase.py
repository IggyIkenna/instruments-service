"""Coinbase reference data adapter — REST only, no WebSocket."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    CoinbaseProductInfo,
    CoinbaseProductsResponse,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_BASE = "https://api.coinbase.com"


def _classify_coinbase_error(exc: Exception, status: int | None = None) -> str:
    """Map a Coinbase HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if "429" in str(status or "") or "rate" in msg:
        return "RATE_LIMIT_EXCEEDED"
    if status is not None and status >= 500:
        return "INTERNAL_SERVICE_ERROR"
    if "503" in msg or "unavailable" in msg:
        return "TEMPORARILY_UNAVAILABLE"
    if "500" in msg or "internal" in msg or "server" in msg:
        return "INTERNAL_SERVICE_ERROR"
    return "UNKNOWN"


class CoinbaseReferenceDataAdapter(BaseReferenceDataAdapter):
    """Coinbase reference data: spot products from Advanced Trade REST API."""

    @property
    def venue(self) -> str:
        return "coinbase"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        if instrument_type not in (None, InstrumentType.SPOT_PAIR):
            return []
        url = f"{_BASE}/api/v3/brokerage/products"
        try:
            async with aiohttp.ClientSession() as session, session.get(url) as resp:
                resp.raise_for_status()
                data = CoinbaseProductsResponse.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_coinbase_error(exc)
            classification = classify_venue_error("coinbase", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Coinbase products request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "coinbase",
                    "endpoint": "products",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        products: list[CoinbaseProductInfo] = data.products
        results: list[InstrumentRecord] = []
        for product in products:
            if str(product.status or "") != "online":
                continue
            if str(product.product_type or "") != "SPOT":
                continue
            product_id = product.product_id
            base = product.base_currency_id
            quote = product.quote_currency_id
            if not product_id or not base or not quote:
                continue
            results.append(
                InstrumentRecord(
                    instrument_key=product_id,
                    venue=self.venue,
                    raw_symbol=product_id,
                    instrument_type=InstrumentType.SPOT_PAIR,
                    base_asset=base,
                    quote_asset=quote,
                    status=InstrumentStatus.ACTIVE,
                    tick_size=Decimal(str(product.quote_increment or "0.01")),
                    min_size=Decimal(str(product.base_increment or "0.00000001")),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    available_from_datetime=datetime(2015, 1, 1, tzinfo=UTC),
                )
            )
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        url = f"{_BASE}/api/v3/brokerage/products/{symbol}"
        try:
            async with aiohttp.ClientSession() as session, session.get(url) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                data = CoinbaseProductInfo.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_coinbase_error(exc)
            classification = classify_venue_error("coinbase", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Coinbase product fetch failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "coinbase",
                    "endpoint": "product",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return None
        product_id = data.product_id
        base = data.base_currency_id
        quote = data.quote_currency_id
        if not product_id or not base or not quote:
            return None
        return InstrumentRecord(
            instrument_key=product_id,
            venue=self.venue,
            raw_symbol=product_id,
            instrument_type=InstrumentType.SPOT_PAIR,
            base_asset=base,
            quote_asset=quote,
            status=InstrumentStatus.ACTIVE,
            tick_size=Decimal(str(data.quote_increment or "0.01")),
            min_size=Decimal(str(data.base_increment or "0.00000001")),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            available_from_datetime=datetime(2015, 1, 1, tzinfo=UTC),
        )

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Coinbase does not support options via public REST API")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Coinbase does not support futures expiry calendar via public REST API")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError(
            "Coinbase Advanced Trade does not support perpetual contracts and has no "
            "funding rates. Use Binance, Bybit, OKX, or another perp-capable venue."
        )

    _COINBASE_GRANULARITY_MAP: ClassVar[dict[str, str]] = {
        "1m": "ONE_MINUTE",
        "5m": "FIVE_MINUTE",
        "15m": "FIFTEEN_MINUTE",
        "30m": "THIRTY_MINUTE",
        "1h": "ONE_HOUR",
        "2h": "TWO_HOUR",
        "6h": "SIX_HOUR",
        "1d": "ONE_DAY",
    }

    def _parse_coinbase_candle(self, candle_raw: object, symbol: str, interval: str) -> OHLCVRef | None:
        """Parse a Coinbase candle dict into OHLCVRef. Returns None if invalid."""
        if not isinstance(candle_raw, dict):
            return None
        candle: dict[str, object] = candle_raw
        start_raw = candle.get("start")
        if start_raw is None:
            return None
        ts = datetime.fromtimestamp(int(str(start_raw)), tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(candle.get("open", "0") or "0")),
            high=Decimal(str(candle.get("high", "0") or "0")),
            low=Decimal(str(candle.get("low", "0") or "0")),
            close=Decimal(str(candle.get("close", "0") or "0")),
            volume=Decimal(str(candle.get("volume", "0") or "0")),
            interval=interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV candles from Coinbase Advanced Trade REST API (public endpoint).

        Uses GET /api/v3/brokerage/products/{product_id}/candles.
        Coinbase supported granularities: ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE,
        THIRTY_MINUTE, ONE_HOUR, TWO_HOUR, SIX_HOUR, ONE_DAY.
        """
        granularity = self._COINBASE_GRANULARITY_MAP.get(interval, "ONE_DAY")
        url = f"{_BASE}/api/v3/brokerage/products/{symbol}/candles"
        params = {"granularity": granularity, "limit": str(limit)}
        try:
            async with aiohttp.ClientSession() as session, session.get(url, params=params) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_coinbase_error(exc)
            classification = classify_venue_error("coinbase", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Coinbase candles failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "coinbase",
                    "endpoint": "candles",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        raw_dict: dict[str, object] = raw if isinstance(raw, dict) else {}
        candles_obj: object = raw_dict.get("candles")
        candles_raw: list[object] = candles_obj if isinstance(candles_obj, list) else []
        return [
            ref
            for candle_raw in reversed(candles_raw)
            if (ref := self._parse_coinbase_candle(candle_raw, symbol, interval)) is not None
        ]
