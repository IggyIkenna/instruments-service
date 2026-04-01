"""Kalshi reference data adapter — prediction market instrument listing.

Kalshi's REST API provides market metadata for prediction markets.
Each active market is returned as an InstrumentRecord so services can treat
prediction markets as tradeable instruments.

Base URL: https://trading-api.kalshi.com/trade-api/v2
Auth: API key passed as header (RSA key signing for production).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import aiohttp
from unified_api_contracts import (
    KalshiMarket,
    classify_venue_error,
)
from unified_api_contracts.internal import InstrumentRecord, InstrumentType
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_KALSHI_BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
_PAGE_LIMIT = 200
_MAX_PAGES = 10  # cap at 2000 markets per fetch


_STATUS_MAP: dict[int, str] = {429: "429", 401: "401", 403: "403", 400: "400"}
_MSG_PATTERNS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("429", "rate"), "429"),
    (("401", "unauthorized"), "401"),
    (("403", "forbidden"), "403"),
    (("400", "bad request"), "400"),
    (("500", "internal", "server"), "500"),
)


def _classify_kalshi_error(exc: Exception, status: int | None = None) -> str:
    """Map a Kalshi HTTP/network error to a UAC error code for classification."""
    if status is not None:
        if status in _STATUS_MAP:
            return _STATUS_MAP[status]
        if status >= 500:
            return "500"
    msg = str(exc).lower()
    for keywords, code in _MSG_PATTERNS:
        if any(kw in msg for kw in keywords):
            return code
    return "UNKNOWN"


class KalshiReferenceDataAdapter(BaseReferenceDataAdapter):
    """Kalshi reference data adapter.

    Returns active prediction markets as InstrumentRecord instances.
    Each market becomes one InstrumentRecord:
      instrument_key  = market ticker (e.g. "KXBTC-21MAR26-T95000")
      venue           = "kalshi"
      instrument_type = "prediction_market"
      raw_symbol      = event_ticker
      base_asset      = series_ticker
      quote_asset     = "USD"
      is_active       = status == "active"

    Auth: service must fetch kalshi-api-key from Secret Manager and pass
    it via ``api_key`` constructor parameter.
    """

    def __init__(
        self,
        api_key: str | None = None,
        project_id: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)

    @property
    def venue(self) -> str:
        return "kalshi"

    def _get_headers(self) -> dict[str, str]:
        """Build request headers with optional API key auth."""
        headers: dict[str, str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self._api_key is not None:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Kalshi markets as InstrumentRecord list.

        instrument_type filter: pass "PREDICTION_MARKET" or None (all). Other values
        return an empty list since Kalshi only exposes prediction markets.
        """
        if instrument_type is not None and instrument_type != "PREDICTION_MARKET":
            return []
        results: list[InstrumentRecord] = []
        now = datetime.now(UTC)
        cursor: str | None = None
        async with aiohttp.ClientSession() as session:
            for _page in range(_MAX_PAGES):
                batch, cursor = await self._fetch_markets_page(session, cursor, now)
                results.extend(batch)
                if cursor is None or len(batch) < _PAGE_LIMIT:
                    break
        return results

    async def _fetch_markets_page(
        self,
        session: aiohttp.ClientSession,
        cursor: str | None,
        now: datetime,
    ) -> tuple[list[InstrumentRecord], str | None]:
        """Fetch one page of markets from Kalshi API.

        Returns (records, next_cursor). next_cursor is None when no more pages.
        """
        url = f"{_KALSHI_BASE_URL}/markets"
        params: dict[str, str] = {
            "limit": str(_PAGE_LIMIT),
            "status": "active",
        }
        if cursor is not None:
            params["cursor"] = cursor
        headers = self._get_headers()
        try:
            async with session.get(url, params=params, headers=headers) as resp:
                if resp.status == 401:
                    logger.error("Kalshi authentication failed (HTTP 401)")
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": "kalshi",
                            "endpoint": "markets",
                            "error": "HTTP 401 Unauthorized",
                            "error_code": "401",
                            "action": "fail",
                            "retry_safe": False,
                        },
                    )
                    return [], None
                resp.raise_for_status()
                raw_json: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_kalshi_error(exc)
            classification = classify_venue_error("kalshi", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Kalshi markets request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "kalshi",
                    "endpoint": "markets",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return [], None

        raw_dict = cast(dict[str, object], raw_json)
        markets_raw = raw_dict.get("markets")
        next_cursor_raw = raw_dict.get("cursor")
        next_cursor = str(next_cursor_raw) if next_cursor_raw else None

        if not isinstance(markets_raw, list):
            return [], None

        records: list[InstrumentRecord] = []
        for raw_item in markets_raw:
            record = self._parse_market(cast(dict[str, object], raw_item), now)
            if record is not None:
                records.append(record)
        return records, next_cursor

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch single market by ticker."""
        url = f"{_KALSHI_BASE_URL}/markets/{symbol}"
        headers = self._get_headers()
        now = datetime.now(UTC)
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 404:
                        return None
                    resp.raise_for_status()
                    raw_json: object = cast(object, await resp.json())
            except aiohttp.ClientError as exc:
                error_code = _classify_kalshi_error(exc)
                classification = classify_venue_error("kalshi", error_code)
                action = classification.action.value if classification else "fail"
                retry_safe = classification.retry_safe if classification else False
                logger.error(
                    "Kalshi get_instrument failed for %s: %s (classified: %s, action: %s)",
                    symbol,
                    exc,
                    error_code,
                    action,
                )
                log_event(
                    "ADAPTER_FETCH_FAILED",
                    details={
                        "venue": "kalshi",
                        "endpoint": f"markets/{symbol}",
                        "error": str(exc),
                        "error_code": error_code,
                        "action": action,
                        "retry_safe": retry_safe,
                    },
                )
                return None

        raw_dict = cast(dict[str, object], raw_json)
        market_raw = raw_dict.get("market")
        if not isinstance(market_raw, dict):
            return None
        return self._parse_market(cast(dict[str, object], market_raw), now)

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError(
            "Kalshi does not provide options chains. Use get_instruments() to list available prediction markets."
        )

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError(
            "Kalshi does not provide expiry calendars. Market close times are in InstrumentRecord.expiry."
        )

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Kalshi does not have perpetual funding rates.")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Kalshi OHLCV is not available via reference data API.")

    def _parse_market(
        self,
        raw: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Map a Kalshi market dict to an InstrumentRecord."""
        market = KalshiMarket.model_validate(raw)
        ticker = market.ticker
        if not ticker:
            return None
        event_ticker = market.event_ticker or ticker
        series_ticker = getattr(market, "series_ticker", None) or event_ticker
        title = getattr(market, "title", None) or getattr(market, "subtitle", None) or ticker
        base_asset = str(series_ticker)[:50] if series_ticker else ticker[:50]
        expiry = self._parse_close_time(market.close_time)
        status_raw = getattr(market, "status", None)
        is_active = str(status_raw).lower() == "active" if status_raw else True
        tick_raw = getattr(market, "tick_size", None)
        tick_size = Decimal(str(tick_raw)) if tick_raw else Decimal("0.01")
        min_order_raw = getattr(market, "min_order_size", None)
        min_order = Decimal(str(min_order_raw)) if min_order_raw else Decimal("1")
        return InstrumentRecord(
            instrument_key=ticker,
            venue=self.venue,
            symbol=str(title)[:100],
            raw_symbol=event_ticker,
            instrument_type=InstrumentType.PREDICTION_MARKET,
            base_asset=base_asset,
            quote_asset="USD",
            tick_size=tick_size,
            min_size=Decimal("1"),
            min_order_size=min_order,
            contract_size=Decimal("1"),
            settlement_asset="USD",
            expiry=expiry,
            strike=None,
            option_type=None,
            is_active=is_active,
            updated_at=now,
        )

    def _parse_close_time(self, close_time_raw: str | None) -> datetime | None:
        """Parse Kalshi close_time ISO string to UTC datetime."""
        if not close_time_raw:
            return None
        try:
            return datetime.fromisoformat(close_time_raw.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            return None
