"""Hyperliquid reference data adapter — REST only, no WebSocket."""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar, cast

import aiohttp
from unified_api_contracts import (
    CEFI_BASE_ASSET_UNIVERSE,
    HyperliquidAssetInfo,
    HyperliquidMeta,
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

_BASE = "https://api.hyperliquid.xyz"


def _classify_hyperliquid_error(exc: Exception, status: int | None = None) -> str:
    """Map a Hyperliquid HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    if "margin" in msg:
        return "INSUFFICIENT_MARGIN"
    return "UNKNOWN"


class HyperliquidReferenceDataAdapter(BaseReferenceDataAdapter):
    """Hyperliquid reference data: perpetuals from meta REST endpoint."""

    @property
    def venue(self) -> str:
        return "HYPERLIQUID"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        if instrument_type not in (None, "perp"):
            return []
        url = f"{_BASE}/info"
        payload = {"type": "meta"}
        try:
            async with aiohttp.ClientSession() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = HyperliquidMeta.model_validate(await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid meta request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "meta",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        universe: list[HyperliquidAssetInfo] = data.universe or []
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []
        for asset in universe:
            name = asset.name
            if not name:
                continue
            if name.upper() not in CEFI_BASE_ASSET_UNIVERSE:
                continue
            sz_decimals: int = asset.szDecimals or 3
            lot = Decimal(10) ** -sz_decimals
            results.append(
                InstrumentRecord(
                    instrument_key=name,
                    venue=self.venue,
                    symbol=f"{name}/USD",
                    raw_symbol=name,
                    instrument_type="perp",
                    base_asset=name,
                    quote_asset="USD",
                    tick_size=Decimal("0.001"),
                    lot_size=lot,
                    min_order_size=lot,
                    contract_size=Decimal("1"),
                    settlement_asset="USDC",
                    expiry=None,
                    strike=None,
                    option_type=None,
                    is_active=True,
                    updated_at=now,
                )
            )
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
        raise NotImplementedError("Hyperliquid does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Hyperliquid perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Fetch latest funding rate from Hyperliquid /info endpoint (fundingHistory type).

        POST /info with type=fundingHistory. Returns most recent rate.
        """
        url = f"{_BASE}/info"
        now = datetime.now(UTC)
        payload = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": int((now.timestamp() - 8 * 3600) * 1000),
        }
        try:
            async with aiohttp.ClientSession() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                raw: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid fundingHistory request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "fundingHistory",
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
        entries: list[object] = raw if isinstance(raw, list) else []
        entry_obj: object = entries[-1] if entries else {}
        entry: dict[str, object] = entry_obj if isinstance(entry_obj, dict) else {}
        rate_raw = entry.get("fundingRate", "0") or "0"
        ts_raw = entry.get("time", int(now.timestamp() * 1000))
        next_funding_time = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return FundingRateRef(
            venue=self.venue,
            symbol=symbol,
            rate=Decimal(str(rate_raw)),
            next_funding_time=next_funding_time,
            mark_price=None,
            updated_at=now,
        )

    _INTERVAL_SECONDS: ClassVar[dict[str, int]] = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }

    def _resolve_hl_interval(self, interval: str) -> tuple[str, int]:
        """Return (hl_interval, bar_seconds) for a given interval string."""
        hl = interval if interval in self._INTERVAL_SECONDS else "1d"
        return hl, self._INTERVAL_SECONDS.get(hl, 86400)

    def _parse_hl_candle(self, candle_raw: object, symbol: str, hl_interval: str) -> OHLCVRef | None:
        """Parse a single Hyperliquid candle dict into an OHLCVRef. Returns None if invalid."""
        if not isinstance(candle_raw, dict):
            return None
        candle: dict[str, object] = candle_raw
        ts_raw = candle.get("t") or candle.get("T") or 0
        ts = datetime.fromtimestamp(int(str(ts_raw)) / 1000, tz=UTC)
        return OHLCVRef(
            venue=self.venue,
            symbol=symbol,
            timestamp=ts,
            open=Decimal(str(candle.get("o", "0") or "0")),
            high=Decimal(str(candle.get("h", "0") or "0")),
            low=Decimal(str(candle.get("l", "0") or "0")),
            close=Decimal(str(candle.get("c", "0") or "0")),
            volume=Decimal(str(candle.get("v", "0") or "0")),
            interval=hl_interval,
        )

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Fetch OHLCV bars from Hyperliquid /info candleSnapshot endpoint.

        POST /info with type=candleSnapshot. Supported intervals: 1m, 5m, 15m, 30m, 1h, 4h, 1d.
        """
        hl_interval, bar_seconds = self._resolve_hl_interval(interval)
        now = datetime.now(UTC)
        end_ts = int(now.timestamp() * 1000)
        start_ts = end_ts - limit * bar_seconds * 1000
        url = f"{_BASE}/info"
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": hl_interval,
                "startTime": start_ts,
                "endTime": end_ts,
            },
        }
        try:
            async with aiohttp.ClientSession() as session, session.post(url, json=payload) as resp:
                resp.raise_for_status()
                raw_ohlcv: object = cast(object, await resp.json())
        except aiohttp.ClientError as exc:
            error_code = _classify_hyperliquid_error(exc)
            classification = classify_venue_error("hyperliquid", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Hyperliquid candleSnapshot request failed for %s: %s (classified: %s, action: %s)",
                symbol,
                exc,
                error_code,
                action,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "hyperliquid",
                    "endpoint": "candleSnapshot",
                    "symbol": symbol,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []
        candles: list[object] = raw_ohlcv if isinstance(raw_ohlcv, list) else []
        return [
            bar for candle_raw in candles if (bar := self._parse_hl_candle(candle_raw, symbol, hl_interval)) is not None
        ]
