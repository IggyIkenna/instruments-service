"""Flash Trade reference data adapter — perpetual market discovery.

Discovers Flash Trade perpetual markets on Solana via the Flash Trade REST API.
Markets are returned as InstrumentRecord with instrument_type=PERPETUAL.

Data source: Flash Trade API (https://api.flash.trade/api/v1/) — public, no auth required.
Program ID: FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn
Reference: https://docs.flash.trade/
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.registry import get_solana_protocol_url
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_DATA_API_URL = get_solana_protocol_url("flash_trade", "api_url") or "https://api.flash.trade/api/v1"
_DEFAULT_CHAIN = "SOLANA"
_FLASH_DEPLOY_DATE = get_protocol_floor_date("flash_trade")


def _classify_flash_error(exc: Exception, status: int | None = None) -> str:
    """Map a Flash Trade HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class FlashTradeReferenceDataAdapter(BaseReferenceDataAdapter):
    """Flash Trade reference data: perpetual market discovery via REST API.

    Uses the public Flash Trade API (https://api.flash.trade/api/v1/markets)
    which requires no auth and returns active Flash Trade perpetual markets.
    Each Flash Trade perp market produces one instrument with instrument_type=PERPETUAL.
    Flash Trade settles in USDC.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()

    @property
    def venue(self) -> str:
        return f"FLASH-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Flash Trade."""
        error_code = _classify_flash_error(exc)
        classification = classify_venue_error("flash_trade", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Flash Trade %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            endpoint,
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": endpoint,
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Flash Trade perpetual markets as instruments."""
        markets = await self._fetch_perp_markets()
        results: list[InstrumentRecord] = []

        for market in markets:
            if instrument_type not in (None, InstrumentType.PERPETUAL, "perpetual"):
                continue
            record = self._build_perp_record(market)
            if record:
                results.append(record)

        logger.info("Flash Trade: fetched %d perp instruments on %s", len(results), self._chain)
        return results

    async def _fetch_perp_markets(self) -> list[dict[str, object]]:
        """Fetch all active perp markets from Flash Trade API."""
        url = f"{_DATA_API_URL}/markets"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "markets")
                raise ConnectionError(str(exc)) from exc
            logger.error("Flash Trade markets request failed after retries: %s", exc)
            raise

        if isinstance(data, list):
            return data
        raw: dict[str, object] = data if isinstance(data, dict) else {}
        markets = raw.get("markets") or raw.get("data") or raw.get("pools")
        if isinstance(markets, list):
            return markets
        return []

    def _build_perp_record(
        self,
        market: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Flash Trade perp market."""
        # Flash Trade market fields: name, token, side, isActive / enabled
        name = str(market.get("name") or market.get("symbol") or market.get("token") or "")
        if not name:
            return None

        is_active = (
            market.get("isActive")
            if "isActive" in market
            else market.get("enabled")
            if "enabled" in market
            else market.get("active")
        )
        if is_active is not None and not is_active:
            return None

        # Flash Trade uses custody/pool names like "SOL-USDC", "BTC-USDC"
        base_asset = name.upper().split("-")[0].replace("_PERP", "").replace("/USDC", "")

        venue_tag = self.venue
        instrument_key = f"{venue_tag}:PERP:{base_asset}"

        tick_size_raw = market.get("tickSize") or market.get("priceIncrement") or "0.0001"
        min_size_raw = market.get("minSize") or market.get("minTradeSize") or "0.001"

        try:
            tick_size = Decimal(str(tick_size_raw))
        except Exception:
            tick_size = Decimal("0.0001")

        try:
            min_size = Decimal(str(min_size_raw))
        except Exception:
            min_size = Decimal("0.001")

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=name,
            instrument_type=InstrumentType.PERPETUAL,
            base_asset=base_asset,
            quote_asset="USDC",
            settle_asset="USDC",
            margin_type=MarginType.LINEAR,
            tick_size=tick_size,
            min_size=min_size,
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_FLASH_DEPLOY_DATE,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == symbol.upper():
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Flash Trade does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Flash Trade markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Flash Trade funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Flash Trade OHLCV not supported via reference data")
