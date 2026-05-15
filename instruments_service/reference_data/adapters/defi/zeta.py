"""Zeta Markets reference data adapter — perpetual market discovery.

Discovers Zeta Markets perpetual markets on Solana via the Zeta DEX API.
Markets are returned as InstrumentRecord with instrument_type=PERPETUAL.

Data source: Zeta DEX API (https://dex.zeta.markets/api/) — public, no auth required.
Program ID: ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD
Reference: https://docs.zeta.markets/
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import cast

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

_DATA_API_URL = get_solana_protocol_url("zeta", "api_url") or "https://dex.zeta.markets/api"
_DEFAULT_CHAIN = "SOLANA"
_ZETA_DEPLOY_DATE = get_protocol_floor_date("zeta")


def _classify_zeta_error(exc: Exception, status: int | None = None) -> str:
    """Map a Zeta HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class ZetaReferenceDataAdapter(BaseReferenceDataAdapter):
    """Zeta Markets reference data: perpetual market discovery via DEX API.

    Uses the public Zeta DEX API (https://dex.zeta.markets/api/markets)
    which requires no auth and returns active Zeta perpetual markets.
    Each Zeta perp market produces one instrument with instrument_type=PERPETUAL.
    Zeta Markets settles in USDC.
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
        return f"ZETA-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Zeta."""
        error_code = _classify_zeta_error(exc)
        classification = classify_venue_error("zeta", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Zeta %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch active Zeta perpetual markets as instruments."""
        markets = await self._fetch_perp_markets()
        results: list[InstrumentRecord] = []

        for market in markets:
            if instrument_type not in (None, InstrumentType.PERPETUAL, "perpetual"):
                continue
            record = self._build_perp_record(market)
            if record:
                results.append(record)

        logger.info("Zeta: fetched %d perp instruments on %s", len(results), self._chain)
        return results

    async def _fetch_perp_markets(self) -> list[dict[str, object]]:
        """Fetch all active perp markets from Zeta DEX API."""
        url = f"{_DATA_API_URL}/markets"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "markets")
                raise ConnectionError(str(exc)) from exc
            logger.error("Zeta markets request failed after retries: %s", exc)
            raise

        if isinstance(data, list):
            return cast(list[dict[str, object]], data)
        raw = cast(dict[str, object], data) if isinstance(data, dict) else cast(dict[str, object], {})
        # Zeta API may return {"markets": [...]} or a flat list
        markets = raw.get("markets") or raw.get("data")
        if isinstance(markets, list):
            return cast(list[dict[str, object]], markets)
        return []

    def _build_perp_record(
        self,
        market: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Zeta perp market."""
        # Zeta market fields: symbol / asset / kind / isActive
        symbol = str(market.get("symbol") or market.get("asset") or "")
        if not symbol:
            return None

        # Filter: only perp markets (kind == "future" in Zeta terminology, or "perp")
        kind = str(market.get("kind") or market.get("type") or "perp").lower()
        if kind not in ("perp", "future", "perpetual"):
            return None

        is_active = market.get("isActive") if "isActive" in market else market.get("active")
        if is_active is not None and not is_active:
            return None

        base_asset = symbol.upper().replace("-PERP", "").replace("-USDC", "").replace("/USDC", "")

        venue_tag = self.venue
        instrument_key = f"{venue_tag}:PERP:{base_asset}"

        tick_size_raw = market.get("tickSize") or market.get("priceIncrement") or "0.0001"
        min_size_raw = market.get("minLotSize") or market.get("minOrderSize") or "0.001"

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
            raw_symbol=symbol,
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
            available_from_datetime=_ZETA_DEPLOY_DATE,
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
        raise NotImplementedError("Zeta does not support options via this adapter")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Zeta markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Zeta funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Zeta OHLCV not supported via reference data")
