"""Phoenix CLOB DEX reference data adapter — market discovery.

Discovers Phoenix Central Limit Order Book (CLOB) markets on Solana via
the Phoenix public API. Markets are returned as InstrumentRecord with
instrument_type=SPOT (CLOB-based spot trading pairs).

Data source: Phoenix API (https://api.phoenix.trade/) — public, no auth required.
Program ID: PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY (Phoenix Markets)
Reference: https://docs.phoenix.trade/
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

_DATA_API_URL = get_solana_protocol_url("phoenix", "api_url") or "https://api.phoenix.trade"
_DEFAULT_CHAIN = "SOLANA"
_PHOENIX_DEPLOY_DATE = get_protocol_floor_date("phoenix")


def _classify_phoenix_error(exc: Exception, status: int | None = None) -> str:
    """Map a Phoenix HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class PhoenixReferenceDataAdapter(BaseReferenceDataAdapter):
    """Phoenix CLOB DEX reference data: market discovery via public API.

    Uses the public Phoenix API (https://api.phoenix.trade/markets)
    which requires no auth and returns active CLOB markets.
    Each Phoenix market produces one SPOT instrument.
    Phoenix is an on-chain CLOB DEX (unlike AMMs), providing orderbook-based trading.
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
        """Return the venue identifier."""
        return f"PHOENIX-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Phoenix."""
        error_code = _classify_phoenix_error(exc)
        classification = classify_venue_error("phoenix", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Phoenix %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch active Phoenix CLOB markets as instruments."""
        if instrument_type not in (None, InstrumentType.SPOT_PAIR, "spot"):
            logger.info("Phoenix only supports SPOT instruments; requested %s", instrument_type)
            return []

        markets = await self._fetch_markets()
        results: list[InstrumentRecord] = []

        for market in markets:
            record = self._build_market_record(market)
            if record:
                results.append(record)

        logger.info("Phoenix: fetched %d CLOB market instruments on %s", len(results), self._chain)
        return results

    async def _fetch_markets(self) -> list[dict[str, object]]:
        """Fetch all active CLOB markets from the Phoenix API."""
        url = f"{_DATA_API_URL}/markets"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "markets")
                raise ConnectionError(str(exc)) from exc
            logger.error("Phoenix markets request failed after retries: %s", exc)
            raise

        if isinstance(data, list):
            return data
        raw: dict[str, object] = data if isinstance(data, dict) else {}
        # Phoenix API may return {"markets": [...]} or {"data": [...]}
        markets = raw.get("markets") or raw.get("data")
        if isinstance(markets, list):
            return markets
        return []

    def _build_market_record(
        self,
        market: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Phoenix CLOB market."""
        # Phoenix market fields: market_address, base_params, quote_params, name
        market_address = str(market.get("market_address") or market.get("address") or "")
        market_name = str(market.get("name") or market.get("symbol") or "")

        # Extract base/quote from name (e.g. "SOL/USDC") or params
        base_params = market.get("base_params") or {}
        quote_params = market.get("quote_params") or {}

        if isinstance(base_params, dict) and base_params and isinstance(quote_params, dict) and quote_params:
            base_asset = str(base_params.get("symbol") or base_params.get("name") or "").upper()
            quote_asset = str(quote_params.get("symbol") or quote_params.get("name") or "USDC").upper()
        elif market_name and "/" in market_name:
            parts = market_name.upper().split("/")
            base_asset = parts[0].strip()
            quote_asset = parts[1].strip() if len(parts) > 1 else "USDC"
        elif market_name and "-" in market_name:
            parts = market_name.upper().split("-")
            base_asset = parts[0].strip()
            quote_asset = parts[1].strip() if len(parts) > 1 else "USDC"
        else:
            return None

        if not base_asset:
            return None

        raw_symbol = market_address if market_address else market_name
        instrument_key = f"{self.venue}:SPOT:{base_asset}-{quote_asset}"

        # Phoenix tick size in lot sizes
        raw_tick = market.get("tick_size_in_quote_atoms_per_base_unit") or market.get("tickSize") or "1"
        try:
            tick_size = Decimal(str(raw_tick))
            # Phoenix uses atoms (smallest units), convert to human-readable
            if tick_size > Decimal("1000"):
                tick_size = tick_size / Decimal("1000000")
        except (ValueError, TypeError):
            tick_size = Decimal("0.0001")

        raw_lot = market.get("base_lot_size") or market.get("lotSize") or "1"
        try:
            min_size = Decimal(str(raw_lot))
            if min_size > Decimal("1000"):
                min_size = min_size / Decimal("1000000000")
        except (ValueError, TypeError):
            min_size = Decimal("0.001")

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            raw_symbol=raw_symbol,
            instrument_type=InstrumentType.SPOT_PAIR,
            base_asset=base_asset,
            quote_asset=quote_asset,
            settle_asset=quote_asset,
            margin_type=MarginType.LINEAR,
            tick_size=tick_size,
            min_size=min_size,
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_PHOENIX_DEPLOY_DATE,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        sym_upper = symbol.upper()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == sym_upper:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Phoenix does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Phoenix CLOB markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Phoenix funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Phoenix OHLCV not supported via reference data")
