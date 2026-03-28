"""Drift reference data adapter -- instrument discovery via DLOB API.

Discovers Drift perpetual and spot markets on Solana.
Markets are returned as InstrumentRecord with instrument_type="perpetual" or "spot".

Data source: Drift DLOB API (https://dlob.drift.trade) — public, no auth required.
Reference: https://docs.drift.trade/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import DEFI_MAJOR_ASSET_SYMBOLS, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord
from unified_api_contracts.registry import get_solana_protocol_url
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

_DATA_API_URL = get_solana_protocol_url("drift", "api_url") or "https://data.api.drift.trade"
_DEFAULT_CHAIN = "SOLANA"


def _classify_drift_error(exc: Exception, status: int | None = None) -> str:
    """Map a Drift HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class DriftReferenceDataAdapter(BaseReferenceDataAdapter):
    """Drift reference data: perp and spot market discovery via Data API.

    Uses the public Data API (https://data.api.drift.trade/stats/markets)
    which requires no auth and returns all 137 markets (74 perp + 63 spot).
    Each Drift perp market produces one instrument with instrument_type="perpetual".
    Drift settles in USDC.
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
        return f"DRIFT-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Drift."""
        error_code = _classify_drift_error(exc)
        classification = classify_venue_error("drift", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Drift %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch active Drift perp and spot markets as instruments."""
        markets = await self._fetch_all_markets()
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []

        for market in markets:
            market_type = str(market.get("marketType", ""))
            status = str(market.get("status", ""))
            if status != "active":
                continue

            if market_type == "perp" and instrument_type in (None, "perpetual"):
                record = self._build_perp_record(market, now)
                if record:
                    results.append(record)
            elif market_type == "spot" and instrument_type in (None, "spot"):
                record = self._build_spot_record(market, now)
                if record:
                    results.append(record)

        logger.info("Drift: fetched %d instruments on %s", len(results), self._chain)
        return results

    async def _fetch_all_markets(self) -> list[dict[str, object]]:
        """Fetch all markets from Drift Data API /stats/markets (public, no auth)."""
        url = f"{_DATA_API_URL}/stats/markets"
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "stats/markets")
            else:
                logger.error("Drift stats/markets request failed after retries: %s", exc)
            return []

        raw: dict[str, object] = data if isinstance(data, dict) else {}
        markets = raw.get("markets")
        if isinstance(markets, list):
            return markets
        return []

    def _build_perp_record(
        self,
        market: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Drift perp market."""
        symbol = str(market.get("symbol", ""))
        if not symbol:
            return None

        base_asset = symbol.split("-")[0].upper() if "-" in symbol else symbol.upper()
        if base_asset not in DEFI_MAJOR_ASSET_SYMBOLS:
            return None

        venue_tag = self.venue
        instrument_key = f"{venue_tag}:PERP:{symbol.upper()}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            symbol=symbol.upper(),
            raw_symbol=symbol,
            instrument_type="perpetual",
            base_asset=base_asset,
            quote_asset="USDC",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.001"),
            min_order_size=Decimal("0"),
            contract_size=Decimal("1"),
            settlement_asset="USDC",
            expiry=None,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
        )

    def _build_spot_record(
        self,
        market: dict[str, object],
        now: datetime,
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Drift spot market."""
        symbol = str(market.get("symbol", ""))
        base_asset = str(market.get("baseAsset", symbol)).upper()
        if not base_asset or base_asset not in DEFI_MAJOR_ASSET_SYMBOLS:
            return None

        venue_tag = self.venue
        instrument_key = f"{venue_tag}:SPOT:{base_asset}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            symbol=base_asset,
            raw_symbol=symbol or base_asset,
            instrument_type="spot",
            base_asset=base_asset,
            quote_asset="USDC",
            tick_size=Decimal("0.0001"),
            lot_size=Decimal("0.001"),
            min_order_size=Decimal("0"),
            contract_size=Decimal("1"),
            settlement_asset="USDC",
            expiry=None,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.symbol == symbol:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Drift does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Drift markets have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Drift funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Drift OHLCV not supported via reference data")
