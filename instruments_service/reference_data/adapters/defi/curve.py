"""Curve reference data adapter — instrument discovery via Curve REST API.

Discovers Curve liquidity pools across supported chains. Pools are returned as
InstrumentRecord with instrument_type="POOL".

Data source: Curve REST API (api.curve.finance).
Reference: https://curve.fi/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import classify_graph_error

logger = logging.getLogger(__name__)

_CURVE_API_TEMPLATE = "https://api.curve.finance/v1/getPools/{chain_slug}/main"
_DEFAULT_CHAIN = "ETHEREUM"

# Chain name → Curve API slug mapping
_CHAIN_SLUG: dict[str, str] = {
    "ETHEREUM": "ethereum",
    "AVALANCHE": "avalanche",
    "OPTIMISM": "optimism",
    "ARBITRUM": "arbitrum",
    "POLYGON": "polygon",
    "BASE": "base",
    "FANTOM": "fantom",
}

# Per-chain deploy dates for Curve — used as available_since floor.
# Sourced from UAC VenueMapping venue_start_dates (SSOT).
_CURVE_DEPLOY_DATES: dict[str, datetime] = {
    "ETHEREUM": datetime(2020, 1, 20, tzinfo=UTC),
    "AVALANCHE": datetime(2021, 11, 10, tzinfo=UTC),
    "OPTIMISM": datetime(2022, 1, 13, tzinfo=UTC),
}


class CurveReferenceDataAdapter(BaseReferenceDataAdapter):
    """Curve reference data: pool discovery from Curve REST API.

    Uses the public api.curve.fi endpoint to discover pools. No API key required.
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
        return "curve"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Curve pools as instruments for the configured chain."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        chain_slug = _CHAIN_SLUG.get(self._chain)
        if not chain_slug:
            logger.warning("Curve: unsupported chain %s, skipping", self._chain)
            return []

        api_url = _CURVE_API_TEMPLATE.format(chain_slug=chain_slug)

        try:
            async with self._make_session() as session, session.get(api_url) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = classify_graph_error(exc)
            classification = classify_venue_error("curve", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Curve API request failed for %s: %s (classified: %s, action: %s, retry_safe: %s)",
                self._chain,
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": f"CURVE-{self._chain}",
                    "endpoint": "getPools",
                    "chain": self._chain,
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            raise ConnectionError(str(exc)) from exc

        pool_data: list[dict[str, object]] = raw.get("data", {}).get("poolData", [])
        results: list[InstrumentRecord] = []

        deploy_date = _CURVE_DEPLOY_DATES.get(self._chain, datetime(2020, 1, 20, tzinfo=UTC))

        for pool in pool_data:
            pool_address = pool.get("address")
            pool_name = str(pool.get("name", ""))
            coins = pool.get("coins")
            if not pool_address or not isinstance(coins, list) or len(coins) < 2:
                continue

            coin0 = coins[0] if isinstance(coins[0], dict) else {}
            coin1 = coins[1] if isinstance(coins[1], dict) else {}
            sym0 = str(coin0.get("symbol", "UNKNOWN")).upper()
            sym1 = str(coin1.get("symbol", "UNKNOWN")).upper()

            symbol = f"{sym0}-{sym1}"
            venue_tag = f"CURVE-{self._chain}"
            instrument_key = f"{venue_tag}:POOL:{symbol}"

            results.append(
                InstrumentRecord(
                    instrument_key=instrument_key,
                    venue=venue_tag,
                    raw_symbol=str(pool_address),
                    instrument_type=InstrumentType.POOL,
                    base_asset=sym0,
                    quote_asset=sym1,
                    tick_size=Decimal("0.000001"),
                    min_size=Decimal("0.000001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    underlying=pool_name if pool_name else None,
                    available_from_datetime=deploy_date,
                )
            )

        logger.info("Curve: fetched %d pool instruments on %s", len(results), self._chain)
        return results

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
        raise NotImplementedError("Curve does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Curve pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Curve pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Curve OHLCV not supported via reference data")
