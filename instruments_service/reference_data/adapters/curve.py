"""Curve reference data adapter — instrument discovery via Curve REST API.

Discovers Curve liquidity pools on Ethereum. Pools are returned as
InstrumentRecord with instrument_type="pool".

Data source: Curve REST API (api.curve.fi).
Reference: https://curve.fi/
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
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

_CURVE_API = "https://api.curve.fi/v1/getPools/ethereum/main"
_DEFAULT_CHAIN = "ETHEREUM"

# Curve Finance Ethereum mainnet deployment date (2020-01-20).
# The Curve REST API does not include per-pool creation timestamps,
# so we use the protocol launch date as the available_since floor for all pools.
_CURVE_DEPLOY_DATE = datetime(2020, 1, 20, tzinfo=UTC)


def _classify_error(exc: Exception, status: int | None = None) -> str:
    if status == 429:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "503"
    msg = str(exc).lower()
    if "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if "503" in msg or "unavailable" in msg:
        return "503"
    return "UNKNOWN"


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
        """Fetch active Curve pools as instruments."""
        if instrument_type not in (None, "pool"):
            return []

        try:
            async with aiohttp.ClientSession() as session, session.get(_CURVE_API) as resp:
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = _classify_error(exc)
            classification = classify_venue_error("curve", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Curve API request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "curve",
                    "endpoint": "getPools",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        pool_data: list[dict[str, object]] = raw.get("data", {}).get("poolData", [])
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []

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
                    symbol=f"{sym0}/{sym1}",
                    raw_symbol=str(pool_address),
                    instrument_type="pool",
                    base_asset=sym0,
                    quote_asset=sym1,
                    tick_size=Decimal("0.000001"),
                    lot_size=Decimal("0.000001"),
                    min_order_size=Decimal("0"),
                    contract_size=Decimal("1"),
                    settlement_asset=sym1,
                    expiry=None,
                    strike=None,
                    option_type=None,
                    is_active=True,
                    updated_at=now,
                    underlying=pool_name if pool_name else None,
                    available_since=_CURVE_DEPLOY_DATE,
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
        instrument_type: str = "future",
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
