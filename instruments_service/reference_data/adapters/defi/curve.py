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


def _parse_decimals(value: object) -> int | None:
    """Parse Curve API decimals (string or int) into an int, or None."""
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _optional_str(value: object) -> str | None:
    """Return non-empty string or None."""
    if value is None:
        return None
    s = str(value)
    return s or None


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
        """Return the venue identifier."""
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

        # DeFi-plan A8 / operator 2026-05-07: Curve REST returns {"success": true, "data": {"poolData": [...]}}.
        # A soft 200 with success=false / a missing-`data` body is a TRANSIENT FETCH FAILURE, not an empty
        # pool universe — raise so discovery records it attempted_failed rather than dropping the venue/day
        # from the coverage denominator. A legit empty universe is {"data": {"poolData": []}} → returns [].
        if not isinstance(raw, dict) or raw.get("success") is False or not isinstance(raw.get("data"), dict):
            raise ConnectionError(f"CURVE-{self._chain}: malformed getPools response — transient fetch failure")
        _curve_data: dict[str, object] = raw["data"]
        pool_data_val: object = _curve_data.get("poolData", [])
        pool_data: list[dict[str, object]] = pool_data_val if isinstance(pool_data_val, list) else []
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

            # Curve does not advertise a pool-level fee tier in the public
            # REST envelope (per-pool fees vary; require on-chain fee()
            # / future API surfacing). Leave pool_fee_tier=None.
            try:
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
                        pool_address=str(pool_address),
                        pool_fee_tier=None,
                        base_asset_contract_address=_optional_str(coin0.get("address")),
                        base_asset_decimals=_parse_decimals(coin0.get("decimals")),
                        base_asset_symbol_onchain=_optional_str(coin0.get("symbol")),
                        quote_asset_contract_address=_optional_str(coin1.get("address")),
                        quote_asset_decimals=_parse_decimals(coin1.get("decimals")),
                        quote_asset_symbol_onchain=_optional_str(coin1.get("symbol")),
                    )
                )
            except Exception as exc:
                logger.warning("Curve: skipping pool %s — %s", pool_address, exc)

        logger.info("Curve: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.instrument_key.endswith(f":{symbol}"):
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Curve does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Curve pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Curve pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Curve OHLCV not supported via reference data")
