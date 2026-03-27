"""Balancer reference data adapter — instrument discovery via Balancer API v3.

Discovers Balancer liquidity pools on Ethereum via the public GraphQL API.
Pools are returned as InstrumentRecord with instrument_type="pool".

Data source: Balancer API v3 (api-v3.balancer.fi/graphql)
Reference: https://docs.balancer.fi/
No API key required — public endpoint.
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import DEFI_MAJOR_ASSET_SYMBOLS, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ..utils.defi_utils import classify_graph_error, parse_created_timestamp

logger = logging.getLogger(__name__)

_BALANCER_API = "https://api-v3.balancer.fi/graphql"
_DEFAULT_CHAIN = "ETHEREUM"

_POOLS_QUERY = """
query GetPools($chain: [GqlChain!]!) {
  poolGetPools(
    first: 500
    skip: 0
    orderBy: totalLiquidity
    orderDirection: desc
    where: {
      chainIn: $chain
    }
  ) {
    id
    name
    type
    address
    chain
    protocolVersion
    createTime
    poolTokens {
      address
      symbol
    }
    dynamicData {
      totalLiquidity
    }
  }
}
"""

_CHAIN_TO_GQL = {
    "ETHEREUM": "MAINNET",
    "ARBITRUM": "ARBITRUM",
    "BASE": "BASE",
    "POLYGON": "POLYGON",
    "OPTIMISM": "OPTIMISM",
    "GNOSIS": "GNOSIS",
    "AVALANCHE": "AVALANCHE",
}


class BalancerReferenceDataAdapter(BaseReferenceDataAdapter):
    """Balancer reference data: pool discovery from Balancer API v3 (GraphQL).

    Uses the public api-v3.balancer.fi endpoint. No API key required.
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
        return "balancer"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Balancer pools as instruments."""
        if instrument_type not in (None, "pool"):
            return []

        gql_chain = _CHAIN_TO_GQL.get(self._chain, "MAINNET")

        try:
            payload = {
                "query": _POOLS_QUERY,
                "variables": {"chain": [gql_chain]},
            }
            async with (
                aiohttp.ClientSession() as session,
                session.post(_BALANCER_API, json=payload) as resp,
            ):
                resp.raise_for_status()
                raw = await resp.json()
        except aiohttp.ClientError as exc:
            error_code = classify_graph_error(exc)
            classification = classify_venue_error("balancer", error_code)
            action = classification.action.value if classification else "fail"
            retry_safe = classification.retry_safe if classification else False
            logger.error(
                "Balancer API request failed: %s (classified: %s, action: %s, retry_safe: %s)",
                exc,
                error_code,
                action,
                retry_safe,
            )
            log_event(
                "ADAPTER_FETCH_FAILED",
                details={
                    "venue": "balancer",
                    "endpoint": "poolGetPools",
                    "error": str(exc),
                    "error_code": error_code,
                    "action": action,
                    "retry_safe": retry_safe,
                },
            )
            return []

        pools = raw.get("data", {}).get("poolGetPools", [])
        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []

        for pool in pools:
            record = self._pool_to_record(pool, now)
            if record is not None:
                results.append(record)

        logger.info("Balancer: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    def _pool_to_record(self, pool: dict[str, object], now: datetime) -> InstrumentRecord | None:
        """Convert a raw Balancer pool dict to an InstrumentRecord, or None if filtered."""
        pool_address = pool.get("address")
        pool_name = str(pool.get("name", ""))
        tokens = pool.get("poolTokens", [])
        if not pool_address or not isinstance(tokens, list) or len(tokens) < 2:
            return None

        sym0 = str(tokens[0].get("symbol", "UNKNOWN")).upper()
        sym1 = str(tokens[1].get("symbol", "UNKNOWN")).upper()

        # Filter: all pool tokens must be major assets (BTC/ETH/stablecoins)
        token_symbols = [str(t.get("symbol", "")).upper() for t in tokens]
        if not all(s in DEFI_MAJOR_ASSET_SYMBOLS for s in token_symbols):
            return None

        symbol = f"{sym0}-{sym1}"
        venue_tag = f"BALANCER-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pool.get("createTime"))

        return InstrumentRecord(
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
            available_since=available_since,
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.symbol == symbol:
                return inst
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        raise NotImplementedError("Balancer does not support options")

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "future") -> CanonicalExpiryCalendar:
        raise NotImplementedError("Balancer pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Balancer pools have no funding rate")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        raise NotImplementedError("Balancer OHLCV not supported via reference data")
