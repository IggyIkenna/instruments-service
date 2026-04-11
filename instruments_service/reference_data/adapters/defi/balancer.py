"""Balancer reference data adapter — instrument discovery via Balancer API v3.

Discovers Balancer liquidity pools on Ethereum via the public GraphQL API.
Pools are returned as InstrumentRecord with instrument_type="POOL".

Data source: Balancer API v3 (api-v3.balancer.fi/graphql)
Reference: https://docs.balancer.fi/
No API key required — public endpoint.
"""

import logging
from datetime import datetime
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
from ...utils.defi_utils import classify_graph_error, parse_created_timestamp

logger = logging.getLogger(__name__)

_BALANCER_API = "https://api-v3.balancer.fi/graphql"
_DEFAULT_CHAIN = "ETHEREUM"

_POOLS_QUERY = """
query GetPools($chain: [GqlChain!]!, $first: Int!, $skip: Int!) {
  poolGetPools(
    first: $first
    skip: $skip
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

_PAGE_SIZE = 1000
_MAX_SKIP = 5000

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
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        gql_chain = _CHAIN_TO_GQL.get(self._chain, "MAINNET")

        all_pools: list[dict[str, object]] = []
        skip = 0
        async with aiohttp.ClientSession() as session:
            while skip <= _MAX_SKIP:
                try:
                    payload = {
                        "query": _POOLS_QUERY,
                        "variables": {"chain": [gql_chain], "first": _PAGE_SIZE, "skip": skip},
                    }
                    async with session.post(_BALANCER_API, json=payload) as resp:
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
                    raise ConnectionError(str(exc)) from exc

                page = raw.get("data", {}).get("poolGetPools", [])
                if not page:
                    break

                all_pools.extend(page)
                logger.debug("Balancer: fetched page skip=%d, got %d pools", skip, len(page))

                if len(page) < _PAGE_SIZE:
                    break
                skip += _PAGE_SIZE

        results: list[InstrumentRecord] = []

        for pool in all_pools:
            record = self._pool_to_record(pool)
            if record is not None:
                results.append(record)

        logger.info("Balancer: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    def _pool_to_record(self, pool: dict[str, object]) -> InstrumentRecord | None:
        """Convert a raw Balancer pool dict to an InstrumentRecord, or None if filtered."""
        pool_address = pool.get("address")
        pool_name = str(pool.get("name", ""))
        tokens = pool.get("poolTokens", [])
        if not pool_address or not isinstance(tokens, list) or len(tokens) < 2:
            return None

        sym0 = str(tokens[0].get("symbol", "UNKNOWN")).upper()
        sym1 = str(tokens[1].get("symbol", "UNKNOWN")).upper()

        symbol = f"{sym0}-{sym1}"
        venue_tag = f"BALANCER-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pool.get("createTime"))

        return InstrumentRecord(
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
            available_from_datetime=available_since,
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        instruments = await self.get_instruments()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.symbol == symbol:
                return inst
        return None

    async def get_options_chain(self, underlying: str, expiry: datetime | None = None) -> CanonicalOptionsChain:
        raise NotImplementedError("Balancer does not support options")

    async def get_expiry_calendar(self, underlying: str, instrument_type: str = "FUTURE") -> CanonicalExpiryCalendar:
        raise NotImplementedError("Balancer pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Balancer pools have no funding rate")

    async def get_ohlcv(self, symbol: str, interval: str = "1d", limit: int = 100) -> list[OHLCVRef]:
        raise NotImplementedError("Balancer OHLCV not supported via reference data")
