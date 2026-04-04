"""Uniswap V4 reference data adapter — instrument discovery via The Graph.

Discovers Uniswap V4 liquidity pools on Ethereum.
Pools are returned as InstrumentRecord with instrument_type="POOL".

Data source: The Graph (decentralized network).
Reference: https://docs.uniswap.org/contracts/v4/overview
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import SUBGRAPH_IDS
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ..utils import date_to_block
from ..utils.defi_utils import classify_graph_error, order_base_quote, parse_created_timestamp

logger = logging.getLogger(__name__)

_SUBGRAPH_IDS: dict[str, str] = SUBGRAPH_IDS.get("uniswap_v4", {})
_DEFAULT_CHAIN = "ETHEREUM"

# Query template — {block_clause} is replaced with '' or ' block: {number: N},'
_POOLS_QUERY_TEMPLATE = """
query GetPools($first: Int!, $skip: Int!) {{
    pools(
        {block_clause}first: $first, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc
    ) {{
        id
        feeTier
        token0 {{ id symbol name decimals }}
        token1 {{ id symbol name decimals }}
        totalValueLockedUSD
        createdAtTimestamp
    }}
}}
"""

_FETCH_LIMIT = 1000
_MAX_SKIP = 5000


class UniswapV4ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Uniswap V4 reference data: pool discovery from The Graph subgraph.

    Uniswap V4 launched February 2025. Protocol is young — all pools with
    activity are worth tracking.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
        date: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()
        self._date = date

    @property
    def venue(self) -> str:
        return "uniswap_v4"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Uniswap V4 pools as instruments."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f"block: {{number: {block_num}}}, " if block_num else ""
        query = _POOLS_QUERY_TEMPLATE.format(block_clause=block_clause)

        # Paginate through all pools (The Graph caps skip at 5000)
        all_pools: list[dict[str, object]] = []
        skip = 0
        async with aiohttp.ClientSession() as session:
            while skip <= _MAX_SKIP:
                variables = {"first": _FETCH_LIMIT, "skip": skip}
                try:
                    async with session.post(
                        url,
                        json={"query": query, "variables": variables},
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()
                except aiohttp.ClientError as exc:
                    self._log_fetch_error(exc)
                    raise ConnectionError(str(exc)) from exc

                page: list[dict[str, object]] = (data.get("data") or {}).get("pools") or []
                if not page:
                    break

                all_pools.extend(page)
                logger.debug("UniswapV4: fetched page skip=%d, got %d pools", skip, len(page))

                if len(page) < _FETCH_LIMIT:
                    break  # last page
                skip += _FETCH_LIMIT

        results: list[InstrumentRecord] = []

        for pool in all_pools:
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("UniswapV4: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        subgraph_id = _SUBGRAPH_IDS.get(self._chain, "")
        if not api_key or not subgraph_id:
            logger.warning(
                "UniswapV4: missing API key or subgraph ID for chain %s",
                self._chain,
            )
            return None
        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"

    async def _resolve_block_num(self) -> int | None:
        """Resolve the historical block number from self._date, if set."""
        if not self._date:
            return None
        block_num = await date_to_block(self._date, chain=self._chain)
        if block_num:
            logger.debug(
                "UniswapV4: querying at historical block %d for date %s",
                block_num,
                self._date,
            )
        return block_num

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Uniswap V4."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error("uniswap_v4", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "UniswapV4 pool query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "uniswap_v4",
                "endpoint": "thegraph_pools",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    def _build_pool_record(
        self,
        pool: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a single Uniswap V4 pool, or None."""
        pool_id = pool.get("id")
        token0 = pool.get("token0")
        token1 = pool.get("token1")
        fee_tier = pool.get("feeTier")
        if not pool_id or not isinstance(token0, dict) or not isinstance(token1, dict):
            return None

        sym0 = str(token0.get("symbol", ""))
        sym1 = str(token1.get("symbol", ""))
        if not sym0 or not sym1:
            return None

        base, quote = order_base_quote(sym0, sym1)

        fee_str = str(fee_tier) if fee_tier else "0"
        symbol = f"{base}-{quote}:{fee_str}"
        venue_tag = f"UNISWAPV4-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pool.get("createdAtTimestamp"))

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=str(pool_id),
            instrument_type=InstrumentType.POOL,
            base_asset=base,
            quote_asset=quote,
            tick_size=Decimal("0.000001"),
            min_size=Decimal("0.000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=available_since,
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
        raise NotImplementedError("Uniswap V4 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Uniswap V4 pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Uniswap V4 pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Uniswap V4 OHLCV not supported via reference data")
