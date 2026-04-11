"""Uniswap V2 reference data adapter — instrument discovery via The Graph.

Discovers Uniswap V2 liquidity pairs on Ethereum.
Pairs are returned as InstrumentRecord with instrument_type="POOL".

Data source: The Graph (decentralized network).
Reference: https://docs.uniswap.org/contracts/v2/overview
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import SUBGRAPH_IDS
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils import date_to_block
from ...utils.defi_utils import classify_graph_error, order_base_quote, parse_created_timestamp

logger = logging.getLogger(__name__)

_SUBGRAPH_IDS: dict[str, str] = SUBGRAPH_IDS.get("uniswap_v2", {})
_DEFAULT_CHAIN = "ETHEREUM"

# Query template — {block_clause} is replaced with '' or ', block: {number: N}'
_PAIRS_QUERY_TEMPLATE = """
query GetPairs($first: Int!, $minReserve: BigDecimal!) {{
    pairs(
        first: $first, orderBy: reserveUSD, orderDirection: desc,
        where: {{ reserveUSD_gt: $minReserve }}{block_clause}
    ) {{
        id
        token0 {{ id symbol name decimals }}
        token1 {{ id symbol name decimals }}
        reserveUSD
        createdAtTimestamp
    }}
}}
"""

_FETCH_LIMIT = 1000


class UniswapV2ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Uniswap V2 reference data: pair discovery from The Graph subgraph."""

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
        return "uniswap_v2"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Uniswap V2 pairs as instruments."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f", block: {{number: {block_num}}}" if block_num else ""
        query = _PAIRS_QUERY_TEMPLATE.format(block_clause=block_clause)

        variables = {"first": _FETCH_LIMIT, "minReserve": "100000"}
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.post(
                    url,
                    json={"query": query, "variables": variables},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            self._log_fetch_error(exc)
            raise ConnectionError(str(exc)) from exc

        pairs: list[dict[str, object]] = (data.get("data") or {}).get("pairs") or []

        results: list[InstrumentRecord] = []

        for pair in pairs:
            record = self._build_pair_record(pair)
            if record:
                results.append(record)

        logger.info("UniswapV2: fetched %d pair instruments on %s", len(results), self._chain)
        return results

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        subgraph_id = _SUBGRAPH_IDS.get(self._chain, "")
        if not api_key or not subgraph_id:
            logger.warning(
                "UniswapV2: missing API key or subgraph ID for chain %s",
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
                "UniswapV2: querying at historical block %d for date %s",
                block_num,
                self._date,
            )
        return block_num

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Uniswap V2."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error("uniswap_v2", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "UniswapV2 pair query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "uniswap_v2",
                "endpoint": "thegraph_pairs",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    def _build_pair_record(
        self,
        pair: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a single Uniswap V2 pair, or None."""
        pair_id = pair.get("id")
        token0 = pair.get("token0")
        token1 = pair.get("token1")
        if not pair_id or not isinstance(token0, dict) or not isinstance(token1, dict):
            return None

        sym0 = str(token0.get("symbol", ""))
        sym1 = str(token1.get("symbol", ""))
        if not sym0 or not sym1:
            return None

        base, quote = order_base_quote(sym0, sym1)

        symbol = f"{base}-{quote}"
        venue_tag = f"UNISWAPV2-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pair.get("createdAtTimestamp"))

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=str(pair_id),
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
        raise NotImplementedError("Uniswap V2 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Uniswap V2 pairs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Uniswap V2 pairs have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Uniswap V2 OHLCV not supported via reference data")
