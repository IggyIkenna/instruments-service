"""Uniswap V3 reference data adapter — instrument discovery via The Graph.

Discovers Uniswap V3 liquidity pools across Ethereum, Arbitrum, and Base.
Pools are returned as InstrumentRecord with instrument_type="POOL".

Data source: The Graph (decentralized network).
Reference: https://docs.uniswap.org/contracts/v3/overview
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

# Uniswap V3 subgraph IDs from UAC registry
_SUBGRAPH_IDS: dict[str, str] = SUBGRAPH_IDS.get("uniswap_v3", {})

# Default chain
_DEFAULT_CHAIN = "ETHEREUM"

# Query template — {block_clause} is replaced with '' or ', block: {number: N}'
_POOLS_QUERY_TEMPLATE = """
query GetPools($first: Int!, $skip: Int!) {{
    pools(
        first: $first, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc
        {block_clause}
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
# The Graph caps skip at 5000 — max reachable = 5000 + 1000 = 6000 pools
_MAX_SKIP = 5000

# Algebra fork schema (Camelot V3, etc.) — no feeTier, uses directional feeZtO/feeOtZ
_ALGEBRA_POOLS_QUERY_TEMPLATE = """
query GetPools($first: Int!, $skip: Int!) {{
    pools(
        first: $first, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc
        {block_clause}
    ) {{
        id
        feeZtO
        feeOtZ
        token0 {{ id symbol name decimals }}
        token1 {{ id symbol name decimals }}
        totalValueLockedUSD
        createdAtTimestamp
    }}
}}
"""

# SushiSwap V3 custom schema — uses `pairs` entity instead of `pools`
_SUSHISWAP_PAIRS_QUERY = """
query GetPairs($first: Int!) {
    pairs(
        first: $first, orderBy: liquidityUSD, orderDirection: desc
    ) {
        id
        token0 { id symbol name decimals }
        token1 { id symbol name decimals }
        liquidityUSD
        createdAtTimestamp
    }
}
"""

# Messari-standard subgraph schema (used by some chain deployments e.g. Base)
_MESSARI_POOLS_QUERY = """
query GetPools($first: Int!) {
    liquidityPools(
        first: $first, orderBy: totalValueLockedUSD, orderDirection: desc
    ) {
        id
        name
        inputTokens { id symbol name decimals }
        fees { feePercentage feeType }
        totalValueLockedUSD
        createdTimestamp
    }
}
"""


class UniswapV3ReferenceDataAdapter(BaseReferenceDataAdapter):
    """Uniswap V3 reference data: pool discovery from The Graph subgraph."""

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
        date: str | None = None,
        protocol_slug: str | None = None,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()
        self._date = date
        self._protocol_slug = protocol_slug or "uniswap_v3"
        # Convert protocol slug (e.g. "pancakeswap_v3") to UAC venue prefix (e.g. "PANCAKESWAP_V3")
        self._venue_prefix = self._protocol_slug.replace("_", "").upper()

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._protocol_slug

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Uniswap V3 pools as instruments."""
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        block_num = await self._resolve_block_num()
        block_clause = f", block: {{number: {block_num}}}" if block_num else ""
        query = _POOLS_QUERY_TEMPLATE.format(block_clause=block_clause)

        # Paginate through all pools (The Graph caps skip at 5000)
        all_pools: list[dict[str, object]] = []
        skip = 0
        schema_error = False
        async with self._make_session() as session:
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

                # Check for GraphQL errors (schema mismatches, indexer issues)
                resp_errors: list[dict[str, object]] = data.get("errors", []) if isinstance(data, dict) else []
                for err in resp_errors:
                    msg = str(err.get("message", "")).lower()
                    if "has no field" in msg or "no field" in msg:
                        schema_error = True
                    if "bad indexers" in msg or "unavailable" in msg or "too far behind" in msg:
                        logger.warning(
                            "%s: subgraph indexers unavailable on %s — infrastructure issue, skipping",
                            self._protocol_slug,
                            self._chain,
                        )
                        return []

                if not isinstance(data, dict) or not data.get("data"):
                    if skip == 0 and not schema_error:
                        logger.warning("UniswapV3: empty response from %s subgraph", self._chain)
                    break

                raw_data = data.get("data") or {}
                page: list[dict[str, object]] = raw_data.get("pools") or []
                if not page:
                    break

                all_pools.extend(page)
                logger.debug("UniswapV3: fetched page skip=%d, got %d pools", skip, len(page))

                if len(page) < _FETCH_LIMIT:
                    break  # last page
                skip += _FETCH_LIMIT

        # Fallback 1: Algebra fork schema (Camelot V3) — pools entity, no feeTier
        if not all_pools and schema_error:
            all_pools = await self._fetch_algebra_pools(url, block_num)

        # Fallback 2: SushiSwap custom schema — pairs entity instead of pools
        if not all_pools:
            all_pools = await self._fetch_sushiswap_pairs(url)

        # Fallback 3: Messari-schema subgraphs use 'liquidityPools' + 'inputTokens'
        if not all_pools:
            all_pools = await self._fetch_messari_pools(url)

        results: list[InstrumentRecord] = []

        for pool in all_pools:
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("UniswapV3: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    async def _fetch_messari_pools(self, url: str) -> list[dict[str, object]]:
        """Fetch pools from Messari-schema subgraph and normalise to official format."""
        try:
            async with (
                self._make_session() as session,
                session.post(
                    url,
                    json={"query": _MESSARI_POOLS_QUERY, "variables": {"first": 1000}},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.warning("UniswapV3 Messari fallback failed on %s: %s", self._chain, exc)
            return []

        if not isinstance(data, dict) or "data" not in data:
            return []

        raw_pools = data.get("data", {}).get("liquidityPools", [])
        normalised: list[dict[str, object]] = []
        for lp in raw_pools:
            tokens = lp.get("inputTokens", [])
            if len(tokens) < 2:
                continue
            fees = lp.get("fees", [])
            fee_pct = next((f["feePercentage"] for f in fees if f.get("feeType") == "FIXED_TRADING_FEE"), "0.3")
            fee_tier = str(int(float(fee_pct) * 10000))
            normalised.append(
                {
                    "id": lp.get("id"),
                    "token0": tokens[0],
                    "token1": tokens[1],
                    "feeTier": fee_tier,
                    "totalValueLockedUSD": lp.get("totalValueLockedUSD", "0"),
                    "createdAtTimestamp": lp.get("createdTimestamp"),
                }
            )
        logger.info("UniswapV3: Messari fallback found %d pools on %s", len(normalised), self._chain)
        return normalised

    async def _fetch_algebra_pools(self, url: str, block_num: int | None) -> list[dict[str, object]]:
        """Fetch pools from Algebra-fork subgraphs (Camelot V3) — no feeTier field."""
        block_clause = f", block: {{number: {block_num}}}" if block_num else ""
        query = _ALGEBRA_POOLS_QUERY_TEMPLATE.format(block_clause=block_clause)
        all_pools: list[dict[str, object]] = []
        skip = 0
        try:
            async with self._make_session() as session:
                while skip <= _MAX_SKIP:
                    variables = {"first": _FETCH_LIMIT, "skip": skip}
                    async with session.post(
                        url,
                        json={"query": query, "variables": variables},
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                    if not isinstance(data, dict) or "data" not in data:
                        break

                    page: list[dict[str, object]] = (data.get("data") or {}).get("pools") or []
                    if not page:
                        break

                    # Normalise Algebra fee fields to feeTier (average of directional fees)
                    for pool in page:
                        fee_zto = int(str(pool.get("feeZtO", "0") or "0"))
                        fee_otz = int(str(pool.get("feeOtZ", "0") or "0"))
                        pool["feeTier"] = str((fee_zto + fee_otz) // 2)

                    all_pools.extend(page)
                    if len(page) < _FETCH_LIMIT:
                        break
                    skip += _FETCH_LIMIT
        except aiohttp.ClientError as exc:
            logger.warning("%s Algebra fallback failed on %s: %s", self._protocol_slug, self._chain, exc)
            return []

        if all_pools:
            logger.info("%s: Algebra fallback found %d pools on %s", self._protocol_slug, len(all_pools), self._chain)
        return all_pools

    async def _fetch_sushiswap_pairs(self, url: str) -> list[dict[str, object]]:
        """Fetch pairs from SushiSwap-custom subgraphs — uses `pairs` entity."""
        try:
            async with (
                self._make_session() as session,
                session.post(
                    url,
                    json={"query": _SUSHISWAP_PAIRS_QUERY, "variables": {"first": 1000}},
                    headers={"Content-Type": "application/json"},
                ) as resp,
            ):
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            logger.warning("%s SushiSwap pairs fallback failed on %s: %s", self._protocol_slug, self._chain, exc)
            return []

        if not isinstance(data, dict) or "data" not in data:
            return []

        raw_pairs: list[dict[str, object]] = (data.get("data") or {}).get("pairs") or []
        # Normalise to same shape as native pools query (feeTier, totalValueLockedUSD)
        normalised: list[dict[str, object]] = []
        for pair in raw_pairs:
            normalised.append(
                {
                    "id": pair.get("id"),
                    "token0": pair.get("token0"),
                    "token1": pair.get("token1"),
                    "feeTier": "3000",  # SushiSwap V3 default 0.3%
                    "totalValueLockedUSD": pair.get("liquidityUSD", "0"),
                    "createdAtTimestamp": pair.get("createdAtTimestamp"),
                }
            )
        if normalised:
            logger.info(
                "%s: SushiSwap pairs fallback found %d pools on %s",
                self._protocol_slug,
                len(normalised),
                self._chain,
            )
        return normalised

    def _resolve_api_url(self) -> str | None:
        """Return the subgraph URL or None if API key / subgraph ID is missing."""
        api_key = self._optional_api_key()
        # Look up subgraph ID for this protocol slug (supports forks like PancakeSwap, SushiSwap)
        protocol_ids = SUBGRAPH_IDS.get(self._protocol_slug, _SUBGRAPH_IDS)
        subgraph_id = protocol_ids.get(self._chain, "")
        if not api_key or not subgraph_id:
            logger.warning(
                "%s: missing API key or subgraph ID for chain %s",
                self._protocol_slug,
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
                "UniswapV3: querying at historical block %d for date %s",
                block_num,
                self._date,
            )
        return block_num

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Uniswap V3."""
        error_code = classify_graph_error(exc)
        classification = classify_venue_error("uniswap_v3", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "UniswapV3 pool query failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": "uniswap_v3",
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
        """Build an InstrumentRecord from a single Uniswap V3 pool, or None."""
        pool_id = pool.get("id")
        token0 = pool.get("token0") or {}
        token1 = pool.get("token1") or {}
        fee_tier = pool.get("feeTier")
        if not pool_id or not isinstance(token0, dict) or not isinstance(token1, dict):
            return None

        sym0 = str(token0.get("symbol", ""))
        sym1 = str(token1.get("symbol", ""))
        if not sym0 or not sym1:
            return None

        base, quote = order_base_quote(sym0, sym1)
        # Determine which raw token corresponds to base vs quote after canonical
        # ordering (order_base_quote may swap sym0 / sym1).
        if base == sym0:
            base_token, quote_token = token0, token1
        else:
            base_token, quote_token = token1, token0

        fee_str = str(fee_tier) if fee_tier else "0"
        symbol = f"{base}-{quote}:{fee_str}"
        venue_tag = f"{self._venue_prefix}-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pool.get("createdAtTimestamp"))

        # Uniswap V3 feeTier is in hundredths-of-bps (e.g. 500 = 0.05%, 3000 = 0.3%).
        # Plan asks for basis points: feeTier / 100 (e.g. 3000 → 30 bps).
        pool_fee_tier_bps = self._parse_fee_tier_bps(fee_tier)
        base_decimals = self._parse_decimals(base_token.get("decimals"))
        quote_decimals = self._parse_decimals(quote_token.get("decimals"))
        if base_decimals is None or quote_decimals is None:
            logger.warning("UniswapV3: skipping pool %s — missing token decimals", pool_id)
            return None
        base_addr = self._parse_address(base_token.get("id"))
        quote_addr = self._parse_address(quote_token.get("id"))
        base_sym_onchain = self._parse_optional_str(base_token.get("symbol"))
        quote_sym_onchain = self._parse_optional_str(quote_token.get("symbol"))

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
            pool_address=str(pool_id),
            pool_fee_tier=pool_fee_tier_bps,
            base_asset_contract_address=base_addr,
            base_asset_decimals=base_decimals,
            base_asset_symbol_onchain=base_sym_onchain,
            quote_asset_contract_address=quote_addr,
            quote_asset_decimals=quote_decimals,
            quote_asset_symbol_onchain=quote_sym_onchain,
        )

    @staticmethod
    def _parse_fee_tier_bps(fee_tier: object) -> int | None:
        """Convert subgraph feeTier (hundredths-of-bps) to bps. None for missing/zero."""
        if fee_tier is None:
            return None
        try:
            raw = int(str(fee_tier))
        except (TypeError, ValueError):
            return None
        if raw <= 0:
            return None
        return raw // 100

    @staticmethod
    def _parse_decimals(value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_address(value: object) -> str | None:
        if value is None:
            return None
        addr = str(value)
        return addr or None

    @staticmethod
    def _parse_optional_str(value: object) -> str | None:
        if value is None:
            return None
        s = str(value)
        return s or None

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
        raise NotImplementedError("Uniswap V3 does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Uniswap V3 pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Uniswap V3 pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Uniswap V3 OHLCV not supported via reference data")
