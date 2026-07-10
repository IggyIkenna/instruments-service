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
from unified_api_contracts.registry import DEFI_MAJOR_ASSET_ADDRESS_LIST, SUBGRAPH_IDS
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

# Bug fix (2026-07-08, `docs/DEFI_INSTRUMENTS.md` "Subgraph fetch" section): the
# TVL-ranked pagination above applies the major-assets whitelist AFTER the ~6,000-pool
# ranking cutoff, so a genuine major/major pool that happens to rank below the cutoff
# on a given day was silently never fetched at all. This supplementary query asks the
# subgraph directly for pools where BOTH token0 AND token1 are in the known major-asset
# address list — guaranteeing every major/major pool is captured regardless of TVL rank.
# Verified live against the production Uniswap V3 gateway (2026-07-08): this exact query
# found a real DAI/USDT pool with totalValueLockedUSD ~= $0.0004 — many orders of magnitude
# below the ~$2,195 TVL of the pool ranked #6000 in the plain TVL-ranked pagination — proving
# the old two-stage pipeline would have silently dropped it.
_MAJOR_ASSET_POOLS_QUERY_TEMPLATE = """
query GetMajorAssetPools($first: Int!, $skip: Int!, $tokens: [Bytes!]!) {{
    pools(
        first: $first, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc
        where: {{ token0_in: $tokens, token1_in: $tokens }}{block_clause}
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
        # Convert protocol slug (e.g. "pancakeswap_v3") to UAC venue prefix (e.g. "PANCAKESWAP_V3").
        # KEEP the underscore — canonical defi venue names are the underscore form (UAC
        # registry/defi_venues.py). Stripping it ("PANCAKESWAPV3") made every record's venue
        # tag unknown to the URDI venue filter, silently dropping the whole fetched universe
        # (R4-IS-freeze finding 2026-06-11; regression from c7d9bb2 which updated this comment
        # but not the code).
        self._venue_prefix = self._protocol_slug.upper()
        # Set per get_instruments() call: True if any cascade leg (primary or fallback) genuinely
        # errored (transient/malformed) — drives the all-fallbacks-failed raise (DeFi-plan A8b).
        self._cascade_errored: bool = False

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return self._protocol_slug

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Uniswap V3 pools as instruments.

        Cascade-aware honest absence (DeFi-plan A8b): this adapter tries a chain of subgraph
        schemas (primary pools → Algebra fork → SushiSwap pairs → Messari). Each fallback's
        ``200-with-errors`` / missing-``data`` is "try the next schema" control-flow — a TRANSIENT
        failure on one fallback is NOT fatal while another fallback may still return real data. But
        if EVERY fallback was exhausted, NONE returned a pool, AND at least one genuinely errored
        (HTTP error / GraphQL-errors / missing ``data`` payload), the universe cannot be trusted as
        empty — RAISE ``ConnectionError`` so the discovery caller records ``attempted_failed``
        instead of silently treating a total fetch failure as an empty instrument universe.
        Mirrors the MTDS ``dex_swaps_handler`` cascade pattern.
        """
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        url = self._resolve_api_url()
        if not url:
            return []

        # Tracks whether ANY cascade leg (primary or fallback) genuinely errored (transient /
        # malformed) vs returned a legitimate-but-empty result. Drives the all-fallbacks-failed raise.
        self._cascade_errored = False

        block_num = await self._resolve_block_num()
        block_clause = f", block: {{number: {block_num}}}" if block_num else ""
        query = _POOLS_QUERY_TEMPLATE.format(block_clause=block_clause)

        # Paginate through all pools (The Graph caps skip at 5000)
        all_pools: list[dict[str, object]] = []
        skip = 0
        schema_error = False
        indexers_unavailable = False
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
                    # Transport error on the PRIMARY query is fatal for the whole discovery — no
                    # fallback can succeed once the session/gateway is down. Raise immediately.
                    self._log_fetch_error(exc)
                    raise ConnectionError(str(exc)) from exc

                # Check for GraphQL errors (schema mismatches, indexer issues)
                resp_errors: list[dict[str, object]] = data.get("errors", []) if isinstance(data, dict) else []
                for err in resp_errors:
                    msg = str(err.get("message", "")).lower()
                    if "has no field" in msg or "no field" in msg:
                        schema_error = True
                    if "bad indexers" in msg or "unavailable" in msg or "too far behind" in msg:
                        indexers_unavailable = True
                if resp_errors:
                    # 200-with-errors on the primary query — a transient/schema fetch failure on
                    # this leg. Record it; the fallbacks below may still recover real data.
                    self._cascade_errored = True
                if indexers_unavailable:
                    logger.warning(
                        "%s: subgraph indexers unavailable on %s — infrastructure issue",
                        self._protocol_slug,
                        self._chain,
                    )
                    break

                if not isinstance(data, dict) or not data.get("data"):
                    if skip == 0 and not schema_error:
                        logger.warning("UniswapV3: empty/absent 'data' from %s subgraph", self._chain)
                        # Primary returned 200 but no usable ``data`` payload — transient failure on
                        # this leg (mirror assert_subgraph_payload semantics). Let fallbacks try.
                        self._cascade_errored = True
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

        # Supplementary: query directly for pools where both tokens are known major
        # assets (see _MAJOR_ASSET_POOLS_QUERY_TEMPLATE above) — closes the TVL-ceiling
        # coverage gap without replacing the TVL-ranked cascade above. Additive only:
        # a failure here must not invalidate an otherwise-successful primary fetch, so
        # it never sets self._cascade_errored. Ethereum-only for now (the address list
        # is Ethereum-mainnet-derived; other chains keep the pre-existing TVL-ranked
        # behavior — a known, documented remaining gap, not silently "fixed everywhere").
        if self._chain == "ETHEREUM" and DEFI_MAJOR_ASSET_ADDRESS_LIST:
            major_pools = await self._fetch_major_asset_pools(url, block_num)
            if major_pools:
                seen_ids = {p.get("id") for p in all_pools}
                new_pools = [p for p in major_pools if p.get("id") not in seen_ids]
                if new_pools:
                    logger.info(
                        "UniswapV3: major-asset direct query found %d additional pool(s) "
                        "below the TVL-ranked cutoff on %s",
                        len(new_pools),
                        self._chain,
                    )
                all_pools.extend(new_pools)

        # All cascade legs exhausted with zero pools. If ANY leg genuinely errored (transient /
        # malformed / GraphQL-errors), the empty universe is NOT trustworthy — raise so discovery
        # records attempted_failed (DeFi-plan A8b). If every leg cleanly returned an empty result,
        # this is a legitimate empty universe → return [].
        if not all_pools and self._cascade_errored:
            self._log_fetch_error(aiohttp.ClientError("all subgraph schemas failed/errored"))
            raise ConnectionError(
                f"{self._protocol_slug}-{self._chain}: all subgraph cascade schemas "
                "(primary/algebra/sushiswap/messari) failed or returned errors — transient fetch failure"
            )

        results: list[InstrumentRecord] = []

        for pool in all_pools:
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("UniswapV3: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    async def _fetch_major_asset_pools(self, url: str, block_num: int | None) -> list[dict[str, object]]:
        """Query pools where both tokens are known major assets, directly by address.

        Supplementary to the TVL-ranked cascade in ``get_instruments`` — closes the
        "genuine major-asset pool ranked below the ~6,000 TVL cutoff is never fetched"
        gap (see ``docs/DEFI_INSTRUMENTS.md`` "Subgraph fetch" section). Failure here is
        soft: logs and returns ``[]`` without setting ``self._cascade_errored``, since this
        is an additive enhancement, not a required leg of the discovery cascade.
        """
        block_clause = f", block: {{number: {block_num}}}" if block_num else ""
        query = _MAJOR_ASSET_POOLS_QUERY_TEMPLATE.format(block_clause=block_clause)
        # The Graph stores/compares `Bytes` (address) entity fields lowercased — a
        # mixed-case checksummed address in an `_in` filter silently matches nothing
        # (verified live: checksummed input -> 0 pools, identical lowercased input ->
        # 404 pools, same subgraph, same call). Never pass checksummed case here.
        tokens = [addr.lower() for addr in DEFI_MAJOR_ASSET_ADDRESS_LIST]

        all_pools: list[dict[str, object]] = []
        skip = 0
        try:
            async with self._make_session() as session:
                while skip <= _MAX_SKIP:
                    variables = {"first": _FETCH_LIMIT, "skip": skip, "tokens": tokens}
                    async with session.post(
                        url,
                        json={"query": query, "variables": variables},
                        headers={"Content-Type": "application/json"},
                    ) as resp:
                        resp.raise_for_status()
                        data = await resp.json()

                    if not isinstance(data, dict) or data.get("errors"):
                        if data.get("errors") if isinstance(data, dict) else None:
                            logger.debug(
                                "UniswapV3 major-asset query returned errors on %s: %s",
                                self._chain,
                                data.get("errors"),
                            )
                        break

                    page: list[dict[str, object]] = (data.get("data") or {}).get("pools") or []
                    if not page:
                        break

                    all_pools.extend(page)
                    if len(page) < _FETCH_LIMIT:
                        break
                    skip += _FETCH_LIMIT
        except aiohttp.ClientError as exc:
            logger.warning("UniswapV3 major-asset query failed on %s (non-fatal): %s", self._chain, exc)
            return []

        return all_pools

    async def _fetch_messari_pools(self, url: str) -> list[dict[str, object]]:
        """Fetch pools from Messari-schema subgraph and normalise to official format.

        Returns ``[]`` on transient/malformed failure (so the caller's cascade can continue) but
        records ``self._cascade_errored`` so ``get_instruments`` can raise if EVERY leg failed
        (DeFi-plan A8b).
        """
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
            self._cascade_errored = True
            return []

        if not isinstance(data, dict) or not data.get("data"):
            # missing OR null `data` (e.g. {"data": None} on indexer-unavailable) → cascade error, not a
            # real empty. `not data.get("data")` catches the None case the bare `"data" not in data` missed
            # (matches _fetch_algebra_pools / _fetch_sushiswap_pairs).
            self._cascade_errored = True
            return []

        raw_pools = (data.get("data") or {}).get("liquidityPools", [])
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
                        # Missing 'data' on the first page is a transient/malformed leg failure;
                        # on a later page it just terminates pagination of an already-fetched page.
                        if not all_pools:
                            self._cascade_errored = True
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
            self._cascade_errored = True
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
            self._cascade_errored = True
            return []

        if not isinstance(data, dict) or "data" not in data:
            self._cascade_errored = True
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

        # Uniswap V3 feeTier is in hundredths-of-bps (e.g. 500 = 0.05%, 3000 = 0.3%).
        # Plan asks for basis points: feeTier / 100 (e.g. 3000 → 30 bps).
        pool_fee_tier_bps = self._parse_fee_tier_bps(fee_tier)
        # Canonical DEX-pool key grammar (docs/DEFI_INSTRUMENTS.md "DEX pools" —
        # instrument_id_format_canonicalization_2026_07_08.md finding 2): fee tier
        # is DASH-separated (not colon) and a real basis-point value (not the raw
        # on-wire feeTier), and is OMITTED entirely when no real fee tier exists
        # (matching the target's "[-FEE_TIER]" optional-bracket grammar) rather
        # than a fabricated "-0" placeholder.
        symbol = f"{base}-{quote}-{pool_fee_tier_bps}" if pool_fee_tier_bps is not None else f"{base}-{quote}"
        venue_tag = f"{self._venue_prefix}-{self._chain}"
        instrument_key = f"{venue_tag}:POOL:{symbol}"

        available_since = parse_created_timestamp(pool.get("createdAtTimestamp"))

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
