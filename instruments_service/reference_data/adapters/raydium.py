"""Raydium reference data adapter -- instrument discovery via REST + RPC.

Discovers Raydium CLMM and standard AMM pools on Solana.
Pools are returned as InstrumentRecord with instrument_type="POOL".

Data sources:
  - REST API (https://api-v3.raydium.io) -- currently active pools with metadata
  - Solana RPC (getProgramAccounts) -- ALL historical pool accounts on-chain

The REST API only returns currently active pools. Pools from 2021-2024 that
have been delisted or removed are invisible to the REST API. The RPC discovery
path uses getProgramAccounts on the Raydium AMM program to enumerate every
pool account that still exists on-chain, regardless of current activity.

Reference: https://docs.raydium.io/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
from unified_api_contracts.registry import SOLANA_DEFI_PROTOCOLS, get_solana_protocol_url
from unified_trading_library import log_event

from ..base_adapter import BaseReferenceDataAdapter
from ..schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ..utils.defi_utils import order_base_quote, parse_created_timestamp
from ._solana_utils import (
    batch_resolve_creation_timestamps,
    discover_program_pool_accounts,
    get_protocol_floor_date,
)

logger = logging.getLogger(__name__)

_BASE_URL = get_solana_protocol_url("raydium") or "https://api-v3.raydium.io"
_DEFAULT_CHAIN = "SOLANA"
_RAYDIUM_DEPLOY_DATE = get_protocol_floor_date("raydium")

# Raydium AMM V4 pool state account data size (bytes).
# Used as a filter in getProgramAccounts to select only pool accounts,
# excluding open-order accounts, authority accounts, etc.
_RAYDIUM_AMM_V4_POOL_DATA_SIZE = 752

# Program ID from UAC SSOT (SOLANA_DEFI_PROTOCOLS)
_RAYDIUM_PROGRAM_ID: str = SOLANA_DEFI_PROTOCOLS["raydium"]["program_id"]


def _classify_raydium_error(exc: Exception, status: int | None = None) -> str:
    """Map a Raydium HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class RaydiumReferenceDataAdapter(BaseReferenceDataAdapter):
    """Raydium reference data: pool discovery from REST API.

    Discovers CLMM and standard AMM pools. Each pool produces one instrument
    with instrument_type="POOL" and symbol=f"{tokenA}/{tokenB}".
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
        return f"RAYDIUM-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Raydium."""
        error_code = _classify_raydium_error(exc)
        classification = classify_venue_error("raydium", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Raydium pools request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": "pools",
                "error": str(exc),
                "error_code": error_code,
                "action": action,
                "retry_safe": retry_safe,
            },
        )

    async def get_instruments(
        self,
        instrument_type: str | None = None,
        include_historical: bool = True,
    ) -> list[InstrumentRecord]:
        """Fetch Raydium pools as instruments.

        Args:
            instrument_type: Filter by instrument type (only POOL supported).
            include_historical: If True, also discover historical pools via
                on-chain RPC (getProgramAccounts). These are pools that existed
                on-chain but are no longer returned by the REST API (delisted,
                depleted, or removed pools from 2021-2024).
        """
        if instrument_type not in (None, InstrumentType.POOL):
            return []

        # Phase 1: Fetch active pools from REST API
        rest_results = await self._fetch_active_pools()

        # Phase 2: Discover historical pools via on-chain RPC
        historical_results: list[InstrumentRecord] = []
        if include_historical:
            rest_pool_ids = {r.raw_symbol for r in rest_results if r.raw_symbol}
            historical_results = await self._discover_historical_pools(
                exclude_addresses=rest_pool_ids,
            )

        results = rest_results + historical_results

        # Resolve creation timestamps via Solana RPC for all pools
        if results:
            addresses = [r.raw_symbol for r in results if r.raw_symbol]
            ts_map = await batch_resolve_creation_timestamps(addresses)
            for record in results:
                if record.raw_symbol in ts_map:
                    record.available_from_datetime = ts_map[record.raw_symbol]

        return results

    async def _fetch_active_pools(self) -> list[InstrumentRecord]:
        """Fetch currently active pools from the Raydium REST API."""
        url = f"{_BASE_URL}/pools/info/list"
        params = {
            "poolType": "all",
            "poolSortField": "liquidity",
            "sortType": "desc",
            "pageSize": "1000",
            "page": "1",
        }
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._get_with_retry(session, url, params=params)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Raydium pools request failed after retries: %s", exc)
            raise

        # Raydium API returns { "id": "...", "success": true, "data": { "data": [...], "count": N, ... } }
        raw_data: dict[str, object] = data if isinstance(data, dict) else {}
        data_wrapper = raw_data.get("data") or {}
        pools: list[dict[str, object]] = (data_wrapper.get("data") if isinstance(data_wrapper, dict) else []) or []

        results: list[InstrumentRecord] = []

        for pool in pools:
            if not isinstance(pool, dict):
                continue
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("Raydium: fetched %d active pool instruments on %s", len(results), self._chain)
        return results

    async def _discover_historical_pools(
        self,
        exclude_addresses: set[str] | None = None,
    ) -> list[InstrumentRecord]:
        """Discover historical pool accounts via on-chain RPC.

        Uses getProgramAccounts on the Raydium AMM program to enumerate ALL
        pool accounts that exist on-chain, then builds minimal InstrumentRecords
        for any pools not already known from the REST API.

        Historical pools have no metadata (token symbols, TVL, etc.) from the
        REST API, so we use the on-chain account data to extract mint addresses
        and resolve token symbols via a secondary RPC call.

        Args:
            exclude_addresses: Pool addresses already known from REST API
                (skipped to avoid duplicates).

        Returns:
            List of InstrumentRecords for historical-only pools.
        """
        exclude = exclude_addresses or set()

        all_pool_addresses = await discover_program_pool_accounts(
            program_id=_RAYDIUM_PROGRAM_ID,
            protocol="raydium",
            data_size=_RAYDIUM_AMM_V4_POOL_DATA_SIZE,
        )

        if not all_pool_addresses:
            logger.info("Raydium: no pool accounts discovered via RPC")
            return []

        # Filter out pools already known from REST API
        historical_only = [addr for addr in all_pool_addresses if addr not in exclude]

        if not historical_only:
            logger.info(
                "Raydium: all %d discovered pools already known from REST API",
                len(all_pool_addresses),
            )
            return []

        logger.info(
            "Raydium: %d historical-only pools discovered (%d total on-chain, %d from REST API)",
            len(historical_only),
            len(all_pool_addresses),
            len(exclude),
        )

        # Build minimal InstrumentRecords for historical pools.
        # Without REST API metadata, we can only set pool_id and venue.
        # Token symbols will be resolved via on-chain account data in a
        # separate enrichment step (mint addresses are at known offsets
        # in the Raydium AMM V4 pool state layout).
        results: list[InstrumentRecord] = []
        for pool_id in historical_only:
            record = self._build_historical_pool_record(pool_id)
            results.append(record)

        logger.info(
            "Raydium: built %d historical pool instruments on %s",
            len(results),
            self._chain,
        )
        return results

    def _build_historical_pool_record(
        self,
        pool_id: str,
    ) -> InstrumentRecord:
        """Build a minimal InstrumentRecord for a historical on-chain pool.

        Historical pools have no REST API metadata, so we use a placeholder
        symbol and mark them as DELISTED. The creation timestamp will be
        resolved by batch_resolve_creation_timestamps() in the caller.
        """
        venue_tag = self.venue
        instrument_key = f"{venue_tag}:POOL:{pool_id}:Historical"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            raw_symbol=pool_id,
            instrument_type=InstrumentType.POOL,
            base_asset="UNKNOWN",
            quote_asset="UNKNOWN",
            tick_size=Decimal("0.000001"),
            min_size=Decimal("0.000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.DELISTED,
            available_from_datetime=_RAYDIUM_DEPLOY_DATE,
        )

    def _build_pool_record(
        self,
        pool: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a single Raydium pool, or None."""
        pool_id = pool.get("id")
        if not pool_id:
            return None

        # Extract token symbols from mintA/mintB or nested token info
        sym_a = self._extract_token_symbol(pool, "mintA")
        sym_b = self._extract_token_symbol(pool, "mintB")
        if not sym_a or not sym_b:
            return None

        base, quote = order_base_quote(sym_a, sym_b)

        # TVL minimum: skip dust pools
        tvl_raw = pool.get("tvl") or pool.get("liquidity") or 0
        tvl = float(str(tvl_raw)) if tvl_raw else 0
        if tvl < 10_000:  # $10k minimum TVL
            return None

        # Raydium API returns openTime (Unix seconds) for pool creation
        available_since = parse_created_timestamp(pool.get("openTime")) or _RAYDIUM_DEPLOY_DATE

        venue_tag = self.venue
        pool_type = str(pool.get("type", "Standard"))
        instrument_key = f"{venue_tag}:POOL:{base}-{quote}:{pool_type}"

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

    def _extract_token_symbol(self, pool: dict[str, object], key: str) -> str:
        """Extract token symbol from pool data.

        Raydium API may nest token info as { "mintA": { "symbol": "SOL", ... } }
        or provide flat fields like "mintSymbolA".
        """
        token_data = pool.get(key)
        if isinstance(token_data, dict):
            return str(token_data.get("symbol", "")).upper()

        # Fallback: check for flat symbol fields (e.g. "mintSymbolA" for "mintA")
        suffix = key[-1] if key.endswith(("A", "B")) else ""
        flat_key = f"mintSymbol{suffix}"
        flat_symbol = pool.get(flat_key)
        if flat_symbol:
            return str(flat_symbol).upper()

        return ""

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
        raise NotImplementedError("Raydium does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Raydium pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Raydium pools have no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Raydium OHLCV not supported via reference data")
