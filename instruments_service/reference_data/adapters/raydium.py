"""Raydium reference data adapter -- instrument discovery via REST API.

Discovers Raydium CLMM and standard AMM pools on Solana.
Pools are returned as InstrumentRecord with instrument_type="pool".

Data source: Raydium REST API (https://api-v3.raydium.io).
Reference: https://docs.raydium.io/
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
from ..utils.defi_utils import order_base_quote

logger = logging.getLogger(__name__)

_BASE_URL = "https://api-v3.raydium.io"
_DEFAULT_CHAIN = "SOLANA"


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
    with instrument_type="pool" and symbol=f"{tokenA}/{tokenB}".
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
    ) -> list[InstrumentRecord]:
        """Fetch active Raydium pools as instruments."""
        if instrument_type not in (None, "pool"):
            return []

        url = f"{_BASE_URL}/pools/info/list"
        params = {
            "poolType": "all",
            "poolSortField": "liquidity",
            "sortType": "desc",
            "pageSize": "100",
            "page": "1",
        }
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._get_with_retry(session, url, params=params)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
            else:
                logger.error("Raydium pools request failed after retries: %s", exc)
            return []

        # Raydium API returns { "id": "...", "success": true, "data": { "data": [...], "count": N, ... } }
        raw_data: dict[str, object] = data if isinstance(data, dict) else {}
        data_wrapper = raw_data.get("data") or {}
        pools: list[dict[str, object]] = (data_wrapper.get("data") if isinstance(data_wrapper, dict) else []) or []

        now = datetime.now(UTC)
        results: list[InstrumentRecord] = []

        for pool in pools:
            if not isinstance(pool, dict):
                continue
            record = self._build_pool_record(pool, now)
            if record:
                results.append(record)

        logger.info("Raydium: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    def _build_pool_record(
        self,
        pool: dict[str, object],
        now: datetime,
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

        # Filter: at least one token must be a major asset
        if base not in DEFI_MAJOR_ASSET_SYMBOLS and quote not in DEFI_MAJOR_ASSET_SYMBOLS:
            return None

        venue_tag = self.venue
        symbol = f"{base}/{quote}"
        pool_type = str(pool.get("type", "Standard"))
        instrument_key = f"{venue_tag}:POOL:{base}-{quote}:{pool_type}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=venue_tag,
            symbol=symbol,
            raw_symbol=str(pool_id),
            instrument_type="pool",
            base_asset=base,
            quote_asset=quote,
            tick_size=Decimal("0.000001"),
            lot_size=Decimal("0.000001"),
            min_order_size=Decimal("0"),
            contract_size=Decimal("1"),
            settlement_asset=quote,
            expiry=None,
            strike=None,
            option_type=None,
            is_active=True,
            updated_at=now,
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
        instrument_type: str = "future",
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
