"""Lifinity Protocol reference data adapter — AMM pool discovery.

Discovers Lifinity proactive market-making (PMM) pools on Solana via
the Lifinity public API. Pools are returned as InstrumentRecord with
instrument_type=SOLANA_AMM_POOL — a Lifinity PMM pool is an AMM/DEX liquidity
pool, NOT a two-token quoted SPOT_PAIR (operator ruling 2026-07-18,
defi_consolidated_closeout_2026_07_18.md "SPOT_ASSET vs SPOT_PAIR vs POOL":
a Solana AMM pool is `SOLANA_AMM_POOL`, mirroring how MTDS types orca/raydium
DEX-pool tick data). Routed through `build_canonical_instrument_id` so the
emitted `instrument_key` TYPE segment is correct AT MINT TIME
(`LIFINITY-SOLANA:SOLANA_AMM_POOL:SOL-USDC`), not the pre-fix `:SPOT:` shorthand
that mismatched the `SPOT_PAIR` field.

Lifinity uses a proactive market-making model (unlike reactive AMMs like Raydium/Orca),
concentrating liquidity at oracle-derived prices to reduce impermanent loss.

Data source: Lifinity API (https://api.lifinity.io/) — public, no auth required.
Program ID: LFNTYraetVioAPnGJht4yNg2aUZFXR776cMeN9VMjXp (Lifinity V2)
Reference: https://lifinity.io/
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import AssetGroup, build_canonical_instrument_id, classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_api_contracts.registry import get_solana_protocol_url
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import extract_rest_list_or_raise
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_DATA_API_URL = get_solana_protocol_url("lifinity", "api_url") or "https://api.lifinity.io"
_DEFAULT_CHAIN = "SOLANA"
_LIFINITY_DEPLOY_DATE = get_protocol_floor_date("lifinity")


def _classify_lifinity_error(exc: Exception, status: int | None = None) -> str:
    """Map a Lifinity HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class LifinityReferenceDataAdapter(BaseReferenceDataAdapter):
    """Lifinity Protocol reference data: PMM pool discovery via public API.

    Uses the public Lifinity API (https://api.lifinity.io/pools)
    which requires no auth and returns active pools.
    Each Lifinity pool produces one SOLANA_AMM_POOL instrument (proactive market-making pool).
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
        return f"LIFINITY-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Lifinity."""
        error_code = _classify_lifinity_error(exc)
        classification = classify_venue_error("lifinity", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Lifinity %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            endpoint,
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": endpoint,
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
        """Fetch active Lifinity PMM pools as instruments."""
        if instrument_type not in (None, InstrumentType.SOLANA_AMM_POOL, "spot"):
            logger.info("Lifinity only supports SOLANA_AMM_POOL instruments; requested %s", instrument_type)
            return []

        pools = await self._fetch_pools()
        results: list[InstrumentRecord] = []

        for pool in pools:
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("Lifinity: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    async def _fetch_pools(self) -> list[dict[str, object]]:
        """Fetch all active PMM pools from the Lifinity API."""
        url = f"{_DATA_API_URL}/pools"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "pools")
                raise ConnectionError(str(exc)) from exc
            logger.error("Lifinity pools request failed after retries: %s", exc)
            raise

        return extract_rest_list_or_raise(data, venue=self.venue, keys=("pools", "data", "pairs"))

    def _build_pool_record(
        self,
        pool: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Lifinity PMM pool."""
        # Lifinity pool fields: pool_address, token_0, token_1, or name
        pool_address = str(pool.get("pool_address") or pool.get("address") or "")
        pool_name = str(pool.get("name") or pool.get("symbol") or "")

        # Extract token pair
        token_0 = pool.get("token_0") or {}
        token_1 = pool.get("token_1") or {}

        if isinstance(token_0, dict) and token_0 and isinstance(token_1, dict) and token_1:
            base_asset = str(token_0.get("symbol") or token_0.get("name") or "").upper()
            quote_asset = str(token_1.get("symbol") or token_1.get("name") or "USDC").upper()
        elif pool_name and "-" in pool_name:
            parts = pool_name.upper().split("-")
            base_asset = parts[0].strip()
            quote_asset = parts[1].strip() if len(parts) > 1 else "USDC"
        elif pool_name and "/" in pool_name:
            parts = pool_name.upper().split("/")
            base_asset = parts[0].strip()
            quote_asset = parts[1].strip() if len(parts) > 1 else "USDC"
        else:
            return None

        if not base_asset:
            return None

        raw_symbol = pool_address if pool_address else pool_name
        # Route through the shared canonical builder so the emitted TYPE segment
        # is the real InstrumentType enum value (SOLANA_AMM_POOL), not the old
        # `:SPOT:` shorthand that mismatched the field. venue already carries the
        # composed VENUE-CHAIN token, so passthrough=True reproduces the exact id.
        instrument_key = build_canonical_instrument_id(
            AssetGroup.DEFI, self.venue, InstrumentType.SOLANA_AMM_POOL, f"{base_asset}-{quote_asset}", passthrough=True
        )

        tick_size_raw = pool.get("tickSize") or pool.get("price_increment") or "0.0001"
        try:
            tick_size = Decimal(str(tick_size_raw))
        except (ValueError, TypeError):
            tick_size = Decimal("0.0001")

        return InstrumentRecord(
            instrument_key=instrument_key,
            # DeFi has no raw-code-to-human-name translation gap the way TradFi does (its symbols
            # are already human-readable) -- canonical_instrument_id mirrors instrument_key.
            canonical_instrument_id=instrument_key,
            venue=self.venue,
            raw_symbol=raw_symbol,
            instrument_type=InstrumentType.SOLANA_AMM_POOL,
            base_asset=base_asset,
            quote_asset=quote_asset,
            settle_asset=quote_asset,
            margin_type=MarginType.LINEAR,
            tick_size=tick_size,
            min_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_LIFINITY_DEPLOY_DATE,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        instruments = await self.get_instruments()
        sym_upper = symbol.upper()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == sym_upper:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Lifinity does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Lifinity PMM pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Lifinity funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Lifinity OHLCV not supported via reference data")
