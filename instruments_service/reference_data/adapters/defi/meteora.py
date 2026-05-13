"""Meteora Dynamic Liquidity reference data adapter — AMM pool discovery.

Discovers Meteora DLMM (Dynamic Liquidity Market Maker) pools on Solana via
the Meteora public API. Pools are returned as InstrumentRecord with
instrument_type=SPOT (AMM liquidity pools).

Data source: Meteora API (https://app.meteora.ag/api/) — public, no auth required.
Program ID: LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo (Meteora Dynamic Liquidity)
Reference: https://docs.meteora.ag/
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
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
from ._solana_utils import get_protocol_floor_date

logger = logging.getLogger(__name__)

_DATA_API_URL = get_solana_protocol_url("meteora", "api_url") or "https://app.meteora.ag/api"
_DEFAULT_CHAIN = "SOLANA"
_METEORA_DEPLOY_DATE = get_protocol_floor_date("meteora")


def _classify_meteora_error(exc: Exception, status: int | None = None) -> str:
    """Map a Meteora HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class MeteoraReferenceDataAdapter(BaseReferenceDataAdapter):
    """Meteora Dynamic Liquidity reference data: AMM pool discovery via public API.

    Uses the public Meteora API (https://app.meteora.ag/api/pools)
    which requires no auth and returns active DLMM pools.
    Each Meteora pool produces one SPOT instrument (liquidity pair).
    Meteora pools use various quote assets (USDC, SOL, etc.).
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
        return f"METEORA-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Meteora."""
        error_code = _classify_meteora_error(exc)
        classification = classify_venue_error("meteora", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Meteora %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch active Meteora DLMM pools as instruments."""
        if instrument_type not in (None, InstrumentType.SPOT, "spot"):
            logger.info("Meteora only supports SPOT instruments; requested %s", instrument_type)
            return []

        pools = await self._fetch_pools()
        results: list[InstrumentRecord] = []

        for pool in pools:
            record = self._build_pool_record(pool)
            if record:
                results.append(record)

        logger.info("Meteora: fetched %d pool instruments on %s", len(results), self._chain)
        return results

    async def _fetch_pools(self) -> list[dict[str, object]]:
        """Fetch active DLMM pools from the Meteora API."""
        url = f"{_DATA_API_URL}/pools"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "pools")
                raise ConnectionError(str(exc)) from exc
            logger.error("Meteora pools request failed after retries: %s", exc)
            raise

        if isinstance(data, list):
            return data
        raw: dict[str, object] = data if isinstance(data, dict) else {}
        # Meteora API may return {"pools": [...]} or a flat list
        pools = raw.get("pools") or raw.get("data") or raw.get("groups")
        if isinstance(pools, list):
            return pools
        return []

    def _build_pool_record(
        self,
        pool: dict[str, object],
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord from a Meteora DLMM pool."""
        # Meteora pool fields: pool_address, pool_name, pool_type, mint_x, mint_y
        pool_name = str(pool.get("pool_name") or pool.get("name") or "")
        pool_address = str(pool.get("pool_address") or pool.get("address") or "")

        if not pool_name and not pool_address:
            return None

        # Parse token pair from name (e.g. "SOL-USDC", "JUP-USDC", "BONK-SOL")
        if pool_name and "-" in pool_name:
            parts = pool_name.upper().split("-")
            base_asset = parts[0].strip()
            quote_asset = parts[1].strip() if len(parts) > 1 else "USDC"
        else:
            # Fall back to pool address as identifier
            base_asset = pool_address[:8] if pool_address else "UNKNOWN"
            quote_asset = "USDC"

        if not base_asset:
            return None

        # Use pool address as unique identifier if available
        raw_symbol = pool_address if pool_address else pool_name
        instrument_key = f"{self.venue}:SPOT:{base_asset}-{quote_asset}"

        # Pool TVL or liquidity as proxy for volume
        tick_size_raw = pool.get("bin_step") or pool.get("tickSize") or "1"
        try:
            # bin_step in Meteora is basis points; convert to price granularity
            bin_step = int(str(tick_size_raw))
            tick_size = Decimal(str(bin_step)) / Decimal("10000")
        except (ValueError, TypeError):
            tick_size = Decimal("0.0001")

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            raw_symbol=raw_symbol,
            instrument_type=InstrumentType.SPOT,
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
            available_from_datetime=_METEORA_DEPLOY_DATE,
            timezone="UTC",
        )

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
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
        raise NotImplementedError("Meteora does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Meteora AMM pools have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Meteora funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Meteora OHLCV not supported via reference data")
