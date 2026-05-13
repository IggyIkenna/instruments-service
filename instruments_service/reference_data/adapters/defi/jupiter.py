"""Jupiter Aggregator reference data adapter — token pair discovery.

Discovers token pairs available via Jupiter aggregation on Solana using
the Jupiter Quote API v6. Jupiter is the dominant DEX aggregator on Solana,
routing swaps across Raydium, Orca, Meteora, Phoenix and other venues.

Data source: Jupiter API v6 (https://lite-api.jup.ag/swap/v1/) — public, no auth required.
Program ID: JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4 (Jupiter v6 aggregator)
Reference: https://dev.jup.ag/docs/
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

# Jupiter lite-api for swap quotes
_DATA_API_URL = get_solana_protocol_url("jupiter", "api_url") or "https://lite-api.jup.ag/swap/v1"
# Jupiter token registry (authoritative list of routable tokens)
_TOKEN_LIST_URL = "https://tokens.jup.ag/tokens?tags=strict"
_DEFAULT_CHAIN = "SOLANA"
_JUPITER_DEPLOY_DATE = get_protocol_floor_date("jupiter")

# Core tradeable pairs — Jupiter routes these reliably for arbitrage_price_dispersion strategy
_CORE_ROUTABLE_PAIRS: list[tuple[str, str]] = [
    ("SOL", "USDC"),
    ("SOL", "USDT"),
    ("JITOSOL", "USDC"),
    ("JITOSOL", "SOL"),
    ("MSOL", "USDC"),
    ("MSOL", "SOL"),
    ("BSOL", "USDC"),
    ("BSOL", "SOL"),
    ("JUP", "USDC"),
    ("JUP", "SOL"),
    ("RAY", "USDC"),
    ("BONK", "USDC"),
    ("WIF", "USDC"),
    ("PYTH", "USDC"),
    ("JTO", "USDC"),
]


def _classify_jupiter_error(exc: Exception, status: int | None = None) -> str:
    """Map a Jupiter HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class JupiterReferenceDataAdapter(BaseReferenceDataAdapter):
    """Jupiter Aggregator reference data: routable token pair discovery.

    Jupiter aggregates liquidity from all major Solana DEXes (Raydium, Orca,
    Meteora, Phoenix, and 20+ others). For instruments-service purposes, we
    represent each routable pair as a SPOT instrument with Jupiter as venue.

    The adapter fetches the Jupiter token list (strict-tagged tokens) to build
    the full universe of routable pairs. For the arbitrage_price_dispersion
    archetype, the relevant pairs are SOL/stablecoins and LST/SOL pairs.
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
        return f"JUPITER-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError, endpoint: str) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Jupiter."""
        error_code = _classify_jupiter_error(exc)
        classification = classify_venue_error("jupiter", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Jupiter %s request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch routable Jupiter token pairs as instruments.

        Returns core routable pairs (SOL/USDC, LST pairs, major tokens).
        For the full token universe, use _fetch_token_list().
        """
        if instrument_type not in (None, InstrumentType.SPOT, "spot"):
            logger.info("Jupiter only supports SPOT instruments; requested %s", instrument_type)
            return []

        results: list[InstrumentRecord] = []
        for base, quote in _CORE_ROUTABLE_PAIRS:
            record = self._build_pair_record(base, quote)
            if record:
                results.append(record)

        logger.info("Jupiter: built %d routable pair instruments on %s", len(results), self._chain)
        return results

    async def _fetch_token_list(self) -> list[dict[str, object]]:
        """Fetch Jupiter strict-tagged token list for extended pair universe."""
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, _TOKEN_LIST_URL)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc, "tokens")
                raise ConnectionError(str(exc)) from exc
            logger.error("Jupiter token list request failed after retries: %s", exc)
            raise

        if isinstance(data, list):
            return data
        return []

    def _build_pair_record(
        self,
        base_asset: str,
        quote_asset: str,
    ) -> InstrumentRecord | None:
        """Build an InstrumentRecord for a Jupiter routable token pair."""
        if not base_asset or not quote_asset:
            return None

        instrument_key = f"{self.venue}:SPOT:{base_asset}-{quote_asset}"
        raw_symbol = f"{base_asset}/{quote_asset}"

        return InstrumentRecord(
            instrument_key=instrument_key,
            venue=self.venue,
            raw_symbol=raw_symbol,
            instrument_type=InstrumentType.SPOT,
            base_asset=base_asset.upper(),
            quote_asset=quote_asset.upper(),
            settle_asset=quote_asset.upper(),
            margin_type=MarginType.LINEAR,
            tick_size=Decimal("0.000001"),  # Jupiter routes sub-cent quotes
            min_size=Decimal("0.001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            available_from_datetime=_JUPITER_DEPLOY_DATE,
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
        raise NotImplementedError("Jupiter does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Jupiter aggregated pairs have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Jupiter funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Jupiter OHLCV not supported via reference data")
