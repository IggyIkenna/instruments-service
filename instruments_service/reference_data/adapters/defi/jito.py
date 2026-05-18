"""Jito reference data adapter — instrument discovery via REST API.

Discovers JitoSOL liquid staking on Solana.
Produces InstrumentRecord with instrument_type="STAKING".

Data source: Jito Stakenet API (https://kobe.mainnet.jito.network).
Reference: https://www.jito.network/docs/
"""

import logging
from datetime import datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType
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

_BASE_URL = get_solana_protocol_url("jito") or "https://kobe.mainnet.jito.network"
_DEFAULT_CHAIN = "SOLANA"
_JITO_DEPLOY_DATE = get_protocol_floor_date("jito")

# JitoSOL token mint address on Solana mainnet
_JITOSOL_MINT = "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn"

# Jito stake pool program address on Solana mainnet
_JITO_STAKE_POOL = "Jito4APyf642JPZPx3hGc6WWJ8zPKtRbRs4P3eg9gB"


def _classify_jito_error(exc: Exception, status: int | None = None) -> str:
    """Map a Jito HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class JitoReferenceDataAdapter(BaseReferenceDataAdapter):
    """Jito reference data: JitoSOL liquid staking discovery from REST API.

    Produces a single STAKING instrument for JitoSOL (MEV-enhanced SOL staking).
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
        return f"JITO-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Jito."""
        error_code = _classify_jito_error(exc)
        classification = classify_venue_error("jito", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Jito request failed: %s (classified: %s, action: %s, retry_safe: %s)",
            exc,
            error_code,
            action,
            retry_safe,
        )
        log_event(
            "ADAPTER_FETCH_FAILED",
            details={
                "venue": self.venue,
                "endpoint": "staking",
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
        """Fetch Jito staking instruments."""
        if instrument_type not in (None, InstrumentType.STAKING):
            return []

        url = f"{_BASE_URL}/api/v1/stake_pool_stats"
        try:
            async with self._make_session() as session:
                await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Jito stats request failed after retries: %s", exc)
            raise

        venue_tag = self.venue
        record = InstrumentRecord(
            instrument_key=f"{venue_tag}:STAKING:JITOSOL",
            venue=venue_tag,
            raw_symbol=_JITOSOL_MINT,
            base_asset_contract_address=_JITOSOL_MINT,
            instrument_type=InstrumentType.STAKING,
            base_asset="SOL",
            quote_asset="JITOSOL",
            tick_size=Decimal("0.000000001"),
            min_size=Decimal("0.000000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            underlying="SOL",
            available_from_datetime=_JITO_DEPLOY_DATE,
        )

        logger.info("Jito: fetched 1 staking instrument on %s", self._chain)
        return [record]

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
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
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Jito does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Jito staking has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Jito staking has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Jito OHLCV not supported via reference data")
