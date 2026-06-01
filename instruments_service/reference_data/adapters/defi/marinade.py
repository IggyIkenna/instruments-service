"""Marinade Finance reference data adapter -- instrument discovery via REST API.

Discovers Marinade stake pools on Solana (mSOL liquid staking + native staking).
Pools are returned as InstrumentRecord with instrument_type="STAKING".

Data source: Marinade REST API (https://api.marinade.finance).
Reference: https://docs.marinade.finance/
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

_BASE_URL = get_solana_protocol_url("marinade") or "https://api.marinade.finance"
_DEFAULT_CHAIN = "SOLANA"
_MARINADE_DEPLOY_DATE = get_protocol_floor_date("marinade")

# Archive metadata (is_mtds_contract_audit_2026_05_20 Phase 2).
# MTDS handlers derive the APY/rate fetch URL from this field; hardcoding is banned.
_MARINADE_APY_URL_TEMPLATE = "https://api.marinade.finance/msol/apy/365d"


def _classify_marinade_error(exc: Exception, status: int | None = None) -> str:
    """Map a Marinade HTTP/network error to a UAC error code for classification."""
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status == 503 or "503" in msg or "unavailable" in msg:
        return "503"
    is_server_err = status is not None and status >= 500
    if is_server_err or "500" in msg or "internal" in msg or "server" in msg:
        return "500"
    return "UNKNOWN"


class MarinadeReferenceDataAdapter(BaseReferenceDataAdapter):
    """Marinade reference data: staking pool discovery from REST API.

    Discovers mSOL liquid staking and native staking opportunities.
    Produces instruments for:
    - STAKE (mSOL): liquid staking instrument, symbol="MSOL"
    - NATIVE_STAKE: native staking instrument, symbol="NATIVE-SOL-STAKE"
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
        return f"MARINADE-{self._chain}"

    def _log_fetch_error(self, exc: aiohttp.ClientError) -> None:
        """Classify and log an ADAPTER_FETCH_FAILED event for Marinade."""
        error_code = _classify_marinade_error(exc)
        classification = classify_venue_error("marinade", error_code)
        action = classification.action.value if classification else "fail"
        retry_safe = classification.retry_safe if classification else False
        logger.error(
            "Marinade request failed: %s (classified: %s, action: %s, retry_safe: %s)",
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
        """Fetch Marinade staking instruments."""
        if instrument_type not in (None, InstrumentType.STAKING):
            return []

        # Fetch Marinade mSOL APY data
        url = f"{_BASE_URL}/msol/apy/30d"
        try:
            async with self._make_session() as session:
                data = await self._get_with_retry(session, url)
        except (aiohttp.ClientError, RuntimeError) as exc:
            if isinstance(exc, aiohttp.ClientError):
                self._log_fetch_error(exc)
                raise ConnectionError(str(exc)) from exc
            logger.error("Marinade state request failed after retries: %s", exc)
            raise

        state: dict[str, object] = data if isinstance(data, dict) else {}
        venue_tag = self.venue
        results: list[InstrumentRecord] = []

        # mSOL liquid staking instrument
        msol_record = self._build_msol_record(state, venue_tag)
        if msol_record:
            results.append(msol_record)

        # Native staking instrument
        native_record = self._build_native_stake_record(state, venue_tag)
        if native_record:
            results.append(native_record)

        logger.info("Marinade: fetched %d staking instruments on %s", len(results), self._chain)
        return results

    def _build_msol_record(
        self,
        state: dict[str, object],
        venue_tag: str,
    ) -> InstrumentRecord:
        """Build InstrumentRecord for mSOL liquid staking."""
        return InstrumentRecord(
            instrument_key=f"{venue_tag}:STAKE:MSOL",
            venue=venue_tag,
            raw_symbol="mSo1iD7sSuqGMvkKPzYGBAjJrpF9VRdrByciFizSJhc",
            base_asset_contract_address="mSo1iD7sSuqGMvkKPzYGBAjJrpF9VRdrByciFizSJhc",
            instrument_type=InstrumentType.STAKING,
            base_asset="SOL",
            quote_asset="MSOL",
            tick_size=Decimal("0.000000001"),
            min_size=Decimal("0.000000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            underlying="SOL",
            available_from_datetime=_MARINADE_DEPLOY_DATE,
            base_asset_decimals=9,
            listed_at=_MARINADE_DEPLOY_DATE.date() if _MARINADE_DEPLOY_DATE else None,
            source_archive_url_template=_MARINADE_APY_URL_TEMPLATE,
        )

    def _build_native_stake_record(
        self,
        state: dict[str, object],
        venue_tag: str,
    ) -> InstrumentRecord:
        """Build InstrumentRecord for Marinade native staking."""
        return InstrumentRecord(
            instrument_key=f"{venue_tag}:STAKE:NATIVE-SOL",
            venue=venue_tag,
            raw_symbol="MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD",
            pool_address="MarBmsSgKXdrN1egZf5sqe1TMai9K1rChYNDJgjq7aD",
            instrument_type=InstrumentType.STAKING,
            base_asset="SOL",
            quote_asset="SOL",
            tick_size=Decimal("0.000000001"),
            min_size=Decimal("0.000000001"),
            contract_size=Decimal("1"),
            expiry=None,
            strike=None,
            option_type=None,
            status=InstrumentStatus.ACTIVE,
            underlying="SOL",
            available_from_datetime=_MARINADE_DEPLOY_DATE,
            base_asset_decimals=9,
            listed_at=_MARINADE_DEPLOY_DATE.date() if _MARINADE_DEPLOY_DATE else None,
            source_archive_url_template=_MARINADE_APY_URL_TEMPLATE,
        )

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
        raise NotImplementedError("Marinade does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Marinade staking has no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Marinade staking has no funding rate")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Marinade OHLCV not supported via reference data")
