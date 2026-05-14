"""Extended reference data adapter — instrument discovery via /info/markets.

Extended is a StarkNet perp DEX (Extended.exchange). Public REST at
api.starknet.extended.exchange — no auth required for market discovery.
Returns all active perpetual markets as InstrumentRecord objects.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp
from unified_api_contracts import classify_venue_error
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType
from unified_trading_library import log_event

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# DIAGNOSIS 2026-05-14 (slot-3): 0 blobs captured from 15 attempted manifest rows
# (all 2026-04-30, all attempted_failed). Likely stale domain — Extended.exchange
# may have rebranded or moved its REST API (mirrors ASTER pattern: ASTER used
# www.aster.exchange instead of fapi.asterdex.com). Action needed: probe this
# URL live and check Extended Finance docs for the current REST base. Until
# corrected, every adapter call will fail into attempted_failed.
# Reference: plans/active/issues/emerging_perp_venue_adapters_broken_2026_05_13.md
_EXTENDED_API_BASE = "https://api.starknet.extended.exchange/api/v1"
# Extended StarkNet mainnet launch (historical OHLCV available from 2024-07-26).
_EXTENDED_DEPLOY_DATE = datetime(2024, 7, 26, tzinfo=UTC)

# Fallback market list used when /info/markets is unreachable.
# Mirrors MTDS _EXTENDED_FALLBACK_SYMBOLS (umi_tick_provider.py:1086-1092).
_EXTENDED_FALLBACK_MARKETS: tuple[str, ...] = (
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "SUI-USD",
)


def _classify_extended_error(exc: Exception, status: int | None = None) -> str:
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "500"
    return "UNKNOWN"


class ExtendedReferenceDataAdapter(BaseReferenceDataAdapter):
    """Extended reference data: perp market discovery via /info/markets.

    Fetches ACTIVE markets from the public /info/markets endpoint.
    Each active market with status=ACTIVE becomes one InstrumentRecord with
    instrument_type=PERPETUAL, settle_asset=USDC.
    Falls back to the curated list on network error.
    """

    @property
    def venue(self) -> str:
        return "EXTENDED-STARKNET"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Extended perp markets from /info/markets."""
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        symbols: list[str] = []
        async with self._make_session() as session:
            try:
                raw = await self._get_with_retry(session, f"{_EXTENDED_API_BASE}/info/markets")
                payload: dict[str, object] = raw if isinstance(raw, dict) else {}
                mkt_list: list[dict[str, object]] = payload.get("data") or []  # type: ignore[assignment]
                symbols = [str(m["name"]) for m in mkt_list if m.get("active") and str(m.get("status", "")) == "ACTIVE"]
                if not symbols:
                    logger.warning("Extended /info/markets returned no ACTIVE markets — using fallback")
                    symbols = list(_EXTENDED_FALLBACK_MARKETS)
            except (aiohttp.ClientError, RuntimeError) as exc:
                if isinstance(exc, aiohttp.ClientError):
                    error_code = _classify_extended_error(exc)
                    classification = classify_venue_error("extended", error_code)
                    action = classification.action.value if classification else "fail"
                    logger.warning(
                        "Extended /info/markets failed: %s (action=%s) — using fallback",
                        exc,
                        action,
                    )
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": self.venue,
                            "endpoint": "info/markets",
                            "error": str(exc),
                            "action": action,
                        },
                    )
                else:
                    logger.warning("Extended /info/markets failed after retries: %s — using fallback", exc)
                symbols = list(_EXTENDED_FALLBACK_MARKETS)

        results: list[InstrumentRecord] = []
        for sym in symbols:
            base_asset = sym.split("-")[0].upper() if "-" in sym else sym.upper()
            results.append(
                InstrumentRecord(
                    instrument_key=f"EXTENDED-STARKNET:PERP:{sym.upper()}",
                    venue=self.venue,
                    raw_symbol=sym,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=base_asset,
                    quote_asset="USD",
                    settle_asset="USDC",
                    margin_type=MarginType.LINEAR,
                    tick_size=Decimal("0.0001"),
                    min_size=Decimal("0.001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=_EXTENDED_DEPLOY_DATE,
                    timezone="UTC",
                )
            )

        logger.info("Extended: fetched %d perp instruments", len(results))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        for inst in await self.get_instruments():
            if inst.raw_symbol == symbol or inst.raw_symbol == symbol.upper():
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        raise NotImplementedError("Extended does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Extended perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Extended funding rate not supported via reference data adapter")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Extended OHLCV not supported via reference data adapter")
