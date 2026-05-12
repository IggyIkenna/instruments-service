"""Lighter reference data adapter — instrument discovery via /orderBookDetails.

Lighter is a zkSync L2 perp DEX (CLOB). Public REST at
mainnet.zklighter.elliot.ai — no auth required for market discovery.
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

_LIGHTER_API_BASE = "https://mainnet.zklighter.elliot.ai/api/v1"
# Lighter zkSync mainnet launch (first perpetual markets live 2024-08-01).
_LIGHTER_DEPLOY_DATE = datetime(2024, 8, 1, tzinfo=UTC)


def _classify_lighter_error(exc: Exception, status: int | None = None) -> str:
    msg = str(exc).lower()
    if status == 429 or "429" in msg or "rate" in msg:
        return "RATE_LIMIT"
    if status is not None and status >= 500:
        return "500"
    return "UNKNOWN"


class LighterReferenceDataAdapter(BaseReferenceDataAdapter):
    """Lighter reference data: perp market discovery via /orderBookDetails.

    Fetches all PERP markets from the public /orderBookDetails endpoint.
    Each market with market_type=perp becomes one InstrumentRecord with
    instrument_type=PERPETUAL, settle_asset=USDC.
    """

    @property
    def venue(self) -> str:
        return "LIGHTER-ZKSYNC"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Fetch active Lighter perp markets from /orderBookDetails."""
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        async with self._make_session() as session:
            try:
                raw = await self._get_with_retry(session, f"{_LIGHTER_API_BASE}/orderBookDetails")
            except (aiohttp.ClientError, RuntimeError) as exc:
                if isinstance(exc, aiohttp.ClientError):
                    error_code = _classify_lighter_error(exc)
                    classification = classify_venue_error("lighter", error_code)
                    action = classification.action.value if classification else "fail"
                    retry_safe = classification.retry_safe if classification else False
                    logger.error(
                        "Lighter /orderBookDetails failed: %s (action=%s, retry_safe=%s)",
                        exc,
                        action,
                        retry_safe,
                    )
                    log_event(
                        "ADAPTER_FETCH_FAILED",
                        details={
                            "venue": self.venue,
                            "endpoint": "orderBookDetails",
                            "error": str(exc),
                            "action": action,
                        },
                    )
                    return []
                logger.error("Lighter /orderBookDetails failed after retries: %s", exc)
                return []

        details: dict[str, object] = raw if isinstance(raw, dict) else {}
        markets_raw: list[dict[str, object]] = (
            details.get("order_book_details") or []  # type: ignore[assignment]
        )

        results: list[InstrumentRecord] = []
        for m in markets_raw:
            if str(m.get("market_type", "")).lower() != "perp":
                continue
            sym = str(m.get("symbol", "")).upper()
            if not sym:
                continue
            base_asset = sym.split("-")[0] if "-" in sym else sym
            results.append(
                InstrumentRecord(
                    instrument_key=f"LIGHTER-ZKSYNC:PERP:{sym}",
                    venue=self.venue,
                    raw_symbol=sym,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=base_asset,
                    quote_asset="USDC",
                    settle_asset="USDC",
                    margin_type=MarginType.LINEAR,
                    tick_size=Decimal("0.0001"),
                    min_size=Decimal("0.001"),
                    contract_size=Decimal("1"),
                    expiry=None,
                    strike=None,
                    option_type=None,
                    status=InstrumentStatus.ACTIVE,
                    available_from_datetime=_LIGHTER_DEPLOY_DATE,
                    timezone="UTC",
                )
            )

        logger.info("Lighter: fetched %d perp instruments", len(results))
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
        raise NotImplementedError("Lighter does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        raise NotImplementedError("Lighter perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        raise NotImplementedError("Lighter funding rate not supported via reference data adapter")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        raise NotImplementedError("Lighter OHLCV not supported via reference data adapter")
