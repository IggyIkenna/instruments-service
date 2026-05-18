"""Pacifica reference data adapter — curated Solana perp DEX instrument list.

Pacifica is a Solana DEX (Hyperliquid clone, mainnet 2025-06). The API does
not expose a public markets discovery endpoint, so the instrument universe is
derived from the same curated top-coin list used by the MTDS tick adapter.
All coins are returned as ACTIVE perpetual instruments.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal

from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType, MarginType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)

logger = logging.getLogger(__name__)

# Mirrors MTDS _PACIFICA_TOP_COINS (umi_tick_provider.py:682).
# Update both lists when new markets are confirmed live.
_PACIFICA_TOP_COINS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "SOL",
    "HYPE",
    "XRP",
    "DOGE",
    "BNB",
    "SUI",
    "PUMP",
    "FARTCOIN",
)

# Pacifica Solana mainnet launch (2025-06-01 per MTDS _PACIFICA_FUNDING_START_MS).
_PACIFICA_DEPLOY_DATE = datetime(2025, 6, 1, tzinfo=UTC)


class PacificaReferenceDataAdapter(BaseReferenceDataAdapter):
    """Pacifica reference data: curated perpetual instrument list.

    Returns all known Pacifica perp markets as InstrumentRecord objects with
    instrument_type=PERPETUAL, settle_asset=USDC, available_from=2025-06-01.

    No live API call is made — the instrument universe is the curated top-coin
    list, which matches the MTDS tick adapter. When Pacifica exposes a public
    /markets endpoint, this adapter should be updated to call it dynamically.
    """

    @property
    def venue(self) -> str:
        """Return the venue identifier."""
        return "PACIFICA-SOLANA"

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return curated Pacifica perp instruments."""
        if instrument_type is not None and instrument_type != InstrumentType.PERPETUAL:
            return []

        results: list[InstrumentRecord] = []
        for coin in _PACIFICA_TOP_COINS:
            sym = f"{coin}-PERP"
            results.append(
                InstrumentRecord(
                    instrument_key=f"PACIFICA-SOLANA:PERP:{sym}",
                    venue=self.venue,
                    raw_symbol=sym,
                    instrument_type=InstrumentType.PERPETUAL,
                    base_asset=coin,
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
                    available_from_datetime=_PACIFICA_DEPLOY_DATE,
                    timezone="UTC",
                )
            )

        logger.info("Pacifica: returning %d curated perp instruments", len(results))
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by identifier."""
        for inst in await self.get_instruments():
            if inst.raw_symbol == symbol or inst.raw_symbol == symbol.upper():
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("Pacifica does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "future",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("Pacifica perpetuals have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("Pacifica funding rate not supported via reference data adapter")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv."""
        raise NotImplementedError("Pacifica OHLCV not supported via reference data adapter")
